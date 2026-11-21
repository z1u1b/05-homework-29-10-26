#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <thread>
#include <vector>

struct rwlock_t
{
  rwlock_t()
  {
    pthread_rwlock_init(&rwlock_, nullptr);
  }

  ~rwlock_t()
  {
    pthread_rwlock_destroy(&rwlock_);
  }

  rwlock_t(const rwlock_t&) = delete;
  rwlock_t& operator=(const rwlock_t&) = delete;

  void lock()
  {
    pthread_rwlock_wrlock(&rwlock_);
  }

  bool try_lock()
  {
    return pthread_rwlock_trywrlock(&rwlock_) == 0;
  }

  void unlock()
  {
    pthread_rwlock_unlock(&rwlock_);
  }

  void lock_shared()
  {
    pthread_rwlock_rdlock(&rwlock_);
  }

  bool try_lock_shared()
  {
    return pthread_rwlock_tryrdlock(&rwlock_) == 0;
  }

  void unlock_shared()
  {
    pthread_rwlock_unlock(&rwlock_);
  }

private:
  pthread_rwlock_t rwlock_;
};

struct task_t
{
  void operator()();
};

struct th_pool_t
{
  using f_t = std::function< void(void) >;

  explicit th_pool_t(unsigned int k, std::size_t batch_size = 4);
  ~th_pool_t();

  void submit(f_t f);
  void wait();
  void pause();
  void resume();

private:
  void execute(std::size_t id);
  bool try_fetch_batch(std::deque< f_t >& local_q);
  bool try_steal(std::size_t thief_id, std::deque< f_t >& local_q);

  std::size_t batch_size_{4};
  std::deque< f_t > tasks_;
  rwlock_t rw_;
  std::mutex cv_mx_;
  std::condition_variable cd_;
  std::condition_variable idle_cd_;

  bool stop_{false};
  bool paused_{false};
  std::size_t active_{0};

  struct worker_data_t
  {
    std::deque< f_t > queue;
    std::mutex mx;
  };

  std::vector< std::unique_ptr< worker_data_t > > workers_;
  std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  rw_.lock();
  tasks_.push_back(std::move(f));
  rw_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k, std::size_t batch_size):
  batch_size_(batch_size)
{
  workers_.reserve(k);
  for (std::size_t i = 0; i < k; ++i) {
    workers_.push_back(std::make_unique< worker_data_t >());
  }

  for (std::size_t i = 0; i < k; ++i) {
    ths_.emplace_back([this, i]() { execute(i); });
  }
}

th_pool_t::~th_pool_t()
{
  rw_.lock();
  stop_ = true;
  paused_ = false;
  rw_.unlock();
  cd_.notify_all();
  for (auto&& th : ths_) {
    th.join();
  }
}

void th_pool_t::wait()
{
  std::unique_lock lck(cv_mx_);
  idle_cd_.wait(lck, [this]() {
    if (active_ != 0) {
      return false;
    }
    rw_.lock_shared();
    bool has_global = !tasks_.empty();
    rw_.unlock_shared();
    if (has_global) {
      return false;
    }
    for (const auto& w : workers_) {
      std::lock_guard q_lck(w->mx);
      if (!w->queue.empty()) {
        return false;
      }
    }
    return true;
  });
}

void th_pool_t::pause()
{
  rw_.lock();
  paused_ = true;
  rw_.unlock();
}

void th_pool_t::resume()
{
  rw_.lock();
  paused_ = false;
  rw_.unlock();
  cd_.notify_all();
}

bool th_pool_t::try_fetch_batch(std::deque< f_t >& local_q)
{
  rw_.lock_shared();
  if (tasks_.empty() || paused_) {
    rw_.unlock_shared();
    return false;
  }
  rw_.unlock_shared();

  rw_.lock();
  if (tasks_.empty() || paused_) {
    rw_.unlock();
    return false;
  }

  std::size_t count = std::min(batch_size_, tasks_.size());
  for (std::size_t i = 0; i < count; ++i) {
    local_q.push_back(std::move(tasks_.front()));
    tasks_.pop_front();
  }
  rw_.unlock();
  return true;
}

bool th_pool_t::try_steal(std::size_t thief_id, std::deque< f_t >& local_q)
{
  std::size_t num_workers = workers_.size();
  for (std::size_t i = 1; i < num_workers; ++i) {
    std::size_t target_id = (thief_id + i) % num_workers;
    auto& victim = *workers_[target_id];

    if (victim.queue.empty()) {
      continue;
    }

    std::unique_lock target_lck(victim.mx, std::try_to_lock);
    if (!target_lck.owns_lock()) {
      continue;
    }

    if (victim.queue.size() > 1) {
      std::size_t steal_count = (victim.queue.size() + 1) / 2;
      for (std::size_t s = 0; s < steal_count; ++s) {
        local_q.push_back(std::move(victim.queue.back()));
        victim.queue.pop_back();
      }
      return true;
    }
  }
  return false;
}

void th_pool_t::execute(std::size_t id)
{
  auto& my_data = *workers_[id];

  while (true) {
    f_t current_task;

    {
      std::lock_guard lck(my_data.mx);
      if (!my_data.queue.empty()) {
        current_task = std::move(my_data.queue.front());
        my_data.queue.pop_front();
      }
    }

    if (!current_task) {
      if (try_fetch_batch(my_data.queue)) {
        std::lock_guard lck(my_data.mx);
        if (!my_data.queue.empty()) {
          current_task = std::move(my_data.queue.front());
          my_data.queue.pop_front();
        }
      }
    }

    if (!current_task) {
      if (try_steal(id, my_data.queue)) {
        std::lock_guard lck(my_data.mx);
        if (!my_data.queue.empty()) {
          current_task = std::move(my_data.queue.front());
          my_data.queue.pop_front();
        }
      }
    }

    if (current_task) {
      {
        std::lock_guard cv_lck(cv_mx_);
        ++active_;
      }

      try {
        current_task();
      } catch (...) {
      }

      {
        std::lock_guard cv_lck(cv_mx_);
        --active_;
      }
      idle_cd_.notify_all();
      continue;
    }

    {
      std::unique_lock cv_lck(cv_mx_);
      auto proceed = [this, &my_data]() {
        if (stop_) {
          return true;
        }
        rw_.lock_shared();
        bool ready = (!paused_ && !tasks_.empty());
        rw_.unlock_shared();
        return ready || !my_data.queue.empty();
      };

      if (stop_) {
        rw_.lock_shared();
        bool empty = tasks_.empty();
        rw_.unlock_shared();
        if (empty) {
          break;
        }
      }

      cd_.wait_for(cv_lck, std::chrono::milliseconds(2), proceed);

      if (stop_) {
        rw_.lock_shared();
        bool empty = tasks_.empty();
        rw_.unlock_shared();
        if (empty) {
          std::lock_guard q_lck(my_data.mx);
          if (my_data.queue.empty()) {
            break;
          }
        }
      }
    }
  }
}

void task_t::operator()()
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(100ms);
  static std::mutex out_mx;
  std::lock_guard lck(out_mx);
  std::cout << "!\n";
}

int main()
{
  th_pool_t pool(4);
  for (size_t i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }

  pool.wait();
  pool.pause();
  for (size_t i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }

  pool.resume();
  pool.wait();
}
