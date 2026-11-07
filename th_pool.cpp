#include <condition_variable>
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <syncstream>
#include <thread>
#include <vector>

struct task_t
{
  void operator()();
};

struct th_pool_t
{
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);

  void wait();  // задача 2
  void pause(); // задача 3

  void resume();

private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  bool stop_{false};
  std::vector< std::thread > ths_;

  std::condition_variable idle_cd_;
  std::size_t active_{0};
  bool paused_{false};
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() { execute(); };
  for (; k; --k)
    ths_.emplace_back(thf);
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
  for (auto&& th : ths_)
    th.join();
}

// задача 2
void th_pool_t::wait()
{
  std::unique_lock lck(mx_);
  idle_cd_.wait(lck, [this]() { return tasks_.empty() && active_ == 0; });
}

// задача 3
void th_pool_t::pause()
{
  std::lock_guard lck(mx_);
  paused_ = true;
}

void th_pool_t::resume()
{
  mx_.lock();
  paused_ = false;
  mx_.unlock();
  cd_.notify_all();
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);
  do {
    auto proceed = [&]() { return stop_ || (!paused_ && !tasks_.empty()); };

    cd_.wait(lck, proceed);
    if (tasks_.empty()) {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    ++active_;
    lck.unlock();
    try {
      task();
    } catch (...) {
    }
    lck.lock();
    --active_;
    if (tasks_.empty() && active_ == 0) {
      idle_cd_.notify_all();
    }

  } while (!stop_ || !tasks_.empty());
}

void task_t::operator()()
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  std::osyncstream(std::cout) << "!\n";
}

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }

  pool.wait();
  pool.pause();
  for (size_t i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }

  pool.resume();
}
