#include "tasksys.h"
#include <algorithm>
#include <thread>

namespace
{
const int kSpinChunkSize = 16;
const int kSleepChunkSize = 32;
}

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    int workers = std::min(num_threads_, num_total_tasks);
    std::vector<std::thread> threads;
    threads.reserve(workers);

    for (int t = 0; t < workers; ++t)
    {
        int begin = (num_total_tasks * t) / workers;
        int end = (num_total_tasks * (t + 1)) / workers;
        threads.emplace_back([runnable, num_total_tasks, begin, end]() {
            for (int i = begin; i < end; ++i)
            {
                runnable->runTask(i, num_total_tasks);
            }
        });
    }

    for (auto &thread : threads)
    {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
    shutting_down_ = false;
    has_work_ = false;
    current_runnable_ = nullptr;
    current_total_tasks_ = 0;
    next_task_id_ = 0;
    completed_tasks_ = 0;

    for (int i = 0; i < num_threads_; ++i)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    shutting_down_ = true;

    for (auto &worker : workers_)
    {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop()
{
    while (!shutting_down_)
    {
        if (!has_work_)
        {
            std::this_thread::yield();
            continue;
        }

        int begin_task_id = -1;
        int end_task_id = -1;
        IRunnable *runnable = nullptr;
        int total_tasks = 0;

        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            if (has_work_ && next_task_id_ < current_total_tasks_)
            {
                begin_task_id = next_task_id_;
                end_task_id = std::min(begin_task_id + kSpinChunkSize, current_total_tasks_);
                next_task_id_ = end_task_id;
                runnable = current_runnable_;
                total_tasks = current_total_tasks_;
            }
        }

        if (begin_task_id >= 0)
        {
            for (int task_id = begin_task_id; task_id < end_task_id; ++task_id)
            {
                runnable->runTask(task_id, total_tasks);
            }
            completed_tasks_.fetch_add(end_task_id - begin_task_id);
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(task_mutex_);
        current_runnable_ = runnable;
        current_total_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        completed_tasks_ = 0;
        has_work_ = true;
    }

    while (completed_tasks_.load() < num_total_tasks)
    {
        std::this_thread::yield();
    }

    has_work_ = false;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name()
{
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads)
{
    num_threads_ = std::max(1, num_threads);
    shutting_down_ = false;
    has_work_ = false;
    current_runnable_ = nullptr;
    current_total_tasks_ = 0;
    next_task_id_ = 0;
    completed_tasks_ = 0;

    for (int i = 0; i < num_threads_; ++i)
    {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutting_down_ = true;
        has_work_ = false;
    }
    work_cv_.notify_all();

    for (auto &worker : workers_)
    {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    if (num_total_tasks <= 0)
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        current_total_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        completed_tasks_ = 0;
        has_work_ = true;
    }

    work_cv_.notify_all();

    std::unique_lock<std::mutex> lock(mutex_);
    done_cv_.wait(lock, [this]() {
        return !has_work_;
    });
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        int begin_task_id = -1;
        int end_task_id = -1;
        IRunnable *runnable = nullptr;
        int total_tasks = 0;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_cv_.wait(lock, [this]() {
                return shutting_down_ || (has_work_ && next_task_id_ < current_total_tasks_);
            });

            if (shutting_down_)
            {
                return;
            }

            begin_task_id = next_task_id_;
            end_task_id = std::min(begin_task_id + kSleepChunkSize, current_total_tasks_);
            next_task_id_ = end_task_id;
            runnable = current_runnable_;
            total_tasks = current_total_tasks_;
        }

        for (int task_id = begin_task_id; task_id < end_task_id; ++task_id)
        {
            runnable->runTask(task_id, total_tasks);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            completed_tasks_ += (end_task_id - begin_task_id);
            if (completed_tasks_ == current_total_tasks_)
            {
                has_work_ = false;
                done_cv_.notify_one();
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CSM306 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CSM306 students will modify the implementation of this method in Part B.
    //

    return;
}