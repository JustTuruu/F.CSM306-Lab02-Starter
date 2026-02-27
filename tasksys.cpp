#include "tasksys.h"
#include <algorithm>

namespace
{
const int kTaskChunkSize = 32;
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

        int task_id = -1;
        IRunnable *runnable = nullptr;
        int total_tasks = 0;

        {
            std::lock_guard<std::mutex> lock(task_mutex_);
            if (has_work_ && next_task_id_ < current_total_tasks_)
            {
                task_id = next_task_id_;
                ++next_task_id_;
                runnable = current_runnable_;
                total_tasks = current_total_tasks_;
            }
        }

        if (task_id >= 0)
        {
            runnable->runTask(task_id, total_tasks);
            completed_tasks_.fetch_add(1);
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
    next_bulk_task_id_ = 0;
    pending_bulk_tasks_ = 0;

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
    }
    work_cv_.notify_all();
    for (auto &worker : workers_)
    {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks)
{
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    std::unique_lock<std::mutex> lock(mutex_);

    TaskID new_task_id = next_bulk_task_id_;
    ++next_bulk_task_id_;

    auto task = std::make_shared<BulkTask>();
    task->id = new_task_id;
    task->runnable = runnable;
    task->num_total_tasks = num_total_tasks;
    task->unresolved_deps = 0;
    task->next_task_id = 0;
    task->completed_tasks = 0;
    task->finished = false;

    tasks_[new_task_id] = task;
    ++pending_bulk_tasks_;

    for (TaskID dependency_id : deps)
    {
        auto it = tasks_.find(dependency_id);
        if (it != tasks_.end() && !it->second->finished)
        {
            ++task->unresolved_deps;
            it->second->dependents.push_back(new_task_id);
        }
    }

    if (task->unresolved_deps == 0)
    {
        if (task->num_total_tasks == 0)
        {
            finishTaskUnlocked(task);
        }
        else
        {
            for (int begin = 0; begin < task->num_total_tasks; begin += kTaskChunkSize)
            {
                int end = std::min(begin + kTaskChunkSize, task->num_total_tasks);
                ready_queue_.push({task, begin, end});
            }
            work_cv_.notify_all();
        }
    }

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{
    std::unique_lock<std::mutex> lock(mutex_);
    done_cv_.wait(lock, [this]() {
        return pending_bulk_tasks_ == 0;
    });
}

void TaskSystemParallelThreadPoolSleeping::finishTaskUnlocked(const std::shared_ptr<BulkTask> &task)
{
    if (task->finished)
    {
        return;
    }

    task->finished = true;
    --pending_bulk_tasks_;

    bool has_new_ready_work = false;
    for (TaskID dependent_id : task->dependents)
    {
        auto dependent_it = tasks_.find(dependent_id);
        if (dependent_it == tasks_.end())
        {
            continue;
        }

        const std::shared_ptr<BulkTask> &dependent_task = dependent_it->second;
        --dependent_task->unresolved_deps;

        if (dependent_task->unresolved_deps == 0)
        {
            if (dependent_task->num_total_tasks == 0)
            {
                finishTaskUnlocked(dependent_task);
            }
            else if (!dependent_task->finished)
            {
                for (int begin = 0; begin < dependent_task->num_total_tasks; begin += kTaskChunkSize)
                {
                    int end = std::min(begin + kTaskChunkSize, dependent_task->num_total_tasks);
                    ready_queue_.push({dependent_task, begin, end});
                }
                has_new_ready_work = true;
            }
        }
    }

    if (has_new_ready_work)
    {
        work_cv_.notify_all();
    }

    if (pending_bulk_tasks_ == 0)
    {
        done_cv_.notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop()
{
    while (true)
    {
        std::shared_ptr<BulkTask> task;
        IRunnable *runnable = nullptr;
        int begin_task_id = -1;
        int end_task_id = -1;
        int total_tasks = 0;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_cv_.wait(lock, [this]() {
                return shutting_down_ || !ready_queue_.empty();
            });

            if (shutting_down_ && ready_queue_.empty())
            {
                return;
            }

            ReadyTask ready_task = ready_queue_.front();
            ready_queue_.pop();

            task = ready_task.bulk_task;
            begin_task_id = ready_task.begin_task_id;
            end_task_id = ready_task.end_task_id;
            runnable = task->runnable;
            total_tasks = task->num_total_tasks;
        }

        for (int task_id = begin_task_id; task_id < end_task_id; ++task_id)
        {
            runnable->runTask(task_id, total_tasks);
        }

        {
            std::unique_lock<std::mutex> lock(mutex_);
            task->completed_tasks += (end_task_id - begin_task_id);
            if (task->completed_tasks == task->num_total_tasks)
            {
                finishTaskUnlocked(task);
            }
        }
    }
}