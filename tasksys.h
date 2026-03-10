#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem
{
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem
{
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem
{
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char *name();
    void run(IRunnable *runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                            const std::vector<TaskID> &deps);
    void sync();

private:
    struct BulkTask
    {
        TaskID id;
        IRunnable *runnable;
        int num_total_tasks;
        std::vector<TaskID> dependents;
        int unresolved_deps;
        int completed_tasks;
        bool finished;
    };

    struct ReadyTask
    {
        std::shared_ptr<BulkTask> bulk_task;
        int begin_task_id;
        int end_task_id;
    };

    void workerLoop();
    void finishTaskUnlocked(const std::shared_ptr<BulkTask> &task);

    int num_threads_;
    std::vector<std::thread> workers_;

    std::mutex mutex_;
    std::condition_variable work_cv_;
    std::condition_variable done_cv_;

    bool shutting_down_;
    TaskID next_bulk_task_id_;
    int pending_bulk_tasks_;

    std::unordered_map<TaskID, std::shared_ptr<BulkTask>> tasks_;
    std::queue<ReadyTask> ready_queue_;
};

#endif
