using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using TaskScheduler.Exceptions;

namespace TaskScheduler
{
    public class TaskScheduler : IJobExecutor
    {
        private Thread _startThread;
        private Thread _stopThread;
        private Thread _threadWhichMainThreadNeedToWait;
        private int _runningThreadsCount;

        private ConcurrentQueue<Action> _queue;
        private ConcurrentDictionary<Guid, Action> _runningTasks;

        private volatile bool  _queueIsNotEmpty;
        private volatile bool _isQueueProcessingComplete;
        private volatile bool _isEverythingComplete;

        private const int StopDefaultTimeout = 3000;
        private const int RefreshTimeout = 5;
        
        private int _amount;
        public int Amount
        {
            get => _queue.Count;
            private set => Interlocked.Exchange(ref _amount, value);
        }
        
        public TaskScheduler()
        {
            _queue = new ConcurrentQueue<Action>();
            _runningTasks = new ConcurrentDictionary<Guid, Action>();
            _runningThreadsCount = 0;
        }

        public void Start(int maxConcurrent)
        {
            if (maxConcurrent > 1020)
            {
                throw new NonValidValueException(nameof(maxConcurrent), nameof(Start), maxConcurrent);
            }

            _startThread = new Thread(() =>
            {
                _isQueueProcessingComplete = false;
                var freeSpace = GetFreeSpace(maxConcurrent);
                SendMaximumPossibleCountOfTasksToRun(freeSpace);

                while (!_isQueueProcessingComplete)
                {
                    freeSpace = WaitSomeMillisecondsAndGetFreeSpaceAfter(RefreshTimeout, maxConcurrent);
                    if (freeSpace != 0) SendMaximumPossibleCountOfTasksToRun(freeSpace);
                }
                //
                // if (!_queueIsNotEmpty && AreAllTheTasksComplete())
                // {
                //     _isEverythingComplete = true;
                // }
            });
            _startThread.Start();
            KillMainThreadIfEverythingComplete();
        }
        
        public void Stop()
        {
            _stopThread = new Thread(() =>
            {
                _isQueueProcessingComplete = true;

                while (!AreAllTheTasksComplete())
                {
                    Thread.Sleep(RefreshTimeout);
                }

                if (_queueIsNotEmpty)
                {
                    Thread.Sleep(StopDefaultTimeout);
                    _isEverythingComplete = true;
                }
            });
            _stopThread.Start();
            KillMainThreadIfEverythingComplete();
        }

        public void Add(Action action)
        {
            if (action == null) throw new NullArgumentException(nameof(action), nameof(Add));
            _queue.Enqueue(action);
            _queueIsNotEmpty = true;
        }

        public void Clear()
        {
            _queue.Clear();
            Amount = 0;
            _queueIsNotEmpty = false;
        }
        
        public void Join()
        {
            _threadWhichMainThreadNeedToWait.Join();
        }

        private void KillMainThreadIfEverythingComplete()
        {
            _threadWhichMainThreadNeedToWait = new Thread(() =>
            {
                Interlocked.Increment(ref _runningThreadsCount);
                while (!_isEverythingComplete)
                {
                    if (!_queueIsNotEmpty && AreAllTheTasksComplete())
                    {
                        _isEverythingComplete = true;
                    }
                    Thread.Sleep(RefreshTimeout);
                }
                Interlocked.Decrement(ref _runningThreadsCount);
            });
            _threadWhichMainThreadNeedToWait.Start();
        }

        private int GetFreeSpace(int maxConcurrent)
        {
            return maxConcurrent - RunningTasksCount();
        }

        private bool AreAllTheTasksComplete()
        {
            return RunningTasksCount() == 0;
        }
        
        private void MoveNextActionToRunningTasksFromQueue()
        {
            if (Amount == 0)
            {
                _queueIsNotEmpty = false;
                return;
            }

            ThreadPool.QueueUserWorkItem(state =>
            {
                var id = Guid.NewGuid();
                if (Amount == 0) return;
                _queue.TryDequeue(out var action);
                _runningTasks[id] = action;

                try
                {
                    action?.Invoke();
                }
                
                finally
                {
                    _runningTasks.Remove(id, out _);
                }
            });
        }

        private void RunTasks(int count)
        {
            for (var i = 0; i < count; i++)
            {
                MoveNextActionToRunningTasksFromQueue();
            }
        }

        private int WaitSomeMillisecondsAndGetFreeSpaceAfter(int millisecondsCount, int maxConcurrent)
        {
            Thread.Sleep(millisecondsCount);
            return GetFreeSpace(maxConcurrent);
        }

        private void SendMaximumPossibleCountOfTasksToRun(int freeSpace)
        {
            if (Amount == 0)
            {
                _queueIsNotEmpty = false;
            }
            RunTasks(Amount <= freeSpace ? Amount : freeSpace);
        }

        public int RunningTasksCount()
        {
            return _runningTasks.Count;
        }
    }
}