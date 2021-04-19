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

        private ConcurrentQueue<Action> _queue;
        private ConcurrentDictionary<Guid, Action> _runningTasks;

        private volatile bool  _queueIsEmpty;
        private volatile bool _isQueueProcessingComplete;
        private volatile bool _isEverythingComplete;

        private const int StopDefaultTimeout = 3000;
        private const int RefreshTimeout = 5;

        public int Amount => _queue.Count;

        public int RunningTasksCount => _runningTasks.Count;

        public TaskScheduler()
        {
            _queue = new ConcurrentQueue<Action>();
            _runningTasks = new ConcurrentDictionary<Guid, Action>();
        }

        public void Start(int maxConcurrent)
        {
            if (maxConcurrent > 1020)
            {
                throw new NonValidValueException(nameof(maxConcurrent), nameof(Start), maxConcurrent);
            }
            
            InitializeStartThread(maxConcurrent);
            InitializeThreadWhichMainThreadNeedToWait();
            
            _startThread.Start();
            _threadWhichMainThreadNeedToWait.Start();
        }
        
        public void Stop()
        {
            InitializeStopThread();
            _stopThread.Start();
        }

        public void Add(Action action)
        {
            if (action == null) throw new NullArgumentException(nameof(action), nameof(Add));
            _queue.Enqueue(action);
            _queueIsEmpty = false;
        }

        public void Clear()
        {
            _queue.Clear();
            _queueIsEmpty = true;
        }
        
        public void LetTheSchedulerFinishCurrentSession()
        {
            _threadWhichMainThreadNeedToWait.Join();
        }
        
        private void InitializeStartThread(int maxConcurrent)
        {
            _startThread = new Thread(() =>
            {
                _isQueueProcessingComplete = false;
                var freeSpace = maxConcurrent - RunningTasksCount;
                SendMaximumPossibleCountOfTasksToRun(freeSpace);

                while (!_isQueueProcessingComplete)
                {
                    freeSpace = WaitSomeMillisecondsAndGetFreeSpaceAfter(RefreshTimeout, maxConcurrent);
                    if (freeSpace != 0) SendMaximumPossibleCountOfTasksToRun(freeSpace);
                }
                
                if (_queueIsEmpty && RunningTasksCount == 0)
                {
                    _isEverythingComplete = true;
                }
            });
        }

        private void InitializeStopThread()
        {
            _stopThread = new Thread(() =>
            {
                _isQueueProcessingComplete = true;

                while (RunningTasksCount != 0)
                {
                    Thread.Sleep(RefreshTimeout);
                }

                if (_queueIsEmpty) return;
                Thread.Sleep(StopDefaultTimeout);
                if(_isQueueProcessingComplete)
                    _isEverythingComplete = true;
            });
        }

        private void InitializeThreadWhichMainThreadNeedToWait()
        {
            _threadWhichMainThreadNeedToWait = new Thread(() =>
            {
                while (!_isEverythingComplete)
                {
                    if (_queueIsEmpty && RunningTasksCount == 0)
                    {
                        _isEverythingComplete = true;
                    }
                    Thread.Sleep(RefreshTimeout);
                }
            });   
        }

        private void MoveNextActionToRunningTasksFromQueue()
        {
            if (Amount == 0)
            {
                _queueIsEmpty = true;
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
        
        private int WaitSomeMillisecondsAndGetFreeSpaceAfter(int millisecondsCount, int maxConcurrent)
        {
            Thread.Sleep(millisecondsCount);
            return maxConcurrent - RunningTasksCount;
        }

        private void SendMaximumPossibleCountOfTasksToRun(int freeSpace)
        {
            if (Amount == 0)
            {
                _queueIsEmpty = true; return;
            }
            var count = Amount <= freeSpace ? Amount : freeSpace;
            for (var i = 0; i < count; i++)
            {
                MoveNextActionToRunningTasksFromQueue();
            }
        }
    }
}