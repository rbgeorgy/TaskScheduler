using System;

namespace TaskScheduler
{
    public interface IJobExecutor
    {
        /// Кол-во задач в очереди на обработку
        int Amount { get; }
        
        /// Запустить обработку очереди и установить максимальное кол-во
        /// параллельных задач
        /// <param name="maxConcurrent">максимальное кол-во одновременно
        /// выполняемых задач</param>
        void Start(int maxConcurrent);

        /// Остановить обработку очереди и выполнять задачи
        void Stop();
        
        /// Добавить задачу в очередь
        void Add(Action action);
        
        /// Очистить очередь задач
        void Clear();
    }
}