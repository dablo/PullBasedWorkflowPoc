using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Routing;

namespace PullBasedWorkflowCS
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Pull Based Workflow POC!");
            Console.WriteLine("Number of workers: ");
            var nrOfWorkers = int.Parse(Console.ReadLine());
            Console.WriteLine("Number of fetchers: 1");
            var nrOfFetchers = 1; // int.Parse(Console.ReadLine());
            Console.WriteLine("Working delay: ");
            var workingDelay = int.Parse(Console.ReadLine());
            Console.WriteLine("Fetching delay: 50");
            var fetchingDelay = 50; //int.Parse(Console.ReadLine());
            Console.WriteLine("Total runtime: 30000");
            var totalRuntime = 30000; // int.Parse(Console.ReadLine());

            ActorSystem system = ActorSystem.Create("MySystem");

            IActorRef metrics = system.ActorOf<MetricsActor>("metrics");
            system.EventStream.Subscribe(metrics, typeof(DeadLetter));

            IActorRef coordinator = system.ActorOf<CoordinatorActor>("coordinator");
            CoordinatorActor.FetchThreshold = nrOfWorkers;
            var fetcherProps = Props.Create<FetcherActor>().WithRouter(new RoundRobinPool(nrOfFetchers));
            IActorRef fetcher = system.ActorOf<FetcherActor>("fetcher");
            FetcherActor.FetchingDelay = fetchingDelay;

            var workerProps = Props.Create<WorkerActor>().WithRouter(new RoundRobinPool(nrOfWorkers));
            IActorRef worker = system.ActorOf(workerProps, "worker");
            WorkerActor.WorkingDelay = workingDelay;

            //Console.ReadLine();
            Console.WriteLine();
            Console.WriteLine("Starting...");
            Console.WriteLine();


            metrics.Tell(new StartWorking());
            worker.Tell(new Broadcast(new StartWorking()));
            await Task.Delay(totalRuntime);
            //Console.ReadLine();
            //worker.Tell(new Broadcast(new StopWorking()));
            //await Task.Delay(5000);
            coordinator.Tell(new StopWorking());

            Console.WriteLine("Stopping coordinator..");
            Console.WriteLine();

            Console.ReadLine();
        }
    }

    internal class MetricsActor : UntypedActor
    {
        private int _numberOfMessagesDone;
        private int _numberOfMessagesFetched;
        private Stopwatch _sw;
        private bool _started;
        private ActorSelection _coordinator;
        private int _messageQueueCount;
        private ActorSelection _workers;
        private int _workerCount;
        private int _workingQueueCount;
        private int _nrOfTimesFetching;

        public MetricsActor()
        {
            _coordinator = Context.ActorSelection("/user/coordinator");
            _workers = Context.ActorSelection("/user/worker");
        }

        protected override void PreStart()
        {
            Context.System.Scheduler.ScheduleTellRepeatedly(1000, 10000, Self, new WriteStats(), ActorRefs.Nobody);
            Context.System.Scheduler.ScheduleTellRepeatedly(1000, 1000, _coordinator, new GetMessageQueueCount(), Self);
            Context.System.Scheduler.ScheduleTellRepeatedly(1000, 1000, _coordinator, new GetWorkingQueueCount(), Self);
            Context.System.Scheduler.ScheduleTellRepeatedly(1000, 1000, _workers, new GetRoutees(), Self);
            base.PreStart();
        }

        internal class GetMessageQueueCount
        {
        }

        internal class MessageQueueCount
        {
            public MessageQueueCount(int count)
            {
                Count = count;
            }

            public int Count { get; }
        }

        internal class WorkingQueueCount
        {
            public WorkingQueueCount(int count)
            {
                Count = count;
            }

            public int Count { get; }
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case MessageProcessed _:
                    _numberOfMessagesDone++;
                    break;
                case Fetch m:
                    _numberOfMessagesFetched += m.Number;
                    _nrOfTimesFetching++;
                    break;
                case WriteStats _:
                    if (_started)
                    {
                        WriteStats();
                    }

                    break;
                case DeadLetter m:
                    Console.WriteLine($"Deadletter: {m.Sender} -> {m.Recipient} | {m.Message}");
                    break;
                case StartWorking _:
                    _started = true;
                    _sw = Stopwatch.StartNew();
                    break;
                case StopWorking _:
                    _started = false;
                    var seconds = _sw.ElapsedMilliseconds / 1000;
                    var msgPerSec = _numberOfMessagesDone / seconds;
                    WriteStats();
                    Console.WriteLine(
                        $"{_numberOfMessagesDone} Messages in {seconds} seconds: {msgPerSec} messages/sec");
                    break;
                case MessageQueueCount m:
                    _messageQueueCount = m.Count;
                    break;
                case WorkingQueueCount m:
                    _workingQueueCount = m.Count;
                    //Console.WriteLine(m.Count);
                    break;
                case Routees m:
                    _workerCount = m.Members.Count();
                    break;
                default:
                    Console.WriteLine($"Unhandled message: {message}");
                    break;
            }
        }

        private void WriteStats()
        {
            Console.WriteLine("NumberOfTimesFetching: " + _nrOfTimesFetching);
            Console.WriteLine("NumberOfMessagesFetched: " + _numberOfMessagesFetched);
            //Console.WriteLine("NumberOfWorkers : " + _workerCount);
            Console.WriteLine("MessageQueueCount : " + _messageQueueCount);
            Console.WriteLine("WorkingQueueCount : " + _workingQueueCount);
            Console.WriteLine("NumberOfMessagesDone: " + _numberOfMessagesDone);
            Console.WriteLine();
        }

        internal class GetWorkingQueueCount
        {
        }
    }

    internal class WriteStats
    {
    }

    internal class FetcherActor : ReceiveActor
    {
        private ActorSelection _metrics;

        public FetcherActor()
        {
            _metrics = Context.ActorSelection("/user/metrics");
            ReceiveAsync<Fetch>(async fetch =>
            {
                var messages = await GetMessages(fetch);
                Sender.Tell(messages);
                _metrics.Tell(fetch);
            });
        }

        public static int FetchingDelay = 0;
        private int _nrOfTimesFetching;

        //protected override void OnReceive(object message)
        //{
        //    switch (message)
        //    {
        //        case Fetch m:
        //            _metrics.Tell(m);
        //            GetMessages(m).PipeTo(Sender);
        //            break;
        //        default:
        //            Console.WriteLine($"Unhandled message: {message}");
        //            break;

        //    }
        //}

        private async Task<List<UnitMessage>> GetMessages(Fetch fetch)
        {
            _nrOfTimesFetching++;
            var messages = new List<UnitMessage>();
            for (int i = 0; i < fetch.Number; i++)
            {
                messages.Add(new UnitMessage(_nrOfTimesFetching * fetch.Number + i));
            }

            await Task.Delay(FetchingDelay);
            return messages;
        }
    }

    internal class UnitMessage
    {
        public int Id { get; }

        public UnitMessage(int id)
        {
            Id = id;
        }
    }

    internal class MessageProcessed
    {
        public int Id { get; }

        public MessageProcessed(int id)
        {
            Id = id;
        }
    }

    internal class Fetch
    {
        public Fetch(int number)
        {
            Number = number;
        }

        public int Number { get; }
    }

    internal class CoordinatorActor : UntypedActor
    {
        private readonly Queue<UnitMessage> _messages;
        private readonly ActorSelection _fetcher;
        private readonly List<UnitMessage> _workingQueue;
        private ActorSelection _metrics;
        private ActorSelection _workers;

        public CoordinatorActor()
        {
            _workingQueue = new List<UnitMessage>();
            _messages = new Queue<UnitMessage>();
            _fetcher = Context.ActorSelection("/user/fetcher");
            _metrics = Context.ActorSelection("/user/metrics");
            _workers = Context.ActorSelection("/user/worker");
        }

        public static int FetchThreshold { get; set; }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case MessageProcessed m:
                    var um = _workingQueue.FirstOrDefault(unitMessage => unitMessage.Id.Equals(m.Id));
                    _workingQueue.Remove(um);
                    //SendMessageToWorker();
                    break;
                case RequestMessage _:
                    SendMessageToWorker();
                    FetchMessages();
                    break;
                case List<UnitMessage> messages:
                    EnqueueMessages(messages);
                    break;
                case MetricsActor.GetMessageQueueCount _:
                    Sender.Tell(new MetricsActor.MessageQueueCount(_messages.Count));
                    break;
                case MetricsActor.GetWorkingQueueCount _:
                    Sender.Tell(new MetricsActor.WorkingQueueCount(_workingQueue.Count));
                    break;
                case StopWorking _:
                    Become(Stopped);
                    Self.Tell(new StopWorking());
                    break;
                default:
                    Console.WriteLine($"Unhandled message: {message}");
                    break;
            }
        }

        private void Stopped(object message)
        {
            switch (message)
            {
                case MessageProcessed m:
                    var um = _workingQueue.FirstOrDefault(unitMessage => unitMessage.Id.Equals(m.Id));
                    _workingQueue.Remove(um);
                    break;
                case RequestMessage _:
                    SendMessageToWorker();
                    break;
                case List<UnitMessage> messages:
                    EnqueueMessages(messages);
                    break;
                case MetricsActor.GetMessageQueueCount _:
                    Sender.Tell(new MetricsActor.MessageQueueCount(_messages.Count));
                    break;
                case MetricsActor.GetWorkingQueueCount _:
                    Sender.Tell(new MetricsActor.WorkingQueueCount(_workingQueue.Count));
                    break;
                case StopWorking _:
                    if (_workingQueue.Any()||_messages.Any())
                    {
                        Self.Tell(new StopWorking());
                        return;
                    }
                    _workers.Tell(new Broadcast(new StopWorking()));
                    _metrics.Tell(new MetricsActor.WorkingQueueCount(_workingQueue.Count));
                    _metrics.Tell(new MetricsActor.MessageQueueCount(_messages.Count));
                    _metrics.Tell(new StopWorking());
                    Console.WriteLine("Coordinator stopped!");
                    Console.WriteLine();
                    break;
            }
        }

        private void FetchMessages()
        {
            if (_messages.Count < FetchThreshold)
            {
                _fetcher.Tell(new Fetch(FetchThreshold));
                Become(Fetching);
            }
        }

        private void SendMessageToWorker()
        {
            if (_messages.Any())
            {
                var m = _messages.Dequeue();
                _workingQueue.Add(m);
                Sender.Tell(m);
            }
        }

        protected void Fetching(object message)
        {
            switch (message)
            {
                case MessageProcessed m:
                    var um = _workingQueue.FirstOrDefault(unitMessage => unitMessage.Id.Equals(m.Id));
                    _workingQueue.Remove(um);
                    break;
                case RequestMessage _:
                    SendMessageToWorker();
                    break;
                case List<UnitMessage> messages:
                    EnqueueMessages(messages);
                    Become(OnReceive);
                    break;
                case MetricsActor.GetMessageQueueCount _:
                    Sender.Tell(new MetricsActor.MessageQueueCount(_messages.Count));
                    break;
                case MetricsActor.GetWorkingQueueCount _:
                    Sender.Tell(new MetricsActor.WorkingQueueCount(_workingQueue.Count));
                    break;
                case StopWorking _:
                    Become(Stopped);
                    Self.Tell(new StopWorking());
                    break;

                default:
                    Console.WriteLine($"Unhandled message: {message}");
                    break;
            }
        }

        private void EnqueueMessages(List<UnitMessage> messages)
        {
            foreach (var unitMessage in messages)
            {
                _messages.Enqueue(unitMessage);
            }
        }
    }

    internal class WorkerActor : ReceiveActor
    {
        private readonly ActorSelection _coordinator;
        private ActorSelection _metrics;
        private bool _started;

        public static int WorkingDelay = 0;

        public WorkerActor()
        {
            _coordinator = Context.ActorSelection("/user/coordinator");
            _metrics = Context.ActorSelection("/user/metrics");
            Receive<StartWorking>(working =>
            {
                _started = true;
                //_coordinator.Tell(new RequestMessage());
            });
            Receive<StopWorking>(working => { _started = false; });
            Receive<ReceiveTimeout>(timeout =>
            {
                if (_started)
                {
                    _coordinator.Tell(new RequestMessage());
                }
            });
            ReceiveAsync<UnitMessage>(async message =>
            {
                var done = await RunWorkflow(message);
                Sender.Tell(done);
                _metrics.Tell(done);
                if (_started)
                {
                    _coordinator.Tell(new RequestMessage());
                }
            });
        }

        protected override void PreStart()
        {
            Context.SetReceiveTimeout(TimeSpan.FromSeconds(1));
            base.PreStart();
        }

        //protected override void OnReceive(object message)
        //{
        //    switch (message)
        //    {
        //        case StartWorking _:
        //            _started = true;
        //            _coordinator.Tell(new RequestMessage());
        //            break;
        //        case StopWorking _:
        //            _started = false;
        //            break;
        //        case UnitMessage m:
        //            RunWorkflow(m).PipeTo(_coordinator);
        //            _metrics.Tell(new MessageProcessed(m.Id));
        //            break;
        //        case ReceiveTimeout _:
        //            if (_started)
        //            {
        //                _coordinator.Tell(new RequestMessage());
        //            }

        //            break;
        //        default:
        //            Console.WriteLine($"Unhandled message: {message}");
        //            break;

        //    }
        //}

        private async Task<MessageProcessed> RunWorkflow(UnitMessage unitMessage)
        {
            await Task.Delay(WorkingDelay);
            return new MessageProcessed(unitMessage.Id);
        }
    }

    internal class StartWorking
    {
    }

    internal class StopWorking
    {
    }

    internal class RequestMessage
    {
    }
}