using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Prajna.Core;
using Prajna.Api.CSharp;
using Prajna.Api.CSharp.Linq;

using Prajna.Tools.FSharp;


namespace PrajnaTest.CS
{
    class Program
    {
        static void GetProcessInfo(Cluster cluster)
        {
            var dset = new DSet<int> { Name = Guid.NewGuid().ToString("D"), Cluster = cluster };
            var descriptions =
                dset
                .DistributeN(cluster.NumNodes * 4, Enumerable.Range(0, cluster.NumNodes * 8))
                .Select(i =>
                {
                    var machineName = System.Environment.MachineName;
                    var process = System.Diagnostics.Process.GetCurrentProcess();
                    var description = $"{machineName}-{process.Id}-{Thread.CurrentThread.ManagedThreadId}-{i}";
                    return description;
                })
                .ToIEnumerable()
                .ToArray();
            foreach (var description in descriptions)
            {
                Console.WriteLine(description);
            }
        }

        static void Main(string[] args)
        {
            var parse = new Prajna.Tools.ArgumentParser(args);
            var clusterFile = parse.ParseString("-cluster", "cluster.lst");
            //Logger.ParseArgs(args);

            //string[] local = new string[2] {"a.bin", "b.bin"};
            //JobDependencies.Current.Add(local);
            //var localRemote = new Tuple<string, string>(@"c:\sort\raw\raw.bin", "a.bin");
            //JobDependencies.Current.AddLocalRemote(localRemote);

            Console.WriteLine("Init...");
            Prajna.Core.Environment.Init();
            Console.WriteLine("Init done.");

            //var cluster = new Cluster(clusterFile);
            var cluster = new Cluster("local[1]");
            var nodes = cluster.Nodes;
            Console.WriteLine($"nodes = {cluster.NumNodes}");

            GetProcessInfo(cluster);
            var watch = System.Diagnostics.Stopwatch.StartNew();
            watch.Start();
            for ( int i = 0; i<3; i++ )
            {
                var t1 = watch.Elapsed;
                Prajna.Tools.CSharp.Logger.Log(Prajna.Tools.LogLevel.Info, "Start executing ... ");
                var containers = Prajna.Service.DistributedFunctionBuiltIn.GetConnectedContainers().ToArray<Tuple<string,string>>();
                Prajna.Tools.CSharp.Logger.Log(Prajna.Tools.LogLevel.Info, "Done executing ... ");
                var elapse = ( watch.Elapsed.Subtract( t1 )).TotalMilliseconds; 

                foreach (var container in containers)
                {
                    var name = container.Item1;
                    var info = container.Item2;
                    Console.WriteLine($"The connected containers are = {name}, {info}, time = {elapse}ms");
                }
            }
            Console.WriteLine("Cleanup...");
            Prajna.Core.Environment.Cleanup();
            Console.WriteLine("Cleanup done");
        }
    }
}
