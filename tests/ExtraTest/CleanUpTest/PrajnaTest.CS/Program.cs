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
            Logger.ParseArgs(args);

            Console.WriteLine("Init...");
            Prajna.Core.Environment.Init();
            Console.WriteLine("Init done.");

            var cluster = new Cluster("cluster.lst");
            var nodes = cluster.Nodes;
            //var cluster = new Cluster("local[2]");
            Console.WriteLine($"nodes = {cluster.NumNodes}");

            GetProcessInfo(cluster);

            var containers = Prajna.Service.DistributedFunctionBuiltIn.GetConnectedContainers();
            foreach (var container in containers )
            {
                var name = container.Item1;
                var info = container.Item2;
                Console.WriteLine($"The connected containers are = {name}, {info}");
            }
            Console.WriteLine("Cleanup...");
            Prajna.Core.Environment.Cleanup();
            Console.WriteLine("Cleanup done");
        }
    }
}
