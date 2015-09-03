//---------------------------------------------------------------------------
//    Copyright 2013 Microsoft
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.                                                      
//
//    File: 
//        PiEstimation.cs
//
//    Description: 
//        Estimate Pi           
// ---------------------------------------------------------------------------

namespace Prajna.Examples.CSharp
{
    using System;
    using System.Linq;

    using Prajna.Core;
    using Prajna.Api.CSharp;
    using Prajna.Api.CSharp.Linq;

    using Prajna.Examples.Common;

    /// <summary>
    /// Estimate the value of Pi
    /// </summary>
    public class PiEstimation : IExample
    {
        const int NumPartitions = 16;
        const int NumSamplesPerPartition = 1024;
        const int NumSamples = NumPartitions * NumSamplesPerPartition;

        private bool Estimate(Cluster cluster)
        {
            var name = Guid.NewGuid().ToString("D");

            var value =
                (new DSet<int> { Name = name, Cluster = cluster })
                .SourceI(NumPartitions, i => Enumerable.Range(1, NumSamplesPerPartition).Select(j => i * NumSamplesPerPartition + j))
                .Select(i =>
                         {
                             var rnd = new Random(i);
                             var x = rnd.NextDouble();
                             var y = rnd.NextDouble();
                             if (x * x + y * y < 1.0)
                                 return 1.0;
                             else
                                 return 0.0;
                         }
                )
                .Aggregate((a, b) => a + b);

            var pi = (value * 4.0) / NumSamples;

            Console.WriteLine("Estimate Pi value: {0}", pi);
            return Math.Abs(pi - Math.PI) < 0.1;
        }

        /// <summary>
        ///  Description of the example
        /// </summary>
        public string Description
        {
            get
            {
                return "Estimate the value of Pi";
            }
        }

        /// <summary>
        ///  Run the example
        /// </summary>
        public bool Run(Cluster cluster)
        {
            return Estimate(cluster);
        }
    }
}
