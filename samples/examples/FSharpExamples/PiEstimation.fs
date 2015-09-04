(*---------------------------------------------------------------------------
    Copyright 2013 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

    File: 
        PiEstimation.fs
  
    Description: 
        Estimate Pi
 ---------------------------------------------------------------------------*)
namespace Prajna.Examples.FSharp

open System
open System.IO

open Prajna.Core
open Prajna.Api.FSharp

open Prajna.Examples.Common

type PiEstimation ()=

    static let NumPartitions = 16
    static let NumSamplesPerPartition = 1024
    static let NumSamples = NumPartitions * NumSamplesPerPartition

    static let Estimate cluster =        
        let guid = Guid.NewGuid().ToString("D")
         
        let value =      
            DSet<_>(Name = guid, Cluster = cluster)    
            |> DSet.sourceI NumPartitions (fun i -> seq { for j in 1..NumSamplesPerPartition do yield (i * NumSamplesPerPartition + j) })  
            |> DSet.map (fun i -> let rnd = Random(i)
                                  let x = rnd.NextDouble()
                                  let y = rnd.NextDouble()
                                  if x * x + y * y < 1. then 1. else 0.)  
            |> DSet.reduce (+)
        let pi = (value * 4.0) / (float NumSamples)       

        printfn "Estimate Pi value: %f" pi

        abs (pi - Math.PI) < 0.1

    interface IExample with
        member this.Description = 
            "Estimate the value of Pi"
        member this.Run(cluster) =
            Estimate cluster
