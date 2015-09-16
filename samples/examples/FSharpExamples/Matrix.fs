(*---------------------------------------------------------------------------
    Copyright 2015 Microsoft

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
        Matrix.fs
  
    Description: 
        Simple distributed matrix-vector multiply
 ---------------------------------------------------------------------------*)
namespace Prajna.Examples.FSharp

open System
open System.Diagnostics
open Prajna.Core
open Prajna.Api.FSharp

open Prajna.Examples.Common

type MatrixSample() =

    let (.*) (xs: float[]) (ys: float[]) = 
        // simulate doing some more work, 
        // e.g.: run the remaining layers of a neural net
        Threading.Thread.Sleep(3) 
        Array.fold2 (fun sum x y -> sum + x * y) 0.0 xs ys 

    let printTiming (msg: string) (x: Lazy<'b>) : 'b =
        let sw = Stopwatch.StartNew()
        let ret = x.Value
        sw.Stop()
        printfn "%s: %A" msg sw.Elapsed
        ret

    let run (cluster: Cluster) =

        let numRows = 200
        let vectorSize = 100

        let r = new Random(0)
        let randomMatrix =  Array.init numRows (fun _ -> Array.init vectorSize (fun _ -> r.NextDouble()))
        let vector = Array.init vectorSize (fun _ -> r.NextDouble())

        printf "Storing... "
        let dSet = 
            DSet<float[]>(Name = "randomMatrix-" + (Guid.NewGuid().ToString()), Cluster = cluster)
            |> DSet.distribute randomMatrix
        do dSet |> DSet.saveToHDD
        printfn "done."

        let singleCoreResult = 
            printTiming "single core" <| lazy 
                randomMatrix |> Array.map (fun row -> row .* vector)
        
        let distributedResult = 
            printTiming "distributed" <| lazy 
                dSet
                |> DSet.mapi(fun i j row -> (i,j), row .* vector)
                |> DSet.toSeq
                |> Seq.sortBy fst
                |> Seq.map snd
                |> Seq.toArray

        singleCoreResult = distributedResult

    interface IExample with
        member x.Description: string = "Distributed matrix vector multiply, simulating some extra work"
        member x.Run(cluster: Cluster): bool = run cluster
        

