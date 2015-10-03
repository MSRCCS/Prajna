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

module Matrix =

    let (.*) (xs: float[]) (ys: float[]) = 
        Array.fold2 (fun sum x y -> sum + x * y) 0.0 xs ys 

    let printTiming (msg: string) (x: Lazy<'b>) : 'b =
        let sw = Stopwatch.StartNew()
        printf "%s: " msg
        let ret = x.Value
        sw.Stop()
        printfn "%A" (float sw.ElapsedMilliseconds / 1000.0)
        ret

    let random = new Random(0)

    let randomVector n =  Array.init n (fun _ -> random.NextDouble())
    
    let randomMatrix m n =  Array.init m (fun _ -> randomVector n)

    /// Matrix-Vector multiplication for illustrational purposes.
    /// The overhead of distributing makes it faster to run this locally.
    /// See Matrix-Matrix for a case where the computation is actually faster distributed.
    let matrixVector (cluster: Cluster) =

        let numRows = 200
        let vectorSize = 100

        let matrix =  randomMatrix numRows vectorSize
        let vector = randomVector vectorSize

        let singleCoreResult = 
            printTiming "single core" <| lazy 
                matrix |> Array.map (fun row -> row .* vector)

        let numPartitions = Environment.ProcessorCount
        let dSet =
            printTiming "distributing" <| lazy
                let d =
                    DSet<float[]>(Cluster = cluster, NumParallelExecution = numPartitions)
                    |> DSet.distributeN numPartitions matrix
                // Force push DSet to cluster
                do d |> DSet.fold (fun _ _ -> 0) (fun _ _ -> 0) 0 |> ignore
                d

        let distributedResult = 
            printTiming "computing distributed" <| lazy 
                dSet
                |> DSet.mapi(fun i j row -> (i,j), row .* vector)
                |> DSet.toSeq
                |> Seq.sortBy fst
                |> Seq.map snd
                |> Seq.toArray

        singleCoreResult = distributedResult

    /// For simplicity, we implement our own matrix multiply out of simple dot products.
    /// Matrix-Matrix actually makes sense to do distributed, even with our simple implementation.
    /// For best performance, use a DSet of sub-matrices (float[,]), split rowwise, one sub-matrix
    /// per node, and a linear algebra library such as BLAS for the local computation.
    let matrixMatrix(cluster: Cluster) =

        // To see the speedup of parallelism, set this to something higher, like 1500
        let S = 120

        // We will multiply matrix A(M x N) by B(N x P) 
        // A is stored as a jagged array, snd will be split row-wise among nodes
        // B is stored as a *transposed* jagged array, and will be replicated to every node
        let M = S
        let N = S
        let P = S

        let A = randomMatrix M N 
        let B_transpose = randomMatrix P N

        let singleCoreResult = 
            printTiming "single core" <| lazy 
                A |> Array.map (fun row -> Array.init P (fun j -> row .* B_transpose.[j]))

        let numPartitions = cluster.NumNodes * 50

        let dSet =
            printTiming "distributing" <| lazy
                let d =
                    DSet<float[]>(Cluster = cluster, NumParallelExecution = numPartitions) 
                    |> DSet.distributeN numPartitions A
                    |> DSet.cacheInMemory
                // Force push DSet to cluster
                do d |> DSet.fold (fun _ _ -> 0) (fun _ _ -> 0) 0 |> ignore
                d 

        let distributedResult = 
            printTiming "computing distributed" <| lazy 
                dSet
                |> DSet.mapi(fun i j row -> (i,j), Array.init P (fun j -> row .* B_transpose.[j]))
                |> DSet.toSeq
                |> Seq.sortBy fst
                |> Seq.map snd
                |> Seq.toArray
  
        singleCoreResult = distributedResult

open Matrix

type MatrixVector() =
    interface IExample with
        member x.Description: string = "Distributed matrix-vector multiply"
        member x.Run(cluster: Cluster): bool = matrixVector cluster
        
type MatrixMatrix() =
    interface IExample with
        member x.Description: string = "Distributed matrix-matrix vector multiply"
        member x.Run(cluster: Cluster): bool = matrixMatrix cluster
        

