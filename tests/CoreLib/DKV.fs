namespace Prajna.Core.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Text.RegularExpressions
open System.Threading

open NUnit.Framework

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp
open Prajna.Tools

open Prajna.Tools.FSharp

[<TestFixture(Description = "Tests for DSet<'K*'V> (a DKV)")>]
type DKVTests () =
    inherit Prajna.Test.Common.Tester()

    static let mutable numDSetSort = 0 
    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize   

    let [<Literal>] defaultDKVName = "DefaultDKVForTests"
    let [<Literal>] defaultDKVNumPartitions = 16
    let [<Literal>] numDKVsPerPartitionForDefaultDKV = 4
    let defaultDKVSize = defaultDKVNumPartitions * numDKVsPerPartitionForDefaultDKV

    let defaultDKV = 
        (DSet<_> ( Name = defaultDKVName, Cluster = cluster, NumReplications = 1, NumParallelExecution = Environment.ProcessorCount ))
        |> DSet.sourceI defaultDKVNumPartitions ( fun i -> seq { for j in 0..(numDKVsPerPartitionForDefaultDKV-1) do yield (i, (j, (sprintf "%i" j))) } )

    [<Test(Description = "Test for DKV.filterByKey")>]
    member x.DKVFilterByKeyTest() =   
        let d = defaultDKV |> DSet.identity
        let fd = d.FilterByKey ( fun k -> k % 2 = 0)

        let r = fd.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDKVSize / 2, r.Length)

        for i in 0..(defaultDKVNumPartitions/2 - 1) do
            for j in 0..(numDKVsPerPartitionForDefaultDKV - 1) do
                let (a, (b, c)) = r.[i * numDKVsPerPartitionForDefaultDKV + j]
                Assert.AreEqual(i * 2, a)
                Assert.AreEqual(j, b)
                Assert.AreEqual((sprintf "%i" j), c)


    member x.MapByValueTest (mapFunc : DSet<int*(int * string)> -> DSet<int*string>) = 
        let d = defaultDKV.Identity()
                          .Map(fun (k, v) -> (k, (k * numDKVsPerPartitionForDefaultDKV + (fst v), snd v)))

        let d1 = d |> mapFunc

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sortBy (fun (k, s) -> let x = s.Split ( [|'-'|] )
                                                                         (k, (Int32.Parse(x.[0]), Int32.Parse(x.[1]))))

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDKVSize, r.Length)

        for i in 0..(defaultDKVNumPartitions - 1) do 
            for j in 0..(numDKVsPerPartitionForDefaultDKV - 1) do
                let idx = i * numDKVsPerPartitionForDefaultDKV + j
                let (a, b) = r.[idx]
                Assert.AreEqual(i, a)
                Assert.AreEqual((sprintf "%i-%i" idx j) , b)

    [<Test(Description = "Test for DKV.mapByValue")>]
    member x.DKVMapByValueTest() = 
        x.MapByValueTest (DKV.mapByValue (fun (a, b) -> sprintf "%i-%s" a b ))

    [<Test(Description = "Test for DKV.asyncMapByValue")>]
    member x.DKVAsyncMapByValueTest() = 
        x.MapByValueTest (DKV.asyncMapByValue (fun (a, b) -> async { return sprintf "%i-%s" a b }))

    [<Test(Description = "Test for DKV.parallelcMapByValue")>]
    member x.DKVParallelMapByValueTest() = 
        x.MapByValueTest (DKV.parallelMapByValue (fun (a, b) -> Tasks.Task.Run(fun _ -> sprintf "%i-%s" a b )))

    member x.RepartitionByKeyTest numPartitions elemsPerPartition partFunc = 
        let guid = Guid.NewGuid().ToString("D") 
        let d = (DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4))
                  .SourceI(numPartitions, (fun i -> seq { for j in 0..(elemsPerPartition - 1) do yield (i, (double)j) } ))
                
        let d1 = d.RepartitionByKey(partFunc)

        Assert.AreEqual(numPartitions, d1.NumPartitions)
        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * elemsPerPartition, r.Length)

        r |> Array.iteri (fun i (k, v) -> Assert.AreEqual(i / elemsPerPartition, k)
                                          Assert.AreEqual((double) (i % elemsPerPartition), v))

    [<Test(Description = "Test for DKV.repartitionByKey")>]
    member x.DKVRepartitionByKeyTest() =
        // 2 partitions, each with 1 elems
        
        // Does not change the partition
        x.RepartitionByKeyTest 2 1 (fun k -> k )
        // Switch the two elements
        x.RepartitionByKeyTest 2 1 (fun k -> (k + 1) % 2)
        // Both to partition 0
        x.RepartitionByKeyTest 2 1 (fun _ -> 0)
        // Both to partition 1
        x.RepartitionByKeyTest 2 1 (fun _ -> 1)
        
        // 8 partitions, each with 8 elems

        // Does not change the partition
        x.RepartitionByKeyTest 8 8 (fun k -> k)
        // Re-shuffle
        x.RepartitionByKeyTest 8 8 (fun k -> (k + 1) % 8)
        x.RepartitionByKeyTest 8 8 (fun k -> (k + 2) % 8)
        x.RepartitionByKeyTest 8 8 (fun k -> (k + 4) % 8)
        x.RepartitionByKeyTest 8 8 (fun k -> (k + 7) % 8)
        // All to partition 3
        x.RepartitionByKeyTest 8 8 (fun _ -> 3)
        // Only to partition 0 and 7
        x.RepartitionByKeyTest 8 8 (fun k -> if k % 2 = 0 then 0 else 7)
        // Only to partition 1, 3, 6
        x.RepartitionByKeyTest 4 4 (fun k -> if k % 3 = 0 then 1 elif k % 3 = 1 then 3 else 6)

    [<Test(Description = "Test for DKV.repartitionPByKey")>]
    member x.DKVRepartitionPByKeyTest() =
        let repartitionTest partFunc numPartitions =
            let d = defaultDKV
                     .Identity()
                     .Map(fun (k, (a, b)) -> (k, a))
                     .RepartitionPByKey(DParam (PreGroupByReserialization = 1000000, NumPartitions = numPartitions ), partFunc)
            
            Assert.AreEqual(numPartitions, d.NumPartitions)

            let r = d.ToSeq() |> Array.ofSeq |> Array.sort

            Assert.IsNotEmpty(r)
            Assert.AreEqual(defaultDKVSize, r.Length)

            r |> Array.iteri (fun i v -> Assert.AreEqual((i / numDKVsPerPartitionForDefaultDKV, i % numDKVsPerPartitionForDefaultDKV), v))

        repartitionTest (fun k -> k) defaultDKVSize
        repartitionTest (fun k-> k / 2) ((defaultDKVSize + 2 - 1)/ 2)
        repartitionTest (fun k -> (k * numDKVsPerPartitionForDefaultDKV + 1) % 2) 2
        repartitionTest (fun k -> k / 3) ((defaultDKVSize + 3 - 1) / 3)
        repartitionTest (fun k -> (k * numDKVsPerPartitionForDefaultDKV + 3) % 3) 3
        repartitionTest (fun _ -> 0) 1

    [<Test(Description = "Test for DKV.repartitionNByKey")>]
    member x.DKVRepartitionNByKeyTest() =
        let repartitionTest partFunc numPartitions =
            let d = defaultDKV
                     .Identity()
                     .Map(fun (k, (a, b)) -> (k, a))
                     .RepartitionNByKey(numPartitions, partFunc)
            
            Assert.AreEqual(numPartitions, d.NumPartitions)

            let r = d.ToSeq() |> Array.ofSeq |> Array.sort

            Assert.IsNotEmpty(r)
            Assert.AreEqual(defaultDKVSize, r.Length)

            r |> Array.iteri (fun i v -> Assert.AreEqual((i / numDKVsPerPartitionForDefaultDKV, i % numDKVsPerPartitionForDefaultDKV), v))

        repartitionTest (fun k -> k) defaultDKVSize
        repartitionTest (fun k-> k / 7) ((defaultDKVSize + 7 - 1)/ 7)
        repartitionTest (fun k -> k % 5) 5
        repartitionTest (fun k -> k % 5) 10
        repartitionTest (fun _ -> 2) 3

////    [<Test(Description = "Test: DKV.repartitionByUniqueKey")>]
////    member x.DKVRepartitionByUniqueKeyTest() =
////        let d = defaultDKV.Identity()
////        let part k = (k + 16) / 2
////        let d1 = d.RepartitionByUniqueKey(fun k -> sprintf "%i" (part k))
////                  .RowsMergeAll()
////
////        let numParts = defaultDKVNumPartitions / 2
////        Assert.AreEqual(numParts, d1.NumPartitions)
////        
////        let r = d.ToSeq() |> Array.ofSeq
////
////        for i in 0..(numParts - 1) do
////            let arr = Array.sub r (i * 2 * numDKVsPerPartitionForDefaultDKV) (2 * numDKVsPerPartitionForDefaultDKV) |> Array.sort
////            let key = part (arr.[0] |> fst)
////            for j in 0..1 do
////                let p = (arr.[j * numDKVsPerPartitionForDefaultDKV] |> fst)
////                Assert.AreEqual(key, part p)
////                for k in 0..(numDKVsPerPartitionForDefaultDKV - 1) do
////                    let (a, (b, c)) = arr.[j * numDKVsPerPartitionForDefaultDKV + k]
////                    Assert.AreEqual(p, a)
////                    Assert.AreEqual(k, b)
////                    Assert.AreEqual(sprintf "%i" k, c)

    member x.TestBinSortByKey numPart numElemsPerPart numBins (sortFunc : DSet<int * (int * int)> -> DSet<int * (int * int)>) = 
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_>( Name = guid, Cluster = cluster, SerializationLimit = 4)
                 .SourceI(numPart, (fun i -> seq { for j in 0..(numElemsPerPart - 1) do yield (i * numElemsPerPart + j, (i, j)) } ))

        let d1 = sortFunc(d)
                  .RowsMergeAll()

        let r = d1.ToSeq() |> Array.ofSeq 
        
        let numElems = numPart * numElemsPerPart

        Assert.IsNotEmpty(r)

        if r.Length <> numElems then
            r |> Array.iter (fun (k, (a, b)) -> Logger.LogF( LogLevel.Info,  fun _ -> sprintf "(%i, (%i, %i))" k a  b) )

        Assert.AreEqual(numElems, r.Length)

        let mutable numElemsSeen = 0

        let numElemsInBin key =
            let floor = numElems / numBins
            let remain = numElems - floor * numBins
            if key < remain then floor + 1 else floor

        for i in 0..(numBins - 1) do
            let p = (r.[numElemsSeen] |> fst) % numBins
            for j in 0..((numElemsInBin p) - 1) do
                let idx = numElemsSeen
                if idx < numPart * numElemsPerPart then
                    let (k, v) = r.[idx]
                    let n = p + numBins * j
                    Assert.AreEqual(n, k, sprintf "%i:%i:%i:%i:%i" i j idx p n)
                    Assert.AreEqual((n / numElemsPerPart, n % numElemsPerPart), v)
                    numElemsSeen <- numElemsSeen + 1

    [<Test(Description = "Test: DKV.binSortByKey")>]
    member x.DKVBinSortByKeyTest() =
        x.TestBinSortByKey 4 16 4 (DKV.binSortByKey (fun x -> x % 4) Comparer<int>.Default)
        x.TestBinSortByKey 4 128 4 (DKV.binSortByKey (fun x -> x % 4) Comparer<int>.Default)
                                          
    [<Test(Description = "Test: DKV.binSortNByKey")>]
    member x.DKVBinSortNByKeyTest() =
        x.TestBinSortByKey 4 16 8 (DKV.binSortNByKey 8 (fun x -> x % 8) Comparer<int>.Default)
        x.TestBinSortByKey 4 16 3 (DKV.binSortNByKey 3 (fun x -> x % 3) Comparer<int>.Default)
        x.TestBinSortByKey 9 6 7 (DKV.binSortNByKey 7 (fun x -> x % 7) Comparer<int>.Default)
                                                            
    [<Test(Description = "Test: DKV.binSortPByKey")>]
    member x.DKVBinSortPByKeyTest() =
        x.TestBinSortByKey 4 16 8 (DKV.binSortPByKey (DParam(NumPartitions = 8)) (fun x -> x % 8) Comparer<int>.Default)
        x.TestBinSortByKey 4 16 3 (DKV.binSortPByKey (DParam(NumPartitions = 3)) (fun x -> x % 3) Comparer<int>.Default)
        x.TestBinSortByKey 9 6 7 (DKV.binSortPByKey (DParam(NumPartitions = 7)) (fun x -> x % 7) Comparer<int>.Default)

    member x.CreateTwoSortedDKVsForJoin numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 numParallelExecution =
        // output:
        // * sortedA:  
        //   + # of bins: numBins
        //   + bins that contains elements:  0, 1, .., numBins1 - 1  (total: numBins1)
        //   + elements in each bin:   first numElemsInBin1 elements in the bin
        // * sortedB:  
        //   + # of bins: numBins
        //   + bins that contains elements:  numBins - numBins2, .., numBins - 1  (total: numBins2)
        //   + elements in each bin:   last numElemsInBin2 elements in the bin
        
        Assert.IsTrue(numBins >= numBins1)
        Assert.IsTrue(numBins >= numBins2)
        Assert.IsTrue(binSize >= numElemsInBin1)
        Assert.IsTrue(binSize >= numElemsInBin2)
        
        let cnt = Interlocked.Increment( &numDSetSort ) 
        // Use fix name to ease debugging. 
        // let guid1 = Guid.NewGuid().ToString("D")
        let guid1 = "SourceDKVForSort_" + cnt.ToString()
        // let guid2 = Guid.NewGuid().ToString("D")
        let cnt = Interlocked.Increment( &numDSetSort )
        let guid2 = "SourceDKVForSort_" + cnt.ToString()

        let size1 = binSize * numBins1

        let a = (DSet<int*int64> ( Name = guid1, Cluster = cluster, SerializationLimit = binSize / 2, NumParallelExecution = numParallelExecution))
                 .SourceI(size1, (fun i ->  if i % binSize < numElemsInBin1 then
                                              seq { yield (i, (int64) i) }
                                            else 
                                              Seq.empty))

        let size2 = binSize * numBins2
        let start2 = (numBins - numBins2) * binSize

        let b = (DSet<int*string> ( Name = guid2, Cluster = cluster, SerializationLimit = binSize / 2, NumParallelExecution = numParallelExecution))
                 .SourceI(size2, (fun i ->  let j = start2 + i
                                            if (j % binSize) >= (binSize - numElemsInBin2) then
                                                 seq { yield (j, (sprintf "%i" j)) }
                                            else 
                                                 Seq.empty))

        let sortedA = a.BinSortPByKey(DParam(NumPartitions = numBins), (fun k -> k / binSize), (Comparer<int>.Default))

        let sortedB = b.BinSortNByKey(numBins, (fun k -> k / binSize), (Comparer<int>.Default))

        (sortedA, sortedB)

    member x.TestJoin joinName numParallelExecution numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 (joinFunc : DSet<int*int64> -> DSet<int*string> -> DSet<int * ('U * 'V)>) verifyFunc = 
            let overlapBins = numBins1 + numBins2 - numBins
            let overlapSizeInBin = numElemsInBin1 + numElemsInBin2 - binSize
            let overlapSize = overlapBins * overlapSizeInBin
            
            let overlapBinStart = numBins1 - overlapBins
            let overlapElemStartInBin = numElemsInBin1 - overlapSizeInBin

            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### %s Test: %i bins, %i elems each bin, first DSet has bins (%i .. %i), each bin has first %i elems; second DSet has bins (%i .. %i), each bin has last %i elems; overlap bins %i, overlap elems %i #####" 
                                                           joinName numBins binSize 0 (numBins1 - 1) numElemsInBin1 overlapBinStart (numBins - 1) numElemsInBin2 overlapBins overlapSize))

            let (sortedA, sortedB) = x.CreateTwoSortedDKVsForJoin numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 numParallelExecution

            let joinedSet = joinFunc sortedA sortedB

            let r = joinedSet.ToSeq() |> Array.ofSeq |> Array.sort

            verifyFunc r overlapSize overlapBinStart overlapBins overlapElemStartInBin overlapSizeInBin

    [<Test(Description = "Test for DKV.innerJoinByMergeAfterBinSortByKey")>]
    [<TestCaseSource("DKVInnerJoinByMergeAfterBinSortByKeyTestCases")>]
    member x.DKVInnerJoinByMergeAfterBinSortByKeyTest(numBins, binSize, numBins1, numBins2, numElemsInBin1, numElemsInBin2) =
        let numParallelExecution = 1
        let joinFunc =  DKV.innerJoinByMergeAfterBinSortByKey (Comparer<int>.Default) (fun x y -> (x, y))
        let verifyFunc (r: (int * (int64 * string))[]) overlapSize overlapBinStart overlapBins overlapElemStartInBin overlapSizeInBin =
            if overlapSize = 0 then
                Assert.IsEmpty(r)
            else
                Assert.IsNotEmpty(r)
                Assert.AreEqual(overlapSize, r.Length)

                let mutable idx = 0
                for i in overlapBinStart..(overlapBinStart + overlapBins - 1) do
                    for j in overlapElemStartInBin..(overlapElemStartInBin + overlapSizeInBin - 1) do
                        let (a, (b, c)) = r.[idx]
                        let elem = i * binSize + j
                        Assert.AreEqual(elem, a)
                        Assert.AreEqual((int64) elem, b)
                        Assert.AreEqual((sprintf "%i" elem), c)
                        idx <- idx + 1                
        x.TestJoin "InnerJoin" numParallelExecution numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 joinFunc verifyFunc

    static member val DKVInnerJoinByMergeAfterBinSortByKeyTestCases = 
        [| 
            [| 7; 1; 4; 4; 1; 1 |]
            [| 7; 1; 4; 5; 1; 1 |]
            [| 6; 2; 4; 4; 2; 2 |]
            [| 15; 8; 8; 8; 8; 8 |]
            [| 24; 4; 16; 16; 4; 4 |]
            [| 24; 4; 16; 16; 3; 3 |]
            [| 24; 4; 16; 16; 4; 3 |]
            [| 24; 4; 16; 16; 3; 4 |]
            [| 96; 1; 64; 64; 1; 1 |]
            [| 112; 5; 64; 64; 3; 4 |]
        |]

    [<Test(Description = "Test for DKV.leftOuterJoinByMergeAfterBinSortByKey")>]
    [<TestCaseSource("DKVLeftOuterJoinByMergeAfterBinSortByKeyTestCases")>]
    member x.DKVLeftOuterJoinByMergeAfterBinSortByKeyTest(numBins, binSize, numBins1, numBins2, numElemsInBin1, numElemsInBin2) =
        let numParallelExecution = 0
        let joinFunc =  DKV.leftOuterJoinByMergeAfterBinSortByKey (Comparer<int>.Default) (fun x y -> (x, y))
        let verifyFunc (r: (int * (int64 * string option))[]) overlapSize overlapBinStart overlapBins overlapElemStartInBin overlapSizeInBin = 
            Assert.IsNotEmpty(r)
            Assert.AreEqual(numBins1 * numElemsInBin1, r.Length)

            let mutable idx = 0
            for i in 0..(numBins1 - 1) do
                for j in 0..(numElemsInBin1 - 1) do
                    let (a, (b, c)) = r.[idx]
                    let elem = i * binSize + j
                    Assert.AreEqual(elem, a)
                    Assert.AreEqual((int64) elem, b)
                    if i < overlapBinStart || j < overlapElemStartInBin then
                        Assert.IsTrue(Option.isNone(c))
                    else
                        Assert.IsTrue(Option.isSome(c))
                        Assert.AreEqual((sprintf "%i" elem), c.Value)
                    idx <- idx + 1

        x.TestJoin "LeftOuterJoin" numParallelExecution numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 joinFunc verifyFunc

    static member val DKVLeftOuterJoinByMergeAfterBinSortByKeyTestCases = 
        [| 
            [| 7; 1; 4; 4; 1; 1 |]
            [| 6; 2; 4; 4; 2; 2 |]
            [| 15; 8; 8; 8; 8; 8 |]
            [| 12; 8; 8; 8; 6; 5 |]
            [| 12; 8; 8; 8; 6; 4 |]
            [| 12; 8; 8; 8; 5; 6 |]
            [| 117; 4; 70; 75; 4; 3 |]
        |]

    [<Test(Description = "Test for DKV.RightJoinByMergeAfterBinSortByKey")>]
    [<TestCaseSource("DKVRightOuterJoinByMergeAfterBinSortByKeyTestCases")>]
    member x.DKVRightOuterJoinByMergeAfterBinSortByKeyTest(numBins, binSize, numBins1, numBins2, numElemsInBin1, numElemsInBin2) =
        let numParallelExecution = Environment.ProcessorCount
        let joinFunc = DKV.rightOuterJoinByMergeAfterBinSortByKey (Comparer<int>.Default) (fun x y -> (x, y) )
        let verifyFunc (r: (int * (int64 option * string))[])  overlapSize overlapBinStart overlapBins overlapElemStartInBin overlapSizeInBin = 
            Assert.IsNotEmpty(r)
            Assert.AreEqual(numBins2 * numElemsInBin2, r.Length)

            let mutable idx = 0
            for i in overlapBinStart..(numBins - 1) do
                for j in overlapElemStartInBin..(binSize - 1) do
                    let (a, (b, c)) = r.[idx]
                    let elem = i * binSize + j
                    Assert.AreEqual(elem, a)
                    if i >= overlapBinStart + overlapBins || j >= overlapElemStartInBin + overlapSizeInBin then
                        Assert.IsTrue(Option.isNone(b))
                    else
                        Assert.IsTrue(Option.isSome(b))
                        Assert.AreEqual((int64) elem, b.Value)
                    Assert.AreEqual((sprintf "%i" elem), c)
                    idx <- idx + 1

        x.TestJoin "RightOuterJoin" numParallelExecution numBins binSize numBins1 numBins2 numElemsInBin1 numElemsInBin2 joinFunc verifyFunc

    static member val DKVRightOuterJoinByMergeAfterBinSortByKeyTestCases = 
        [| 
            [| 8; 3; 3; 3; 3; 3 |]
            [| 8; 8; 3; 5; 2; 7 |]
            [| 13; 8; 8; 8; 8; 8 |]
            [| 17; 8; 8; 9; 6; 5 |]
            [| 19; 9; 8; 7; 6; 4 |]
        |]

    member x.GroupByKeyTest (groupFunc : DSet<int * (int * int)> -> DSet<int * List<int * int>>) numPartitions numElemsPerPartition numKeys = 
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "%%%% GroupByKeyTest %i %i %i" numPartitions numElemsPerPartition numKeys))
        let guid = Guid.NewGuid().ToString("D") 
        let d = (DSet<_> ( Name = guid, Cluster = cluster))
                 .SourceI(numPartitions, (fun i -> seq { for j in 0..(numElemsPerPartition - 1) do yield ( (i * numElemsPerPartition + j) % numKeys, (i, j)) } ))

        let d1 = d |> groupFunc

        let r = d1 |> DSet.toSeq |> Array.ofSeq |> Array.sortBy (fun (k, _) -> k)

        let total = numPartitions * numElemsPerPartition
        let numLists = if numKeys <= total then numKeys else total
        
        Assert.IsNotEmpty(r)
        Assert.AreEqual(numLists, r.Length)

        let numKvs = r |> Array.fold (fun  s (_ , l) -> s + l.Count) 0 

        Assert.AreEqual(total, numKvs)

        let floor = total / numKeys
        let remain = total - floor * numKeys
        
        for i in 0..(numLists - 1) do
            let (k, l) = r.[i]
            Assert.AreEqual(i, k)
            let lSize = floor + (if i < remain then 1 else 0)
            if lSize > 0 then
                Assert.IsNotEmpty(l)
                Assert.AreEqual(lSize, l.Count)
                let arr = l |> Array.ofSeq |> Array.sort
                for j in 0..(lSize - 1) do
                    let (m, n) = arr.[j]
                    let a = i + j * numKeys
                    let b = a / numElemsPerPartition
                    let c = a % numElemsPerPartition
                    Assert.AreEqual(b, m)
                    Assert.AreEqual(c, n)

    [<Test(Description = "Test for DKV.groupByKey")>]
    member x.DKVGroupByKeyTest ()=
        x.GroupByKeyTest (DKV.groupByKey) 1 1 1
        x.GroupByKeyTest (DKV.groupByKey) 4 4 4
        x.GroupByKeyTest (DKV.groupByKey) 4 4 17
        x.GroupByKeyTest (DKV.groupByKey) 4 4 24
        x.GroupByKeyTest (DKV.groupByKey) 2 16 4
        x.GroupByKeyTest (DKV.groupByKey) 4 16 4
        x.GroupByKeyTest (DKV.groupByKey) 4 16 5
        x.GroupByKeyTest (DKV.groupByKey) 5 16 6
        x.GroupByKeyTest (DKV.groupByKey) 7 23 11

    [<Test(Description = "Test for DKV.groupByKeyN")>]
    member x.DKVGroupByKeyNTest ()=
        x.GroupByKeyTest (DKV.groupByKeyN 1) 1 1 1
        x.GroupByKeyTest (DKV.groupByKeyN 10) 1 1 1
        x.GroupByKeyTest (DKV.groupByKeyN 3) 5 5 4
        x.GroupByKeyTest (DKV.groupByKeyN 5) 5 5 5
        x.GroupByKeyTest (DKV.groupByKeyN 6) 5 5 7
        x.GroupByKeyTest (DKV.groupByKeyN 12) 6 3 6
        x.GroupByKeyTest (DKV.groupByKeyN 12) 6 9 7
        x.GroupByKeyTest (DKV.groupByKeyN 1) 7 1 1
        x.GroupByKeyTest (DKV.groupByKeyN 7) 7 2 2
        x.GroupByKeyTest (DKV.groupByKeyN 13) 17 23 11
        x.GroupByKeyTest (DKV.groupByKeyN 13) 17 23 37

    [<Test(Description = "Test for DKV.reduceByKey")>]
    member x.DKVReduceByKeyTest () =
        let d = defaultDKV.Identity().MapByValue(fun (a, b) -> a)

        let d1 = d.ReduceByKey(fun a b -> a + b)

        let r = d1 |> DSet.toSeq |> Array.ofSeq |> Array.sort
        
        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDKVNumPartitions, r.Length)

        let sum = (0 + numDKVsPerPartitionForDefaultDKV - 1) * numDKVsPerPartitionForDefaultDKV / 2

        r |> Array.iteri (fun i (a, b) -> Assert.AreEqual(i, a)
                                          Assert.AreEqual(sum, b))
