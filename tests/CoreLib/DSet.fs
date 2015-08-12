namespace Prajna.Core.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading

open NUnit.Framework

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp
open Prajna.Tools

open Prajna.Tools.FSharp

[<TestFixture(Description = "Tests for type DSet")>]
type DSetTests () =

    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize   

    let [<Literal>] defaultDsetName = "DefaultDSetForTests"
    let [<Literal>] defaultDSetSize = 64
    let [<Literal>] defaultDSetNumPartitions = 64

    let defaultDSet = 
        (DSet<_> ( Name = defaultDsetName, Cluster = cluster, NumReplications = 1)) 
        |> DSet.sourceI defaultDSetSize ( fun i -> seq { yield i } )

    // To be called before each test
    [<SetUp>] 
    member x.InitTest () =
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Test %s starts (%s) #####" TestContext.CurrentContext.Test.FullName (StringTools.UtcNowToString())))

    // To be called right after each test
    [<TearDown>] 
    member x.CleanUpTest () =
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Test %s ends (%s): %s #####" TestContext.CurrentContext.Test.FullName (StringTools.UtcNowToString()) (TestContext.CurrentContext.Result.Status.ToString())))

    [<Test(Description = "Test for DSet.identity")>]
    member x.DSetIdentityTest() =
        let newDset = 
            defaultDSet |> DSet.identity
        Assert.AreEqual(sprintf "%s_i" defaultDsetName, newDset.Name)
        let result = newDset.ToSeq() |> Array.ofSeq
        Assert.IsNotEmpty(result)
        Assert.AreEqual(defaultDSetSize, result.Length)
        result |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i, v))   

    [<Test(Description = "Test for DSet.source")>]
    member x.DSetSourceTest() =       
        let guid = Guid.NewGuid().ToString("D")
        let dset = DSet<_> ( Name = guid, Cluster = cluster)
        let result = (dset |> DSet.source ( fun () -> seq { yield (sprintf "%i-%i" (System.Diagnostics.Process.GetCurrentProcess().Id) (Thread.CurrentThread.ManagedThreadId)) })).ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(result)
        Assert.AreEqual(clusterSize, result.Length)
        Assert.IsTrue(result.[0] <> result.[1])

    [<Test(Description = "Test for DSet.sourceN")>]
    member x.DSetSourceNTest() =       
        let guid = Guid.NewGuid().ToString("D")
        let dset = DSet<_> ( Name = guid, Cluster = cluster)
        let result = (dset |> (DSet.sourceN  3 (fun i -> seq { yield i } ))).ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(result)
        Assert.AreEqual(clusterSize * 3, result.Length)      
        result |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i / clusterSize, v))

    [<Test(Description = "Test for DSet.sourcei")>]
    member x.DSetSourceITest() =       
        let guid = Guid.NewGuid().ToString("D")
        let dset = DSet<_> ( Name = guid, Cluster = cluster)
                   |> DSet.sourceI 127 ( fun i -> seq { yield i })
        Assert.AreEqual(127, dset.NumPartitions)
        let result = dset.ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(result)
        Assert.AreEqual(127, result.Length)
        result |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

    [<Test(Description = "Test: create child DSets with identity ")>]
    member x.DSetCreateTwoChildrenWithIdentityTest() =       
        let guid = Guid.NewGuid().ToString("D")
        let parent = defaultDSet |> DSet.map (fun v -> v * v)
        let d1 = parent |> DSet.identity
        let d2 = parent |> DSet.identity
        let testName = TestContext.CurrentContext.Test.FullName

        d2 |> DSet.map (fun v -> let fName = sprintf "%s-%i.txt" guid v
                                 let path = Path.Combine(Path.GetTempPath(), fName)
                                 Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Test %s: to create file %s" testName path))
                                 use fs = File.Create(path)
                                 v) |> ignore

        let r1 = d1.ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(r1)
        Assert.AreEqual(defaultDSetSize, r1.Length)
        r1 |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i * i, v))
       
        // No side-effect should happen from d2's map operation
        let files = Directory.GetFiles(Path.GetTempPath(), guid + "-*")
        Assert.IsEmpty(files)
    
    [<Test(Description = "Test for DSet.choose")>]
    member x.DSetChooseTest() =
        let d = defaultDSet |> DSet.choose (fun v -> if v % 2 = 0 then v |> Some else None)
        let r = d.ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize / 2, r.Length)
        r |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i * 2, v))

    [<Test(Description = "Test for DSet.collect")>]
    member x.DSetCollectTest() =
        let dsetSize = defaultDSetSize
        let d = defaultDSet |> DSet.identity |> DSet.collect (fun v -> seq { yield v + dsetSize
                                                                             yield v + dsetSize * 2})
        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize * 2, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(i + defaultDSetSize, v))

    [<Test(Description = "Test for DSet.count")>]
    member x.DSetCountTest() =
        let c = defaultDSet |> DSet.identity |> DSet.count

        Assert.AreEqual(defaultDSetSize, c)

    [<Test(Description = "Test for DSet.crossJoin")>]
    member x.DSetCrossJoinTest() =
        let verify (d3:DSet<int*int>) d1Size d2Size =
            let r = d3.ToSeq() |> Array.ofSeq |> Array.sort
            Assert.AreEqual(d1Size * d2Size, r.Length)

            for i in 0..(d1Size - 1) do
                for j in 0..(d2Size - 1) do
                    let idx = i * d2Size + j
                    let (a, b) = r.[idx]
                    Assert.AreEqual(i, a)
                    Assert.AreEqual(j, b)

        let guid = Guid.NewGuid().ToString("D")
        let d1 = defaultDSet |> DSet.identity
        let d2 =  (DSet<_> ( Name = guid, Cluster = cluster)) 
                  |> DSet.sourceI 8 ( fun i -> seq { yield i })

        let d3 = d2 |> DSet.crossJoin (fun a b -> (a, b)) (d1)
        verify d3 defaultDSetSize 8

        let d4 = d1 |> DSet.crossJoin (fun a b -> (b, a)) (d2)
        verify d4 defaultDSetSize 8

        // Reuse d1 (which has already been multicasted in crossjoin above)
        let guid2 = Guid.NewGuid().ToString("D")
        let d4 =  (DSet<_> ( Name = guid2, Cluster = cluster)) 
                  |> DSet.sourceI 33 ( fun i -> seq { yield i })
        let d5 = d1 |> DSet.crossJoin (fun a b -> (b, a)) (d4)
        verify d5 defaultDSetSize 33

    [<Test(Description = "Test for DSet.crossJoinChoose")>]
    member x.DSetCrossJoinChooseTest() =
        let verify (d3:DSet<int*int>) d1Size d2Size =
            let r = d3.ToSeq() |> Array.ofSeq |> Array.sort
            Assert.AreEqual(d1Size * d2Size / 2, r.Length)

            for i in 0..(d1Size/2 - 1) do
                for j in 0..(d2Size - 1) do
                    let idx = i * d2Size + j
                    let (a, b) = r.[idx]
                    Assert.AreEqual(i * 2 + 1, a)
                    Assert.AreEqual(j, b)

        let guid = Guid.NewGuid().ToString("D")
        let d1 = defaultDSet |> DSet.identity
        let d2 =  (DSet<_> ( Name = guid, Cluster = cluster)) 
                  |> DSet.sourceI 7 ( fun i -> seq { yield i })

        let d3 = d2 |> DSet.crossJoinChoose (fun a b -> if a % 2 = 0 then None else (a, b) |> Some) (d1)        
        verify d3 defaultDSetSize 7

        let d4 = d1 |> DSet.crossJoinChoose (fun a b -> if b % 2 = 0 then None else (b, a) |> Some) (d2)
        verify d4 defaultDSetSize 7


    [<Test(Description = "Test for DSet.crossJoinFold")>]
    member x.DSetCrossJoinFoldTest() =
        let guid = Guid.NewGuid().ToString("D")
        let d1 = defaultDSet |> DSet.identity
        let d2 =  (DSet<_> ( Name = guid, Cluster = cluster)) 
                  |> DSet.sourceI 3 ( fun i -> seq { yield i })

        // cross join d2 to d1 then fold along d1
        let d3 = d2 |> DSet.crossJoinFold (fun a b -> (a, b)) (fun (sa, sb) (a, b) -> (sa + a, sb + b)) (0, 0) (d1)

        let r3 = d3.ToSeq() |> Array.ofSeq |> Array.sort
        Assert.AreEqual(defaultDSetSize, r3.Length)
        for i in 0..(defaultDSetSize - 1) do
            let (a, b) = r3.[i]
            Assert.AreEqual(i * 3, a)
            Assert.AreEqual(0 + 1 + 2, b)

        // cross join d1 to d2 then fold along d2
        let d4 = d1 |> DSet.crossJoinFold (fun a b -> (a, b)) (fun (sa, sb) (a, b) -> (sa + a, sb + b)) (0, 0) (d2)
        let r4 = d4.ToSeq() |> Array.ofSeq |> Array.sort
        Assert.AreEqual(3, r4.Length)
        let bSum = ((0 + defaultDSetSize - 1) * defaultDSetSize) / 2;
        for i in 0..(3 - 1) do
            let (a, b) = r4.[i]
            Assert.AreEqual(i * defaultDSetSize, a)
            Assert.AreEqual(bSum, b)

    [<Test(Description = "Test for DSet.distribute")>]
    member x.DSetDistributeTest() =      
        let distribute num = 
            let guid = Guid.NewGuid().ToString("D") 
            let s = seq { for i in 1..num do yield i }

            let d = DSet<_> ( Name = guid, Cluster = cluster)
            let newD = d |> DSet.distribute s
            Assert.AreEqual(cluster.NumNodes, newD.NumPartitions)
        
            let r = newD.ToSeq() |> Array.ofSeq

            if num = 0 then
                Assert.IsEmpty(r)
            else
                Assert.IsNotEmpty(r)
                Assert.AreEqual(num, r.Length)
                r |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i+1, v))
        distribute 0
        distribute 1
        distribute 2
        distribute 3
        distribute 4
        distribute 5
        distribute 99
        distribute 100

    [<Test(Description = "Test for DSet.distributeN")>]
    member x.DSetDistributeNTest() =      
        let distributeN totalNum numP  = 
            let guid = Guid.NewGuid().ToString("D") 
            let s = seq { for i in 1..totalNum do yield i }

            let d = DSet<_> ( Name = guid, Cluster = cluster)
            let newD = d |> DSet.distributeN numP s
            Assert.AreEqual(cluster.NumNodes * numP, newD.NumPartitions)
        
            let r = newD.ToSeq() |> Array.ofSeq
            if totalNum = 0 then
                Assert.IsEmpty(r)
            else
                Assert.IsNotEmpty(r)
                Assert.AreEqual(totalNum, r.Length)
                r |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i+1, v))
        distributeN 0 1
        distributeN 0 5
        distributeN 1 1
        distributeN 1 2
        distributeN 1 3
        distributeN 2 2
        distributeN 2 3
        distributeN 3 2
        distributeN 4 3
        distributeN 5 3
        distributeN 99 10
        distributeN 100 7

    [<Test(Description = "Test for DSet.execute and DSet.executeN")>]
    member x.DSetExecuteTest() =      
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster)

        d |> DSet.executeN 4 (fun n ->
                               let tmpFile = Path.Combine(Path.GetTempPath(), (sprintf "%s-%i-%i-%i.txt" guid (Process.GetCurrentProcess().Id) AppDomain.CurrentDomain.Id n))
                               use file = File.Create(tmpFile)
                               ()
                             )

        let di = new DirectoryInfo(Path.GetTempPath())
        let files = di.GetFiles((sprintf "%s*.txt" guid))
        Assert.IsNotEmpty(files)
        Assert.AreEqual(cluster.NumNodes * 4, files.Length)

        d |> DSet.execute (fun _ ->
                               for i in 0..3 do
                                let tmpFile = Path.Combine(Path.GetTempPath(), (sprintf "%s-%i-%i-%i.txt" guid (Process.GetCurrentProcess().Id) AppDomain.CurrentDomain.Id i))
                                File.Delete(tmpFile)
                          )

        let files = di.GetFiles((sprintf "%s*.txt" guid))
        Assert.IsEmpty(files)

    [<Test(Description = "Test for DSet.filter")>]
    member x.DSetFilterTest() =   
        let d = defaultDSet |> DSet.identity

        let fd = d |> DSet.filter (fun v ->  v % 3 = 0)

        let r = fd.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual((defaultDSetSize + 3 - 1)/ 3, r.Length)
        for i in 0..(r.Length-1) do
            Assert.AreEqual(i * 3, r.[i])

    [<Test(Description = "Test for DSet.fold")>]
    member x.DSetFoldTest() =   
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster)
                |> DSet.sourceI 127 (fun i -> seq {yield i} )

        let v = d |> DSet.fold (+) (+) 0

        let sum = (0 + 127 - 1) * 127 / 2
        Assert.AreEqual(sum, v)

    [<Test(Description = "Test for DSet.iter")>]
    member x.DSetIterTest() =
        let guid = Guid.NewGuid().ToString("D")
        let testName = TestContext.CurrentContext.Test.FullName

        defaultDSet 
        |> DSet.identity 
        |> DSet.iter (fun v ->  let fName = sprintf "%s-%i.txt" guid v
                                let path = Path.Combine(Path.GetTempPath(), fName)
                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Test %s: to create file %s" testName path))
                                use fs = File.Create(path)
                                ())

      
        for i in 0..(defaultDSetSize - 1) do
            let fName = sprintf "%s-%i.txt" guid i
            let path = Path.Combine(Path.GetTempPath(), fName)
            Assert.IsTrue(File.Exists(path))
            File.Delete path

    [<Test(Description = "Test for DSet.init")>]
    member x.DSetInitTest() =  
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster)
                |> DSet.init (fun (x, y) -> (x, y)) (fun i -> (i + 1) * 3)

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        let mutable size = 0
        for i in 0..(cluster.NumNodes - 1) do
            size <- size + (i + 1) * 3
        Assert.AreEqual(size, r.Length)
        let mutable cnt = 0
        for i in 0..(cluster.NumNodes - 1) do
            for j in 0..((i + 1) * 3 - 1) do
                let idx = cnt + j
                let (a, b) = r.[idx]
                Assert.AreEqual(a, i)
                Assert.AreEqual(b, j)
            cnt <- cnt + (i + 1) * 3
    
    [<Test(Description = "Test for DSet.initS")>]
    member x.DSetInitSTest() =
        let initSTest n =
            let guid = Guid.NewGuid().ToString("D") 
            let d = DSet<_> ( Name = guid, Cluster = cluster)
                    |> DSet.initS (fun (x, y) -> (x, y)) n

            let r = d.ToSeq() |> Array.ofSeq |> Array.sort

            Assert.AreEqual(cluster.NumNodes * n, r.Length)
        
            for i in 0..(cluster.NumNodes - 1) do
                for j in 0..(n - 1) do
                    let idx = i * n + j
                    let (a, b) = r.[idx]
                    Assert.AreEqual(a, i)
                    Assert.AreEqual(b, j)
        initSTest 1
        initSTest 2
        initSTest 3
        initSTest 11

    [<Test(Description = "Test for DSet.reduce")>]
    member x.DSetReduceTest() =   
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster)
                |> DSet.sourceI 37 (fun i -> seq {yield i * i} )

        let v = d |> DSet.reduce (+)

        let sum = [| for i in 0..36 do yield i * i |] |> Array.sum
        
        Assert.AreEqual(sum, v)

    member x.StoreLoadSourceTest numElems numPartitions numReplications =
        let guid = Guid.NewGuid().ToString("D") 
        let ver = DateTime.UtcNow

        let toSave = seq { for i in 1..numElems do yield (i, i * i) }

        let store = DSet<_>(Name = guid, Version = ver, Cluster = cluster, NumPartitions = numPartitions, NumReplications = numReplications)
        toSave |> store.Store

        let d = DSet<int * int> ( Name = guid, Cluster = null) |> DSet.loadSource

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numElems, r.Length)
        r |> Array.iteri (fun i v -> 
                            let (a, b) = v
                            Assert.AreEqual(i + 1, a)
                            Assert.AreEqual((i + 1) * (i + 1), b))

    [<Test(Description = "Test for DSet.Store and DSet.loadSource")>]
    member x.DSetStoreLoadSourceTest() =
        x.StoreLoadSourceTest 16 1 1
        x.StoreLoadSourceTest 16 2 1
        x.StoreLoadSourceTest 16 3 1
        x.StoreLoadSourceTest 16 4 1
        x.StoreLoadSourceTest 16 5 1
        x.StoreLoadSourceTest 16 7 1
        // x.StoreLoadSourceTest 16 4 2
        // x.StoreLoadSourceTest 19 2 2

    member x.MapTest numPartitions numRowsPerPartition sLimit (mapFunc : DSet<int> -> DSet<int * int>) (expectedFun : int -> int*int )=
        let guid = Guid.NewGuid().ToString("D") 

        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = sLimit)
               |> DSet.sourceI numPartitions (fun i -> seq { for j in 0..(numRowsPerPartition-1) do yield (i * numRowsPerPartition + j)})

        let d1 = d |> mapFunc

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * numRowsPerPartition, r.Length)

        r |> Array.iteri ( fun i v ->  Assert.AreEqual (expectedFun i, v))
                                                        
    [<Test(Description = "Test for DSet.map")>]
    member x.DSetMapTest() =
        x.MapTest 4 13 1 (DSet.map ( fun v -> (v, v * v))) (fun i -> (i, i * i))

    [<Test(Description = "Test for DSet.map with side effect")>]
    member x.DSetMapTest2 () =
        let guid = Guid.NewGuid().ToString("D")
        let d = defaultDSet |> DSet.identity |> DSet.map (fun v -> v * v)

        let d1 = d |> DSet.map (fun v ->  let fName = sprintf "%s-%i.txt" guid v
                                          let path = Path.Combine(Path.GetTempPath(), fName)
                                          use fs = File.Create(path)
                                          v)
                                                                                
        let r = d1.ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize, r.Length)
        r |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i * i, v))
       
        for i in 0..(defaultDSetSize - 1) do
            let fName = sprintf "%s-%i.txt" guid (i * i)
            let path = Path.Combine(Path.GetTempPath(), fName)
            Assert.IsTrue(File.Exists(path))
            File.Delete path


    member x.MapiTest numPartitions numRowsPerPartition sLimit (mapiFunc : DSet<int*int> -> DSet<int * int64 * (int * int)>) =
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = sLimit)
                |> DSet.sourceI numPartitions (fun i -> seq { for j in 0..(numRowsPerPartition - 1) do yield (i, j) })
                |> mapiFunc

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * numRowsPerPartition, r.Length)
        for i in 0..(numPartitions - 1) do
            for j in 0..(numRowsPerPartition - 1) do
                let idx = i * numRowsPerPartition + j
                let (a, b, (c, d)) = r.[idx]
                Assert.AreEqual(i, a)
                Assert.AreEqual(j, b)
                Assert.AreEqual(i, c)
                Assert.AreEqual(j, d)

    [<Test(Description = "Test for DSet.mapi")>]
    member x.DSetMapiTest() =
        x.MapiTest 7 11 4 (DSet.mapi (fun i j v -> (i, j, v)))

    [<Test(Description = "Test for DSet.mapByCollection")>]
    member x.DSetMapByCollectionTest() = 
        let guid = Guid.NewGuid().ToString("D") 

        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                 |> DSet.sourceI 5 (fun i -> seq { for j in 0..15 do yield (i * 16 + j)})

        let d1 = d
                 |> DSet.mapByCollection (fun x -> [| for i in 1..x.Length do yield (x.[i - 1], x.Length) |])

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 5, r.Length)

        r |> Array.iteri ( fun i (a, b) -> Assert.AreEqual(i, a)
                                           Assert.AreEqual(4, b))

    [<Test(Description = "Test for DSet.mapreduce")>]
    member x.DSetMapReduceTest() = 
        let d = defaultDSet 
                |> DSet.identity
                |> DSet.mapReduce (fun v -> seq { for i in 1..3 do yield (v / 2, v * i) }) ( fun (k, l) -> let a = l |> Array.ofSeq |> Array.sort
                                                                                                           (k, a))

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize / 2, r.Length)

        r |> Array.iteri ( fun i (k, arr) -> let eArr = [|
                                                            for m in 0..1 do
                                                                for n in 1..3 do
                                                                    yield (i * 2 + m) * n
                                                        |] |> Array.sort
                                             Assert.AreEqual(i, k)
                                             Assert.AreEqual(eArr, arr))

    [<Test(Description = "Test for DSet.mapReduceP")>]
    member x.DSetMapReducePTest() = 
        let param = DParam ( PreGroupByReserialization = 1000000, NumPartitions = defaultDSetNumPartitions / 2 )
        let d = defaultDSet 
                |> DSet.identity
                |> DSet.mapReduceP param (fun v -> seq { for i in 1..3 do yield (v / 2, v * i) }) ( fun (k, l) -> let a = l |> Array.ofSeq |> Array.sort
                                                                                                                  (k, a))

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize / 2, r.Length)

        r |> Array.iteri ( fun i (k, arr) -> let eArr = [|
                                                            for m in 0..1 do
                                                                for n in 1..3 do
                                                                    yield (i * 2 + m) * n
                                                        |] |> Array.sort
                                             Assert.AreEqual(i, k)
                                             Assert.AreEqual(eArr, arr))

    [<Test(Description = "Test for DSet.mapReducePWithPartitionFunction")>]
    member x.DSetMapReducePWithPartitionFunctionTest() = 
        let param = DParam ( PreGroupByReserialization = 1000000, NumPartitions = defaultDSetNumPartitions / 2 )
        let d = defaultDSet 
                |> DSet.identity
                |> DSet.mapReducePWithPartitionFunction 
                    param 
                    (fun v -> seq { for i in 1..5 do yield (v / 2, v * i) }) 
                    (fun k -> (k + 3) % param.NumPartitions)
                    (fun (k, l) -> let a = l |> Array.ofSeq |> Array.sort
                                   (k, a))

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize / 2, r.Length)

        r |> Array.iteri ( fun i (k, arr) -> let eArr = [|
                                                            for m in 0..1 do
                                                                for n in 1..5 do
                                                                    yield (i * 2 + m) * n
                                                        |] |> Array.sort
                                             Assert.AreEqual(i, k)
                                             Assert.AreEqual(eArr, arr))


    [<Test(Description = "Test for DSet.length")>]
    member x.DSetLengthTest() = 
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        let ver = DateTime.UtcNow
        
        let d = DSet<_>(Name = guid1, Version = ver, Cluster = cluster)
                |> DSet.sourceI 16 (fun i -> seq { for j in 0..8 do yield (i, j)})
                
        let d1 = d |> DSet.saveToHDD
        let d2 = d |> DSet.saveToHDDByName guid2

        let d1Reload = DSet<int * int> ( Name = guid1, Version = ver, Cluster = cluster) |> DSet.loadSource
        let d2Reload = DSet<int * int> ( Name = guid2, Version = ver, Cluster = cluster) |> DSet.loadSource

        let l1 = d1Reload |> DSet.length
        let l2 = d2Reload |> DSet.length

        Assert.AreEqual(16L * 9L, l1)
        Assert.AreEqual(16L * 9L, l2)

    [<Test(Description = "Test for DSet.localIter")>]
    member x.DSetLocalIterTest() =
        let a = Array.zeroCreate defaultDSetSize
        let i = ref 0
        let d = defaultDSet 
                |> DSet.identity
                |> DSet.localIter (fun v -> a.[!i] <- v
                                            i := !i + 1)

        let r = a |> Array.sort
        Assert.AreEqual(defaultDSetSize, !i)
        r |> Array.iteri (fun idx v -> Assert.AreEqual(idx, v))

    [<Test(Description = "Test for DSet.mix")>]
    member x.DSetMixTest() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 

        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield (i * i, i + i)})

        let d3 = d2 |> DSet.mix d1

        let r = d3.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(17, r.Length)
        r |> Array.iteri ( fun i (a, (b, c)) -> Assert.AreEqual(i, a)
                                                Assert.AreEqual(i * i , b)
                                                Assert.AreEqual(i + i , c))

    [<Test(Description = "Test for DSet.mix2")>]
    member x.DSetMix2Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 

        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 8 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 8 (fun i -> seq { yield (double)i})

        let d3 = d2 |> DSet.mix2 d1

        let r = d3.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(8, r.Length)
        r |> Array.iteri ( fun i (a, b) -> Assert.AreEqual(i, a)
                                           Assert.AreEqual((double)i , b))

    [<Test(Description = "Test for DSet.mix3")>]
    member x.DSetMix3Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        let guid3 = Guid.NewGuid().ToString("D") 
        
        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield (double)i})

        let d3 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield (sprintf "%i" i)})

        let d4 = d3 |> DSet.mix3 d1 d2

        let r = d4.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(17, r.Length)
        r |> Array.iteri ( fun i (a, b, c) -> Assert.AreEqual(i, a)
                                              Assert.AreEqual((double)i , b)
                                              Assert.AreEqual(i.ToString() , c))

    [<Test(Description = "Test for DSet.mix4")>]
    member x.DSetMix4Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        let guid3 = Guid.NewGuid().ToString("D") 
        let guid4 = Guid.NewGuid().ToString("D") 
        
        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (double)i})

        let d3 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (sprintf "%i" i)})

        let d4 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (i * i, i + i)})


        let d5 = d4 |> DSet.mix4 d1 d2 d3

        let r = d5.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(23, r.Length)
        r |> Array.iteri ( fun i (a, b, c, (d, e)) -> Assert.AreEqual(i, a)
                                                      Assert.AreEqual((double)i , b)
                                                      Assert.AreEqual(i.ToString() , c)
                                                      Assert.AreEqual(i * i , d)
                                                      Assert.AreEqual(i + i , e))

    [<Test(Description = "Test for DSet.map2")>]
    member x.DSetMap2Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 

        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 8 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 8 (fun i -> seq { yield (double)i})

        let d3 = d2 |> DSet.map2 (fun x1 x2 -> (x1, x2 + x2)) d1

        let r = d3.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(8, r.Length)
        r |> Array.iteri ( fun i (a, b) -> Assert.AreEqual(i, a)
                                           Assert.AreEqual((double)i + (double)i, b))

    [<Test(Description = "Test for DSet.map3")>]
    member x.DSetMap3Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        let guid3 = Guid.NewGuid().ToString("D") 
        
        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield i * 2})

        let d3 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 17 (fun i -> seq { yield (sprintf "%i" i)})

        let d4 = d3 |> DSet.map3 (fun x1 x2 x3 -> (x1, (sprintf "%s_%i" x3 x2))) d1 d2

        let r = d4.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(17, r.Length)
        r |> Array.iteri ( fun i (a, b) -> Assert.AreEqual(i, a)
                                           Assert.AreEqual((sprintf "%i_%i" i (i * 2)) , b))

    [<Test(Description = "Test for DSet.map4")>]
    member x.DSetMap4Test() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        let guid3 = Guid.NewGuid().ToString("D") 
        let guid4 = Guid.NewGuid().ToString("D") 
        
        let d1 = DSet<_> ( Name = guid1, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield i})

        let d2 = DSet<_> ( Name = guid2, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (double)i})

        let d3 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (sprintf "%i" i)})

        let d4 = DSet<_> ( Name = guid3, Cluster = cluster)
                 |> DSet.sourceI 23 (fun i -> seq { yield (i * i, i + i)})


        let d5 = d4 |> DSet.map4 (fun x1 x2 x3 x4 -> (x1, (x2, (x3, x4)))) d1 d2 d3

        let r = d5.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(23, r.Length)
        r |> Array.iteri ( fun i (a, (b, (c, (d, e)))) -> Assert.AreEqual(i, a)
                                                          Assert.AreEqual((double)i , b)
                                                          Assert.AreEqual(i.ToString(), c)
                                                          Assert.AreEqual(i * i , d)
                                                          Assert.AreEqual(i + i , e))

    [<Test(Description = "Test for DSet.multicast")>]
    member x.DSetMulticastTest() =
        let d1 = defaultDSet |> DSet.identity 
        let d2 = d1 |> DSet.multicast 
        
        Assert.AreEqual(cluster.NumNodes, d2.NumReplications)

        let r = d2.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize, r.Length)

        r |> Array.iteri ( fun i v ->  Assert.AreEqual (i, v))

    [<Test(Description = "Test for DSet.asyncMap")>]
    member x.DSetAsyncMapTest() =
        x.MapTest 3 32 4 (DSet.asyncMap ( fun v -> async { return (v, v * v)} )) (fun i -> (i, i * i))

    [<Test(Description = "Test for DSet.asyncMap and DSet.reorgWDegree")>]
    member x.DSetAsyncMapWDegreeTest() =
        x.MapTest 7 19 3 (fun d -> d |> DSet.reorgWDegree 16 |> DSet.asyncMap ( fun v -> async { return (v, v * v)})) (fun i -> (i, i * i))

    [<Test(Description = "Test for DSet.asyncMapi")>]
    member x.DSetAsyncMapiTest() =
        x.MapiTest 3 37 4 (DSet.asyncMapi (fun i j v -> async { return (i, j, v) }))

    [<Test(Description = "Test for DSet.asyncMapi and DSet.reorgWDegree")>]
    member x.DSetAsyncMapiWDegreeTest() =
        x.MapiTest 5 64 3 (fun d -> d |> DSet.reorgWDegree 32 |> DSet.asyncMapi (fun i j v -> async { return (i, j, v) }))

    [<Test(Description = "Test for DSet.parallelMap")>]
    member x.DSetParallelMapTest() =
        x.MapTest 4 32 4 (DSet.parallelMap ( fun v -> Tasks.Task.Run(fun _ -> (v, v * v * v)))) (fun i -> (i, i * i * i))

    [<Test(Description = "Test for DSet.asyncMapi")>]
    member x.DSetParallelMapiTest() =
        x.MapiTest 3 37 4 (DSet.parallelMapi (fun i j v ->  Tasks.Task.Run(fun _ -> (i, j, v))))

    [<Test(Description = "Test for DSet.printfn")>]
    member x.DSetPrintfnTest() =
        let d = defaultDSet |> DSet.identity
        let originalOut = Console.Out
        try
            use sw = new StringWriter()
            Console.SetOut(sw)
            d |> DSet.printfn "%i"
            let r = sw.ToString()
            let lines = r.Split([| Environment.NewLine |], StringSplitOptions.RemoveEmptyEntries)
            let a = 
                lines 
                |> Array.map (fun x -> Int32.Parse(x)) 
                |> Array.sort

            Assert.IsNotEmpty(a)
            Assert.AreEqual(defaultDSetSize, a.Length)
            a |> Array.iteri (fun i v -> Assert.AreEqual(i, v))
        finally
            Console.SetOut(originalOut)        
    
    member x.ReorgTest (func : DSet<int> -> DSet<int>) numRowsPerCollection =
        let guid = Guid.NewGuid().ToString("D") 

        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 5 (fun i -> seq { for j in 0..15 do yield (i * 16 + j)})
                |> func

        let d1 = d
                 |> DSet.mapByCollection (fun x -> [| for i in 1..x.Length do yield (x.[i - 1], x.Length) |])

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 5, r.Length)
        
        let expectedB = if numRowsPerCollection >= 16 then 16 else numRowsPerCollection

        r |> Array.iteri ( fun i (a, b) -> Assert.AreEqual(i, a)
                                           if 16 % expectedB = 0 then
                                             Assert.AreEqual(expectedB, b)
                                           else
                                             Assert.GreaterOrEqual(expectedB, b))

    [<Test(Description = "Test for DSet.rowsReorg")>]
    member x.DSetRowsReorgTest() = 
        let testRowsReorg numRowsPerCollection =
            x.ReorgTest (DSet.rowsReorg numRowsPerCollection) numRowsPerCollection

        testRowsReorg 1
        testRowsReorg 2
        testRowsReorg 3
        testRowsReorg 8
        testRowsReorg 15
        testRowsReorg 16
        testRowsReorg 17

    [<Test(Description = "Test for DSet.rowsMergeAll")>]
    member x.DSetRowsMergeAllTest() = 
        x.ReorgTest (DSet.rowsMergeAll) 16

    [<Test(Description = "Test for DSet.rowsSplit")>]
    member x.DSetRowsSplitTest() = 
        x.ReorgTest (DSet.rowsSplit) 1

    member x.RepartitionTest numPartitions elemsPerPartition partFunc = 
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI numPartitions (fun i -> seq { for j in 0..(elemsPerPartition - 1) do yield i * elemsPerPartition + j } )
        d |> x.RepartitionDSetTest numPartitions elemsPerPartition partFunc

    member x.RepartitionDSetTest numPartitions elemsPerPartition partFunc (input:DSet<int>)= 
        let d = input |> DSet.repartition partFunc

        Assert.AreEqual(numPartitions, d.NumPartitions)
        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * elemsPerPartition, r.Length)

        r |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

    [<Test(Description = "Test for DSet.repartition")>]
    member x.DSetRepartitionTest1() =
        // 2 partitions, each with 1 elems

        // Does not change the partition
        x.RepartitionTest 2 1 (fun v -> v)
        // Switch the two elements
        x.RepartitionTest 2 1 (fun v -> (v + 1) % 2)
        // Both to partition 0
        x.RepartitionTest 2 1 (fun v -> 0)
        // Both to partition 1
        x.RepartitionTest 2 1 (fun v -> 1)


        // 4 partitions, each with 4 elems

        // Does not change the partition
        x.RepartitionTest 4 4 (fun v -> v / 4)
        // Re-shuffle
        x.RepartitionTest 4 4 (fun v -> (v + 1) % 4)
        x.RepartitionTest 4 4 (fun v -> (v + 2) % 4)
        x.RepartitionTest 4 4 (fun v -> (v + 3) % 4)
        // All to partition 1
        x.RepartitionTest 4 4 (fun v -> 1)
        // Only to partition 0 and 1
        x.RepartitionTest 4 4 (fun v -> v % 2)
        // Only to partition 0, 1, 2
        x.RepartitionTest 4 4 (fun v -> v % 3)

        // 5 partitions, each with 3 elems

        // Only to partition 1, 3
        x.RepartitionTest 5 3 (fun v -> ((v % 2) + 1) * 2 - 1)
        // Re-shuffle
        x.RepartitionTest 5 3 (fun v -> (v + 1) % 5)
        x.RepartitionTest 5 3 (fun v -> (v + 4) % 5)
        // Only to partition 0, 1, 2
        x.RepartitionTest 5 3 (fun v -> v % 3)
        // Only to partition 0, 2, 4
        x.RepartitionTest 5 3 (fun v -> (v % 3) * 2)

    [<Test(Description = "Test for DSet.repartition")>]
    member x.DSetRepartitionTest2() =
        // Repartition a DSet twice

        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 16 (fun i -> seq { for j in 0..(4 - 1) do yield i * 4 + j })

        let d1 = d 
                 |> DSet.repartition (fun v -> (v % 16))
                 |> DSet.repartition (fun v -> (v % 8))

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

    [<Test(Description = "Test for DSet.repartition")>]
    member x.DSetRepartitionTest3() =
        // Repartition a DSet multiple times

        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 16 (fun i -> seq { for j in 0..(4 - 1) do yield i * 4 + j })

        let d1 = d 
                 |> DSet.repartition (fun v -> (v % 16))
                 |> DSet.repartition (fun v -> (v % 8))
                 |> DSet.repartition (fun v -> ((v % 8) + 8))
                 |> DSet.repartition (fun v -> ((v + 1) % 16))
                 |> DSet.repartition ( fun v -> (v + 3) % 16)

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

    [<Test(Description = "Test for DSet.repartition")>]
    member x.DSetRepartitionTest4() =
        // Repartition a DSet multiple times with a map transformation in between

        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 16 (fun i -> seq { for j in 0..(4 - 1) do yield i * 4 + j })

        let d1 = d 
                 |> DSet.repartition (fun v -> (v % 16))
                 |> DSet.map (fun v -> v * v)
                 |> DSet.repartition (fun v -> (v % 8))

        let r = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(i * i, v))

    [<Test(Description = "Test for DSet.repartitionP")>]
    member x.DSetRepartitionPTest() =
        let repartitionTest partFunc numPartitions =
            let d = defaultDSet 
                    |> DSet.identity
                    |> DSet.repartitionP (DParam (PreGroupByReserialization = 1000000, NumPartitions = numPartitions )) partFunc
            
            Assert.AreEqual(numPartitions, d.NumPartitions)

            let r = d.ToSeq() |> Array.ofSeq |> Array.sort

            Assert.IsNotEmpty(r)
            Assert.AreEqual(defaultDSetSize, r.Length)

            r |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

        repartitionTest (fun v -> v) defaultDSetSize
        repartitionTest (fun v -> v / 2) ((defaultDSetSize + 2 - 1)/ 2)
        repartitionTest (fun v -> v % 2) 2
        repartitionTest (fun v -> v / 3) ((defaultDSetSize + 3 - 1) / 3)
        repartitionTest (fun v -> v % 3) 3
        repartitionTest (fun v -> v % 3) 4
        repartitionTest (fun v -> 0) 1

    [<Test(Description = "Test for DSet.repartitionN")>]
    member x.DSetRepartitionNTest() =
        let repartitionTest partFunc numPartitions =
            let d = defaultDSet 
                    |> DSet.identity
                    |> DSet.repartitionN numPartitions partFunc
            
            Assert.AreEqual(numPartitions, d.NumPartitions)

            let r = d.ToSeq() |> Array.ofSeq |> Array.sort

            Assert.IsNotEmpty(r)
            Assert.AreEqual(defaultDSetSize, r.Length)

            r |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

        repartitionTest (fun v -> v) defaultDSetSize
        repartitionTest (fun v -> v / 2) ((defaultDSetSize + 2 - 1)/ 2)
        repartitionTest (fun v -> v % 2) 5
        repartitionTest (fun v -> v / 3) ((defaultDSetSize + 3 - 1) / 3)
        repartitionTest (fun v -> v % 3) 4
        repartitionTest (fun v -> 0) 1
        repartitionTest (fun v -> 2) 3

    // Helper function to test replicate and by pass
    member x.TestBypass (func : DSet<int> -> DSet<int> * (DSet<int> [])) =
        let guid = Guid.NewGuid().ToString("D")
        let parent = defaultDSet |> DSet.identity |> DSet.map (fun v -> let x = v * v
                                                                        x)

        // d0 is for pull data
        // ds contains n DSets for push data
        let d0, ds = parent |> func

        let testName = TestContext.CurrentContext.Test.FullName

        // Set up the push data graph 
        let rs = ds |> Array.mapi ( fun i d -> d |> DSet.map (fun v ->  let fName = sprintf "%s-%i-%i.txt" guid i v
                                                                        let path = Path.Combine(Path.GetTempPath(), fName)
                                                                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Test %s: to create file %s" testName path))
                                                                        use fs = File.Create(path)
                                                                        v))
                                                                                
        // pull data from d0, should trigger evaluation of parent, which pushes data to DSets in ds
        let r0 = d0.ToSeq() |> Array.ofSeq

        Assert.IsNotEmpty(r0)
        Assert.AreEqual(defaultDSetSize, r0.Length)
        r0 |> Array.sort |> Array.iteri (fun i v -> Assert.AreEqual(i * i, v))
       
        for i in 0..(ds.Length - 1) do
            for j in 0..(defaultDSetSize - 1) do
                let fName = sprintf "%s-%i-%i.txt" guid i (j * j)
                let path = Path.Combine(Path.GetTempPath(), fName)
                Assert.IsTrue(File.Exists(path))
                File.Delete path

    [<Test(Description = "Test: create 2 child DSets with bypass")>]
    member x.DSetBypassTest() =
        x.TestBypass (fun d -> let (a,b) = d |> DSet.bypass
                               (a, [| b |]))

    [<Test(Description = "Test: create 6 child DSets with bypassN")>]
    member x.DSetBypassNTest() =
        x.TestBypass (fun d -> let (a, b) = d |> DSet.bypassN 6
                               (a, [| b.[0]; b.[1]; b.[2]; b.[3]; b.[4] |]))

    [<Test(Description = "Test: create 2 child DSets with bypass2")>]
    member x.DSetBypass2Test() =
        x.TestBypass (fun d -> let (a, b) = d |> DSet.bypass2
                               (a, [| b |]))

    [<Test(Description = "Test: create 3 child DSets with bypass3")>]
    member x.DSetBypass3Test() =
        x.TestBypass (fun d -> let (a, b, c) = d |> DSet.bypass3
                               (a, [| b; c|]))

    [<Test(Description = "Test: create 4 child DSets with bypass4")>]
    member x.DSetBypass4Test() =
        x.TestBypass (fun x -> let (a, b, c, d) = x |> DSet.bypass4
                               (a, [| b; c; d|]))

    member x.SplitTest (func : DSet<int * (int * int)> -> (DSet<int> []) * DSet<int * int>) =
        let guid = Guid.NewGuid().ToString("D") 

        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 16 (fun i -> seq { for j in 0..15 do yield (i, (i, j))})

        let (rs, last) = d |> func

        for i in 0..(rs.Length - 1) do
            let r = rs.[i].ToSeq() |> Array.ofSeq |> Array.sort
            Assert.IsNotEmpty(r)
            Assert.AreEqual(16 * 16, r.Length)
            r |> Array.iteri (fun j v -> Assert.AreEqual(j/16 * (i + 1), v))

        let r = last.ToSeq() |> Array.ofSeq |> Array.sort
        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 16, r.Length)
        for i in 0..15 do
            for j in 0..15 do
                let idx = i * 16 + j
                let (a, b) = r.[idx]
                Assert.AreEqual(i, a)
                Assert.AreEqual(j, b)

    [<Test(Description = "Test: DSet.split2")>]
    member x.DSetSplit2Test() =
        x.SplitTest (fun d -> let (s1, s2) = d |> DSet.split2 (fun (x, y) -> x) (fun (x, y) -> y)
                              ([| s1 |], s2))

    [<Test(Description = "Test: DSet.split3")>]
    member x.DSetSplit3Test() =
        x.SplitTest (fun d -> let (s1, s2, s3) = d |> DSet.split3 (fun (x, y) -> x) (fun (x, y) -> x * 2) (fun (x, y) -> y)
                              ([| s1; s2 |], s3))

    [<Test(Description = "Test: DSet.split4")>]
    member x.DSetSplit4Test() =
        x.SplitTest (fun d -> let (s1, s2, s3, s4) = d |> DSet.split4 (fun (x, y) -> x) (fun (x, y) -> x * 2) (fun (x, y) -> x * 3) (fun (x, y) -> y)
                              ([| s1; s2; s3 |], s4))

    [<Test(Description = "Test: DSet.tryFind")>]
    member x.DSetTryFindTest() =
        let guid1 = Guid.NewGuid().ToString("D") 
        let guid2 = Guid.NewGuid().ToString("D") 
        
        let d = DSet<_> ( Name = guid1, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 16 (fun i -> seq { yield i })

        let d1 = d |> DSet.saveToHDD
        let d2 = d |> DSet.saveToHDDByName guid2

        let found1 = DSet.tryFind cluster guid1
        Assert.IsNotEmpty(found1)
        Assert.AreEqual(1, found1.Length)
        let r1 = found1.[0].ToSeq() |> Array.ofSeq |> Array.sort

        r1 |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

        let found2 = DSet.tryFind cluster guid2
        Assert.IsNotEmpty(found2)
        Assert.AreEqual(1, found1.Length)
        let r2 = found2.[0].ToSeq() |> Array.ofSeq |> Array.sort

        r2 |> Array.iteri (fun i v -> Assert.AreEqual(i, v))

    [<Test(Description = "Test: DSet.union")>]
    member x.DSetUnionTest() =
        let unionTest numInputs size =
            let inputs = seq {
                           for i in 0..(numInputs - 1) do
                               let guid = Guid.NewGuid().ToString("D") 
                               let start = i * size
                               let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                                       |> DSet.sourceI size (fun j -> seq { let v = start + j
                                                                          yield (v, v * v, v + v) })
                               yield d
                         }
            let rd = inputs |> DSet.union

            let r = rd.ToSeq() |> Array.ofSeq |> Array.sort
            Assert.IsNotEmpty(r)
            Assert.AreEqual(numInputs * size, r.Length)
            r |> Array.iteri (fun i v -> let (a, b, c) = v
                                         Assert.AreEqual(i, a)
                                         Assert.AreEqual(i * i, b)
                                         Assert.AreEqual(i + i, c))
        
        unionTest 1 1
        unionTest 1 3
        unionTest 1 16
        unionTest 2 5
        unionTest 2 16
        unionTest 3 16
        unionTest 8 16

    [<Test(Description = "Test: DSet.binSort")>]
    member x.DSetBinSortTest() =
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 4 (fun i -> seq { for j in 0..15 do yield i * 16 + j } )

        let d1 = d 
                 |> DSet.binSort (fun x -> x % 4) Comparer<int>.Default 
                 |> DSet.rowsMergeAll
        
        let r = d1.ToSeq() |> Array.ofSeq 

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)

        for i in 0..3 do
            let p = r.[i * 16] % 4
            for j in 0..15 do
                let idx = i * 16 + j
                Assert.AreEqual(p + 4 * j, r.[idx])
                                                    
    [<Test(Description = "Test: DSet.binSortP")>]
    member x.DSetBinSortPTest() =
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 4 (fun i -> seq { for j in 0..15 do yield i * 16 + j } )

        let d1 = d 
                 |> DSet.binSortP  (DParam(PreGroupByReserialization = 1000000, NumPartitions = 8)) (fun x -> x % 8) Comparer<int>.Default 
                 |> DSet.rowsMergeAll
        
        let r = d1.ToSeq() |> Array.ofSeq 

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)

        for i in 0..7 do
            let p = r.[i * 8] % 8
            for j in 0..7 do
                let idx = i * 8 + j
                Assert.AreEqual(p + 8 * j, r.[idx])

    [<Test(Description = "Test: DSet.binSortN")>]
    member x.DSetBinSortNTest() =
        let guid = Guid.NewGuid().ToString("D") 
        let d = DSet<_> ( Name = guid, Cluster = cluster, SerializationLimit = 4)
                |> DSet.sourceI 4 (fun i -> seq { for j in 0..15 do yield i * 16 + j } )

        let d1 = d 
                 |> DSet.binSortN  8 (fun x -> x % 8) Comparer<int>.Default 
                 |> DSet.rowsMergeAll
        
        let r = d1.ToSeq() |> Array.ofSeq 

        Assert.IsNotEmpty(r)
        Assert.AreEqual(16 * 4, r.Length)

        for i in 0..7 do
            let p = r.[i * 8] % 8
            for j in 0..7 do
                let idx = i * 8 + j
                Assert.AreEqual(p + 8 * j, r.[idx])

    [<Test(Description = "Test: DSet.cacheInMemory")>]
    member x.DSetCacheInMemoryTest() = 
        let guid = Guid.NewGuid().ToString("D") 
        let testName = TestContext.CurrentContext.Test.FullName        

        let d = defaultDSet |> DSet.identity
        
        let d1 = d 
                |> DSet.map (fun v -> let fName = sprintf "%s-%i.txt" guid v
                                      let path = Path.Combine(Path.GetTempPath(), fName)
                                      Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Test %s: to create file %s" testName path))
                                      use fs = File.Create(path)
                                      v)
                |> DSet.map(fun v -> v * v)
                |> DSet.cacheInMemory
        
        // Action: trigger d1 to be cached, the above map operation with side effect (create file) should be executed
        let c = d1 |> DSet.identity |> DSet.count

        Assert.AreEqual(defaultDSetSize, c)

        // Verify files are created 
        for i in 0..(defaultDSetSize - 1) do
            let fName = sprintf "%s-%i.txt" guid i
            let path = Path.Combine(Path.GetTempPath(), fName)
            Assert.IsTrue(File.Exists(path))
            File.Delete path

        let d2 = d1 |> DSet.map (fun v -> v + v)

        let r = d2.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(2 * i * i, v))

        // Since d1 is cached already, the map operation that has side effect is not executed again, thus no files created
        // Note: cacheInMemory is a hint, it does not ensure the DSet being cached. Thus strictly speaking, it's still possible
        // to trigger evaluation of transformations before the cache operation. However, in the setting of unit test, we assume
        // there are enough resource, thus the cache will always happen.
        let files = Directory.GetFiles(Path.GetTempPath(), sprintf "%s-*.txt" guid)
        Assert.IsEmpty(files)

    [<Test(Description = "Test: a DSet is reused")>]
    member x.DSetReuseTest1() = 
        let guid = Guid.NewGuid().ToString("D") 
        let testName = TestContext.CurrentContext.Test.FullName

        let d = defaultDSet |> DSet.identity
        
        let d1 = d |> DSet.map(fun v -> v * v)
                
        let c = d1 |> DSet.count

        Assert.AreEqual(defaultDSetSize, c)

        let d2 = d1 |> DSet.map (fun v -> v + v)

        let r = d2.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(defaultDSetSize, r.Length)
        r |> Array.iteri (fun i v -> Assert.AreEqual(2 * i * i, v))

    [<Test(Description = "Test: a DSet is reused")>]
    member x.DSetReuseTest2() = 
        let guid = Guid.NewGuid().ToString("D") 
        let testName = TestContext.CurrentContext.Test.FullName

        let d = defaultDSet |> DSet.identity
        
        let d1 = d |> DSet.map(fun v -> v * v)
                
        let r1 = d1.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r1)
        Assert.AreEqual(defaultDSetSize, r1.Length)
        r1 |> Array.iteri (fun i v -> Assert.AreEqual(i * i, v))

        let d2 = d1 |> DSet.map (fun v -> v + v)

        let r2 = d2.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r2)
        Assert.AreEqual(defaultDSetSize, r2.Length)
        r2 |> Array.iteri (fun i v -> Assert.AreEqual(2 * i * i, v))

    member x.SaveToHDDTest numPartitions elemsPerPartition byName =
        let guid = Guid.NewGuid().ToString("D")
        let ver = DateTime.UtcNow
        
        let dset = DSet<_>( Name = guid, Cluster = cluster)
                  |> DSet.sourceI numPartitions ( fun i -> seq { for j in 0..(elemsPerPartition - 1) do yield (i, j)})
        
        let name = 
            if byName then 
                let newName = Guid.NewGuid().ToString("D") 
                dset |> DSet.saveToHDDByName newName
                newName
            else
                dset |> DSet.saveToHDD
                guid

        let d = DSet<int*int> ( Name = name, Cluster = cluster) |> DSet.loadSource

        Assert.AreEqual(name, d.Name)
        Assert.AreEqual(numPartitions, d.NumPartitions)

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * elemsPerPartition, r.Length)
        r |> Array.iteri (fun i v -> 
                            let (a, b) = v
                            Assert.AreEqual(i / elemsPerPartition, a)
                            Assert.AreEqual(i % elemsPerPartition, b))

    [<Test(Description = "Test: DSet.saveToHDD")>]
    member x.DSetSaveToHDDTest() = 
        x.SaveToHDDTest 4 4 false
        x.SaveToHDDTest 8 10 false
        x.SaveToHDDTest 7 19 false
        x.SaveToHDDTest 1 1 false

    [<Test(Description = "Test: DSet.saveToHDDByName")>]
    member x.DSetSaveToHDDByNameTest() = 
        x.SaveToHDDTest 3 7 true
        x.SaveToHDDTest 2 9 true
        x.SaveToHDDTest 6 6 true
        x.SaveToHDDTest 4 1024 true

    member x.LazySaveToHDDTest numPartitions elemsPerPartition byName =
        let guid = Guid.NewGuid().ToString("D")
        let ver = DateTime.UtcNow
        
        let dset = DSet<_>( Name = guid, Cluster = cluster)
                  |> DSet.sourceI numPartitions ( fun i -> seq { for j in 0..(elemsPerPartition - 1) do yield (i, j)})

        let (pull, push) = dset |> DSet.bypass
        push.Name <- guid
        let name = 
            if byName then 
                let newName = Guid.NewGuid().ToString("D") 
                push |> DSet.lazySaveToHDDByName newName
                newName
            else
                push |> DSet.lazySaveToHDD
                guid

        // trigger the pull/push dataflow
        let cnt = pull |> DSet.count
        Assert.AreEqual(numPartitions * elemsPerPartition, cnt)

        let d = DSet<int*int> ( Name = name, Cluster = cluster) |> DSet.loadSource

        Assert.AreEqual(name, d.Name)
        Assert.AreEqual(numPartitions, d.NumPartitions)

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * elemsPerPartition, r.Length)
        r |> Array.iteri (fun i v -> 
                            let (a, b) = v
                            Assert.AreEqual(i / elemsPerPartition, a)
                            Assert.AreEqual(i % elemsPerPartition, b))

    [<Test(Description = "Test: DSet.lazySaveToHDD")>]
    member x.DSetLazySaveToHDDTest() = 
        x.LazySaveToHDDTest 1 32 false
        x.LazySaveToHDDTest 3 1 false
        x.LazySaveToHDDTest 7 2 false
        x.LazySaveToHDDTest 10 9 false

    [<Test(Description = "Test: DSet.lazySaveToHDDByName")>]
    member x.DSetLazySaveToHDDByNameTest() = 
        x.LazySaveToHDDTest 3 3 true
        x.LazySaveToHDDTest 7 17 true
        x.LazySaveToHDDTest 9 9 true
        x.LazySaveToHDDTest 2 18 true

    member x.SaveToHDDTestWithMonitor numPartitions elemsPerPartition byName =
        let guid = Guid.NewGuid().ToString("D")
        let ver = DateTime.UtcNow
        
        let dset = DSet<_>( Name = guid, Cluster = cluster)
                   |> DSet.sourceI numPartitions ( fun i -> seq { for j in 0..(elemsPerPartition - 1) do yield (i, j)})
        
        let monitorFunc (k, v) =
            if v = elemsPerPartition / 2 || v = elemsPerPartition - 1 then true else false

        let locals = Dictionary<int, List<int>>()
        let localIterFunc (k, v) = 
            let b, l = locals.TryGetValue(k)
            if b then
                l.Add(v)
            else 
                let l = List<int>()
                l.Add(v)
                locals.[k] <- l

        let name = 
            if byName then 
                let newName = Guid.NewGuid().ToString("D") 
                dset |> DSet.saveToHDDByNameWithMonitor monitorFunc localIterFunc newName
                newName
            else
                dset |> DSet.saveToHDDWithMonitor monitorFunc localIterFunc
                guid

        let d = DSet<int*int> ( Name = name, Cluster = cluster) |> DSet.loadSource

        Assert.AreEqual(name, d.Name)
        Assert.AreEqual(numPartitions, d.NumPartitions)

        let r = d.ToSeq() |> Array.ofSeq |> Array.sort

        Assert.IsNotEmpty(r)
        Assert.AreEqual(numPartitions * elemsPerPartition, r.Length)
        r |> Array.iteri (fun i v -> 
                            let (a, b) = v
                            Assert.AreEqual(i / elemsPerPartition, a)
                            Assert.AreEqual(i % elemsPerPartition, b))

        for i in 0..(numPartitions - 1) do
            let b, l = locals.TryGetValue(i)
            Assert.IsTrue(b)
            let e1 = elemsPerPartition / 2 
            let e2 = elemsPerPartition - 1 
            let cnt = if e1 <> e2 then 2 else 1
            Assert.IsNotEmpty(l)
            Assert.AreEqual(cnt, l.Count)
            let arr = l |> Array.ofSeq
            Assert.AreEqual(e1, arr.[0])
            if cnt = 2 then
                Assert.AreEqual(e2, arr.[1])

    [<Test(Description = "Test: DSet.SaveToHDDWithMonitor")>]
    member x.DSetSaveToHDDWithMonitorTest() = 
        x.SaveToHDDTestWithMonitor 1 1 false
        x.SaveToHDDTestWithMonitor 2 1 false
        x.SaveToHDDTestWithMonitor 16 2 false
        x.SaveToHDDTestWithMonitor 16 8 false

    [<Test(Description = "Test: DSet.SaveToHDDByNameWithMonitor")>]
    member x.DSetSaveToHDDByNameWithMonitorTest() = 
        x.SaveToHDDTestWithMonitor 7 7 true
        x.SaveToHDDTestWithMonitor 7 9 true
        x.SaveToHDDTestWithMonitor 9 11 true
        x.SaveToHDDTestWithMonitor 11 13 true
