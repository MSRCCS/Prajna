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
        DistributedKMeans.fs
  
    Description: 
        Distributed KMeans. Distributedly generating random vector of NUM classes that conforms to Guassian distribution. Noises may be added to classes. \n\
        Perform mapreduce and k-mena clustering on the vector

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        June. 2014
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.Serialization
open System.Threading.Tasks

open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp


open System.Runtime.InteropServices

/// Helper function on double array. 
type internal ArrayFunc() = 
    /// calculate the distance of two double array. 
    static member Distance (arr1:float[]) (arr2:float[]) = 
        Math.Sqrt( Array.fold2 ( fun sum xi yi -> sum + ( xi - yi ) * ( xi - yi ) ) 0.0 arr1 arr2 )

type internal UniformFunc() = 
    static member Random (rnd:Random) dimn =
        Array.init dimn ( fun _ -> rnd.NextDouble() )

type internal LaplacianFunc() = 
    static member Random (rnd:Random) dimn = 
        Array.init dimn ( fun _ -> - Math.Log( 1.0 - rnd.NextDouble() )  )
                                   

type internal GaussianFunc() = 
    /// Generate a pair of gaussian random variable with 0 means and variance 1
    static member BoxMuller2 (rnd:Random) =
        let mutable x1 = 0.0
        let mutable x2 = 0.0
        let mutable w = 2.0
        while w >= 1.0 do 
            x1 <- rnd.NextDouble() * 2.0 - 1.0
            x2 <- rnd.NextDouble() * 2.0 - 1.0
            w <- x1 * x1 + x2 * x2
        let r = Math.Sqrt( -2.0 * Math.Log(w) / w )
        x1 * r, x2 * r

    static member Random2 (rnd:Random) dimn =
        let v = Array.zeroCreate dimn
        for i = 0 to ( v.Length - 1 ) / 2 do
            let x1, x2 = GaussianFunc.BoxMuller2 rnd
            v.[ i*2 ] <- x1
            v.[ i*2 + 1 ] <- x1
        if v.Length%2 <> 0 then 
            let x1, _ = GaussianFunc.BoxMuller2 rnd
            v.[ v.Length - 1 ] <- x1
        v

    static member BoxMuller (rnd:Random) =
        let u1 = rnd.NextDouble()
        let u2 = rnd.NextDouble()
        Math.Sqrt( -2.0 * Math.Log( u1 ) ) * Math.Sin( 2.0 * Math.PI * u2 )

    static member Random (rnd:Random) dimn =
        Array.init dimn ( fun _ -> GaussianFunc.BoxMuller rnd )

    static member RandomMV (rnd:Random) (mean:float[]) variance =
        let var = variance / Math.Sqrt( double mean.Length )
        let v = GaussianFunc.Random rnd (mean.Length)
        Array.map2 ( fun v m -> (v * var) + m ) v mean
//        for i = 0 to v.Length - 1 do 
//            v.[i] <- ( v.[i] * var + mean.[i] )  
//        v

    static member RandomMV2 (rnd:Random) (mean:float[]) variance =
        let var = variance / Math.Sqrt( double mean.Length )
        let v = GaussianFunc.Random2 rnd (mean.Length)
        Array.map2 ( fun v m -> (v * var) + m ) v mean

let Usage = "
    Usage: Distributed KMeans. Distributedly generating random vector of NUM classes that conforms to Guassian distribution. Noises may be added to classes. \n\
        Perform mapreduce clustering on the vector (k-means are to be implemented) \n\
    Command line arguments:\n\
    -cluster    Name of cluster used \n\
    -clusterlst The cluster list file \n\
    -in         Copy into Prajna \n\
    -out        Copy outof Prajna \n\
    -local      Local directory. All files in the directories will be copy to (or from) remote \n\
    -remote     Name of the DKV\n\
    -remotes    Name of the multiple DKVs (to be unioned together)\n\
    -patterns   Name of the multiple DKVs (in pattern to be searched and unioned together)\n\
    -ver V      Select a particular DKV with Version string: in format yyMMdd_HHmmss.fff \n\
    -rep REP    Number of Replication \n\
    -slimit S   # of record to serialize \n\
    -balancer B Type of load balancer \n\
    -parallel N Number of threads used in execution \n\
    -nump N     Number of partitions \n\
    -flag FLAG  DKVFlag \n\
    -speed S    Limiting speed of each peer to S bps\n\
    -gen        Generate a random set of vectors (to be sorted/k-mean)\n\
    -test       Local test of Gaussian variable generation \n\
    -num        Number of vectors to be generated\n\
    -dim        Dimension of vectors to be generated\n\
    -class      Number of classes to generate or classify \n\
    -var        Variance of the Gaussian variable. \n\
    -noise      Add some small noise. \n\
    -mapreduce  Add map reduce operation. \n\
    -dist METHOD Calculate a distance of distribution between any two vector in the vec. \n\    
    -binsize V  bin size used in the histogram operation of distance distribution. \n\
    -seed SEED  seed of random generator \n\
    "

[<AllowNullLiteral>]
type distStatistis(binSize) = 
    member val Min = Double.MaxValue with get, set
    member val Max = Double.MinValue with get, set
    member val Bins : int64[] ref = ref null with get, set
    override x.ToString() = 
        sprintf "Min: %f Max: %f BinSize: %f Bins: %A" x.Min x.Max binSize x.Bins 

let doDistStatistics (binSize:float) (s0:distStatistis) dist = 
    let s = if Utils.IsNull s0 then distStatistis(binSize) else s0
    let nBin = Math.Max( 0, int( Math.Ceiling( dist / binSize)  ) ) // Non Negative. 
    if Utils.IsNull (!s.Bins) || (!s.Bins).Length <= nBin then 
        Array.Resize( s.Bins, nBin + 1 ) 
    (!s.Bins).[nBin] <- (!s.Bins).[nBin] + 1L
    if s.Min > dist then s.Min <- dist
    if s.Max < dist then s.Max <- dist
    s

// Aggregation function is always protected, but when it is used for fold, we need to protect it.         
let foldStatistics (s0:distStatistis) (s1:distStatistis) = 
    if Utils.IsNotNull s0 && Utils.IsNotNull s1 then 
        if s0.Min > s1.Min then s0.Min <- s1.Min
        if s0.Max < s1.Max then s0.Max <- s1.Max
        let s0size = if Utils.IsNull (!s0.Bins) then 0 else (!s0.Bins).Length
        let s1size = if Utils.IsNull (!s1.Bins) then 0 else (!s1.Bins).Length
        if s0size < s1size then 
            Array.Resize( s0.Bins, s1size )
        if s1size > 0 then 
            (!s1.Bins) |> Array.iteri ( fun i c -> (!s0.Bins).[i] <- (!s0.Bins).[i] + c )
        s0
    elif Utils.IsNull s0 then 
        s1
    else
        s0
        

let findClosestFunc( genV, meanArray ) = 
    let distArray = meanArray |> Array.map( fun cl -> ArrayFunc.Distance genV cl ) 
    let smalli = ref -1 
    let smalld = ref Double.MaxValue
    distArray |> Array.iteri( fun i d -> if !smalli<0 || d < !smalld then 
                                            smalli:=i
                                            smalld:=d
                                            )
    !smalli

let findClosestWithdFunc( genV, meanArray ) = 
    let distArray = meanArray |> Array.map( fun cl -> ArrayFunc.Distance genV cl ) 
    let smalli = ref -1 
    let smalld = ref Double.MaxValue
    distArray |> Array.iteri( fun i d -> if !smalli<0 || d < !smalld then 
                                            smalli:=i
                                            smalld:=d
                                            )
    !smalli, !smalld

let findClosestFunc1( genV, meanArray, centDist ) = 


    //let distArray = meanArray |> Array.map( fun cl -> ArrayFunc.Distance genV cl ) 
    let smalli = ref -1 
    let smalld = ref Double.MaxValue

    Array.iteri2 ( fun i cent centdist -> if centdist < !smalld then
                                                let d = ArrayFunc.Distance genV cent
                                                if !smalli<0 || d < !smalld then 
                                                    smalli:=i
                                                    smalld:=d
                                            ) meanArray centDist

//    distArray |> Array.iteri( fun i d -> if !smalli<0 || d < !smalld then 
//                                            smalli:=i
//                                            smalld:=d
//                                            )
    !smalli, !smalld


let lockobj = new Object() 
let findClosestANNFunc( genV, kdtree) =
    //ALGKDTree.alglib.kdtreequeryaknn (kdtree, genV, 1,0.99)
    0,0
//    let index = ref -1
//    lock lockobj (fun () ->
//    index := ALGKDTree.alglib.kdtreequeryaknn (kdtree, genV, 1,0.99)
//    )
//    !index

let KMeans k (key, (data:float[][])) =
    
    let randomCentroids (sample: float[] seq) (k:int) =
        let rng = new System.Random()
        let size = Seq.length sample
        let sampleArr = Seq.toArray sample
        seq { for i in 1 .. k do
                                let pick = sampleArr.[(rng.Next(size))]
                                yield pick 
            }
        |> Seq.toArray 

    let findClosest( genV, meanArray: float[][] ) = 
        meanArray
        |> Seq.mapi (fun i c ->(i, ArrayFunc.Distance genV c))
        |> Seq.minBy (fun (i,d) -> d)
        |> fst

    let Kmeans_Quantization (centroids:float[][]) (data:float[][]) =
        data
        |> Array.map (fun d -> findClosest (d, centroids))


    let AvgCentroid (current: float []) (sample: float [] seq) =
        let size = Seq.length sample
        match size with
        | 0 -> 
            current
        | _ ->
            sample
            |> Seq.reduce (fun v1 v2 ->
                Array.map2 (fun v1x v2x -> v1x + v2x) v1 v2)
            |> Array.map (fun e -> e / (float)size)


    let initcentroids = randomCentroids data k

    let mutable change = true
    let mutable centroids = initcentroids
    let mutable maxiter = 10000
    while change && maxiter >=0 do 
        maxiter <-maxiter - 1
        let mutable assignment = None
        let next = Kmeans_Quantization centroids data
        change <-
            match assignment with
                | Some(previous) ->
                    Seq.zip previous next   
                        |> Seq.exists (fun (i, j) -> not (i = j))
                | None -> true
        if change then
            centroids <-
                let assignedDataset = Seq.zip next data
                centroids
                    |> Seq.mapi (fun i centroid ->
                                        assignedDataset
                                        |> Seq.filter (fun (ci, _) -> ci = i)
                                        |> Seq.map (fun (_,obs) -> obs)
                                        |> AvgCentroid centroid)
                                        |> Seq.toArray
            assignment <- Some(next)


    let centroids_s = centroids
    let classifier = fun datapoint ->
        centroids_s
        |> Array.minBy (fun centroid -> ArrayFunc.Distance centroid datapoint)

    (centroids, classifier)



module AssemblyProperties =
// Signs the assembly in F#
    open System
    open System.Reflection;
    open System.Runtime.InteropServices;

#if DEBUG
    [<assembly: AssemblyConfiguration("Debug")>]
#else
    [<assembly: AssemblyConfiguration("Release")>]
#endif 

    do()

// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let bDistribute = parse.ParseBoolean( "-distribute", false )
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let PrajnaClusterListFile = parse.ParseString( "-clusterlst", "")
    let localdir = parse.ParseString( "-local", "" )
    let remotePatterns = parse.ParseString( "-pattern", "" )
    let remotesName = parse.ParseStrings( "-remotes", null )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let nDistMethod = parse.ParseInt( "-dist", 0 )
    let binSize = parse.ParseFloat( "-binsize", 0.01 )
    let bIn = parse.ParseBoolean( "-in", false )
    let bOut = parse.ParseBoolean( "-out", false )
    let nrep = parse.ParseInt( "-rep", 3 )
    let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
    let slimit = parse.ParseInt( "-slimit", (1<<<18) )
    let password = parse.ParseString( "-password", "" )
    let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
    let flag = parse.ParseInt( "-flag", -100 )
    let nump = parse.ParseInt( "-nump", 0 )
    let numParallel = parse.ParseInt( "-parallel", 0 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let bVerify = parse.ParseBoolean( "-verify", false )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
    let bGen = parse.ParseBoolean( "-gen", false )
    let bTest = parse.ParseBoolean( "-test", false )
    let bCon = parse.ParseBoolean( "-con", false )
    let bKmeans = parse.ParseBoolean( "-kmeans", false )
    let num = parse.ParseInt( "-num", 1000000 )
    let dim = parse.ParseInt( "-dim", 2048 )
    let nClasses = parse.ParseInt( "-class", 100 )
    let var = parse.ParseFloat( "-var", 0.1 )
    let noise = parse.ParseFloat( "-noise", Double.MinValue )
    let bMapReduce = parse.ParseBoolean( "-mapreduce", false )
    let seed = parse.ParseInt( "-seed", 0 )
    let mutable bExecute = false
    let mutable round = 10
    let mutable maxIters = 100

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let defaultSearchPattern = "*.*"
    let searchPattern = parse.ParseString( "-spattern", defaultSearchPattern )
    let searchOption = if ( parse.ParseBoolean( "-rec", false ) ) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
    
    let sha512hash (bytearray: byte[]) = 
        let sha512Hash = System.Security.Cryptography.SHA512.Create()
        let res = sha512Hash.ComputeHash( bytearray )
        BitConverter.ToInt64( res, 0 )

    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit <- 18000L
        JobDependencies.Current.JobDirectory <- "KmeansExt"
        Logger.Log( LogLevel.MildVerbose, ("attempt to load cluster information. "))
        if not (String.IsNullOrEmpty PrajnaClusterListFile) then
            Cluster.StartCluster( PrajnaClusterListFile )
        else
            Cluster.StartCluster( PrajnaClusterFile )
        let cluster = Cluster.GetCurrent()
        Logger.Log( LogLevel.MildVerbose, ("cluster established. "))
        JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.AppDomainMask
        if true then
            if bExe then 
                JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask
            let mutable t1 = (DateTime.UtcNow)
            if bGen && bTest then 
                let rnd = System.Random(seed)
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                   let arr = GaussianFunc.Random rnd 1000000
                   let sum = Array.fold ( fun sum x -> sum + x ) 0.0 arr
                   let sum2 = Array.fold ( fun sum x -> sum + x * x ) 0.0 arr
                   sprintf "Test Gaussian, sum of avg, variance of 1000000 vectors %f, %f" sum sum2 ))
                let meanArray = Array.init nClasses ( fun _ -> UniformFunc.Random rnd dim )
                let varArray = Array.create nClasses var
                let probArray = LaplacianFunc.Random rnd nClasses
                let cumArrayPre = probArray |> Array.scan ( fun sum x -> sum + x ) 0.0 
                let cumValue = cumArrayPre.[ cumArrayPre.Length - 1 ]
                let cumArray = Array.sub(cumArrayPre) 1 ( cumArrayPre.Length - 1 ) |> Array.map ( fun x -> x/cumValue ) 
                let t1 = (DateTime.UtcNow)
                let genVector = Array.init num ( fun p -> let v = rnd.NextDouble() 
                                                          let idx = ref 0
                                                          for i = 0 to cumArray.Length - 1 do
                                                            if v > cumArray.[i] then 
                                                                idx := i + 1 
                                                          let genV = GaussianFunc.RandomMV2 rnd meanArray.[!idx] varArray.[!idx]
                                                          Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Pt %d, distance to generated vector: %f, variance: %f" p (ArrayFunc.Distance genV meanArray.[!idx]) (varArray.[!idx]) ))
                                                          genV
                                               )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Generate %d gaussian vector of dimension %d with %f secs" num dim elapse.TotalSeconds ))
                for i = 0 to meanArray.Length - 1 do
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Mean of class %d : %A" i meanArray.[i]  ))
                    let distArray = Array.map ( fun mean -> ArrayFunc.Distance mean meanArray.[i] ) meanArray 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Mean of class %d distance matrix:%A " i distArray ))

                for i = 0 to genVector.Length - 1 do
                    let distArray = Array.map ( fun mean -> ArrayFunc.Distance genVector.[i] mean) meanArray 
                    printfn "Vector %d : distance %A" i distArray 
                bExecute <- true               
            elif ( bGen || bDistribute) && bIn then 
                let t1 = (DateTime.UtcNow)
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DKV setup started."))
                let startDKV = DSet<_>( Name = "VectorGen", NumReplications = nrep, 
                                        Version = ver, SerializationLimit = slimit, 
                                        Password = password, 
                                        TypeOfLoadBalancer = typeOf, 
                                        PeerRcvdSpeedLimit = rcvdSpeedLimit )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "1st DKV class done."))
                let numPartitions = if bGen then
                                        cluster.NumNodes    
                                    else 
                                                if nump > 0 then 
                                                     startDKV.NumPartitions <- nump 
                                                     nump 
                                                else 
                                                     startDKV.NumPartitions
                let partitionSizeFunc( parti ) = 
                    let ba = num / numPartitions
                    if parti < ( num % numPartitions ) then ba + 1 else ba


                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "start to generating local data." ))

                startDKV.NumParallelExecution <- numParallel
                let rnd = Random(seed)
                let meanArray = Array.init nClasses ( fun _ -> UniformFunc.Random rnd dim )
                let varArray = Array.create nClasses var
                let probArray = LaplacianFunc.Random rnd nClasses
                let cumArrayPre = probArray |> Array.scan ( fun sum x -> sum + x ) 0.0 
                let cumValue = cumArrayPre.[ cumArrayPre.Length - 1 ]
                let cumArray = Array.sub(cumArrayPre) 1 ( cumArrayPre.Length - 1 ) |> Array.map ( fun x -> x/cumValue ) 
                
                if Utils.IsNotNull localdir && localdir.Length>0 then 
                    use writeFile = FileTools.CreateFileStreamForWrite( localdir ) 
                    let fmt = Formatters.Binary.BinaryFormatter()
                    fmt.Serialize( writeFile, meanArray ) 
                    fmt.Serialize( writeFile, varArray )
                    fmt.Serialize( writeFile, cumArray )
                    writeFile.Flush()

                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "done generating local data." ))



                let initFunc( parti, serial ) = 
                    // each generated vector is with a different random seed
                    let seed = serial * numPartitions + parti
                    let rnd = Random( serial * numPartitions + parti )
                    let v = rnd.NextDouble() 
                    let idx = ref 0
                    for i = 0 to cumArray.Length - 1 do
                        if v > cumArray.[i] then 
                            idx := i + 1 
                    let genV = GaussianFunc.RandomMV2 rnd meanArray.[!idx] varArray.[!idx]
                    seed, genV
                let genFunc seed = 
                    let rnd = Random( seed )
                    let v = rnd.NextDouble() 
                    let idx = ref 0
                    for i = 0 to cumArray.Length - 1 do
                        if v > cumArray.[i] then 
                            idx := i + 1 
                    let genV = GaussianFunc.RandomMV2 rnd meanArray.[!idx] varArray.[!idx]
                    seed, genV
                    
                          
                let dkvGen0 = 
                    if bGen then 
                        startDKV |> DSet.init initFunc partitionSizeFunc 
                    elif bDistribute then 
                        let startDSet = DSet<_>( Name = "VectorGen", 
                                                    Version = ver, SerializationLimit = slimit, 
                                                    Password = password, 
                                                    TypeOfLoadBalancer = typeOf, 
                                                    PeerRcvdSpeedLimit = rcvdSpeedLimit )
                        let genDSet = if numParallel<=1 then 
                                            startDSet |> DSet.distribute (seq { 0 .. num } )
                                      else
                                            startDSet |> DSet.distributeN numParallel (seq { 0 .. num } )
                                            
                        genDSet |> DSet.map genFunc
                    else
                        failwith "Logic error, program should not reached here"     
                Logger.Log( LogLevel.MildVerbose, ("done DSet.distribute ")                                           )
                let dkvGen = dkvGen0 |> DSet.rowsReorg ( 1 <<< 18 )
                Logger.Log( LogLevel.MildVerbose, ("done DKV.reorg ")                                           )
                let reportDKV = 
                    if Utils.IsNotNull remoteDKVname && remoteDKVname.Length>0 then 
                        let mDKV = dkvGen |> DSet.map( fun (seed, genV) -> (seed, findClosestFunc( genV, meanArray)), genV )
                        let rDKV, s1 = mDKV |> DSet.split2 ( fun ((seed,cl), genV) -> seed, cl )
                                                              ( fun tuple -> tuple )
                        let saveDKV = 
                            if bMapReduce then 
                                let m1, m2 = s1 |> DSet.bypass2 
                                let mapReduce = m2 |> DSet.mapReduce ( fun (info, genV) -> let _, classi = info
                                                                                           // Logger.LogF(LogLevel.Info, ( fun _ -> sprintf "map %A to class %d" genV classi ))
                                                                                           Seq.singleton( classi, genV ) )
                                                        ( fun (cl, vecList) -> (cl, vecList.ToArray() ) ) 
                                mapReduce |> DSet.lazySaveToHDDByName (remoteDKVname+"_MapReduce") |> ignore
                                m1
                            else
                                s1
                        if noise < 0.0 then 
                            let s = saveDKV |> DSet.map(fun ((seed,cl), genV) -> seed, genV)
                            s |> DSet.lazySaveToHDDByName remoteDKVname |> ignore
//                        saveDKV.Name <- remoteDKVname
//                        let stream = saveDKV.Save()
                        else 
                            let s1, s2 = saveDKV |>  DSet.split2 ( fun ((seed,cl), genV) -> seed, genV ) 
                                                                 ( fun ((seed,cl), genV) -> let rnd = RandomWithSalt( seed ) 
                                                                                            ( seed, genV |> Array.map( fun v -> v + ( rnd.NextDouble() - 0.5) * noise ) ) )
                            s1 |> DSet.lazySaveToHDDByName remoteDKVname |> ignore
                            s2 |> DSet.lazySaveToHDDByName (remoteDKVname+"_Noise") |> ignore
                        rDKV
                    else
                        dkvGen |> DSet.map( fun (seed, genV) -> (seed, findClosestFunc( genV, meanArray) ) )
                // Ease debugging by allow only one execution. 
                reportDKV.NumParallelExecution <- numParallel
                let msgSeq = reportDKV.ToSeq()
                let nTotal = ref 0
                let nClassCount = Array.zeroCreate<_> meanArray.Length
                msgSeq 
                |> Seq.iter ( 
                    fun (seed, classi ) -> 
                        nTotal := !nTotal + 1 
                        nClassCount.[classi] <- nClassCount.[classi] + 1 
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Seed %d -> class %d" seed classi ))
                    )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Generate %d gaussian vector of dimension %d with %f secs, class distribution %A, cumulative prob %A" !nTotal dim elapse.TotalSeconds nClassCount cumArray))
                bExecute <- true
            elif bGen && bOut && Utils.IsNotNull localdir && localdir.Length>0 then 
                let t1 = (DateTime.UtcNow)
                let startDKV = 
                    if Utils.IsNotNull remoteDKVname && remoteDKVname.Length > 0 then 
                        DSet<int*float[]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                    elif Utils.IsNotNull remotesName && remotesName.Length > 0 then 
                        let dkvs0 = remotesName |> Array.map ( fun name -> DSet<_>( Name = name, PeerRcvdSpeedLimit = rcvdSpeedLimit ) )
                        let dkvs = dkvs0 |> Array.map ( fun dkv -> DSet.loadSource dkv )
                        DSet.union dkvs
                    elif Utils.IsNotNull remotePatterns && remotePatterns.Length > 0 then  
                        let dkvs = DSet.tryFind cluster remotePatterns 
                        DSet<_>.union dkvs
                    else
                        let msg = sprintf "Need to supply a DKV name via either -remote, -remotes or -pattern"
                        failwith msg
                startDKV.NumParallelExecution <- numParallel
//                printfn "%d" startDKV.NumPartitions
                let readFile = FileTools.CreateFileStreamForRead( localdir ) 
                let fmt = Formatters.Binary.BinaryFormatter()
                let meanArray = fmt.Deserialize( readFile ) :?> float[][]
                let varArray = fmt.Deserialize( readFile ) :?> float[]
                let cumArray = fmt.Deserialize( readFile ) :?> float[]
                readFile.Close()
                                    
                let reportArr = startDKV |> DSet.map ( fun (seed, genV) -> let countarray = Array.zeroCreate<int> meanArray.Length
                                                                           let id = findClosestFunc( genV, meanArray)
                                                                           countarray.[id] <- countarray.[id] + 1 
                                                                           countarray )
                                         |> DSet.reduce ( fun (c1:int[]) (c2:int[]) -> Array.map2( fun v1 v2 -> v1 + v2 ) c1 c2 )
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Gaussian vector of dimension %d class distribution %A" meanArray.Length reportArr))
                bExecute <- true
            elif bVerify && bOut && Utils.IsNotNull localdir && localdir.Length>0 then 
                let t1 = (DateTime.UtcNow)
                let startDKV = DSet<int*float[][]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit, NumParallelExecution=numParallel ) |> DSet.loadSource 
                let readFile = FileTools.CreateFileStreamForRead( localdir ) 
                let fmt = Formatters.Binary.BinaryFormatter()
                let meanArray = fmt.Deserialize( readFile ) :?> float[][]
                let varArray = fmt.Deserialize( readFile ) :?> float[]
                let cumArray = fmt.Deserialize( readFile ) :?> float[]
                readFile.Close()
//                Logger.LogF(LogLevel.Info, ( fun _ -> sprintf "Mean Array : %A" meanArray ))
                                      
                let reportArr = startDKV.Fold( ( fun (c:Dictionary<_,_>) (cl, genVarr ) -> let countarray = if Utils.IsNull c then Dictionary<_,_>() else c
                                                                                           let idArr = genVarr |> Array.map ( fun genV -> findClosestFunc( genV, meanArray) )
                                                                                           let total = idArr.Length
                                                                                           let inCorrect = idArr |> Array.filter ( fun id -> id<>cl ) 
                                                                                           if not (countarray.ContainsKey( cl )) then 
                                                                                              countarray.Item(cl) <- (total, inCorrect, 1)
                                                                                           else
                                                                                              let total1, arr1, aggCount = countarray.Item(cl)
                                                                                              countarray.Item(cl) <- (total + total1, ( Seq.append arr1 inCorrect) |> Seq.toArray, aggCount + 1  ) 
                                                                                           countarray ),
                                                ( fun (c1) (c2) -> if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
                                                                       for item in c2 do 
                                                                         let cl = item.Key
                                                                         let total1, incor1, agg1 = item.Value
                                                                         if c1.ContainsKey(cl) then 
                                                                            let total2, incor2, agg2 = c1.Item(cl) 
                                                                            c1.Item( cl ) <- ( total1+total2, (Seq.append incor1 incor2) |> Seq.toArray, agg1 + agg2 )
                                                                         else
                                                                            c1.Item( cl ) <- ( total1, incor1, agg1 )
                                                                       c1
                                                                   elif Utils.IsNotNull c1 then 
                                                                       c1
                                                                   elif Utils.IsNotNull c2 then 
                                                                       c2
                                                                   else 
                                                                       null ),
                                                (null))
                startDKV.NumParallelExecution <- numParallel                                                                                                                                
                let distribution = Array.zeroCreate<_> meanArray.Length
                let sortArr = SortedDictionary<_,_>( reportArr )
                for pair in sortArr do 
                    let cl = pair.Key
                    let total, incor, aggcount = pair.Value
                    distribution.[cl] <- total
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Gaussian vector of dimension %d, distribution: %A " meanArray.Length distribution))
                for pair in sortArr do 
                    let cl = pair.Key
                    let total, incor, aggcount = pair.Value
                    if incor.Length > 0 || aggcount <> 1 then 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Class %d: Total %d, Incorrect mapping to: %A, with aggregation count: %d" cl total incor aggcount ))
                bExecute <- true
            elif nDistMethod<>0 && Utils.IsNotNull remoteDKVname && remoteDKVname.Length>0 then 
                let t1 = (DateTime.UtcNow)
                let startDKV = DSet<int*float[]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit, NumParallelExecution=numParallel  ) |> DSet.loadSource 
                let startDSet = startDKV |> DSet.map ( fun (_, vec) -> vec ) 
                let distStatistics = 
                    match nDistMethod with
                    | 1 -> 
                        // Calculate pair wise distance with a DKV.crossjoin
                        let distDKV = DSet.crossJoin ArrayFunc.Distance startDSet startDSet
                        DSet.fold (doDistStatistics binSize) foldStatistics null distDKV
                    | _ -> 
                        let staDKV = DSet.crossJoinFold ArrayFunc.Distance (doDistStatistics binSize) null startDSet startDSet
                        DSet.fold foldStatistics foldStatistics null staDKV
                let sumDist = (!distStatistics.Bins) |> Array.sum 
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1).TotalSeconds
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "DKV %s (%d vectors, %d distributions): pairwise distance distribution: %A" remoteDKVname startDKV.Length sumDist distStatistics ))
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Execution time %f sec." elapse ))
                bExecute <- true
            elif bKmeans then

//                let readFile = FileTools.CreateFileStreamForRead( localdir ) 
//                let fmt = Formatters.Binary.BinaryFormatter()
//                let meanArray = fmt.Deserialize( readFile ) :?> float[][]
//                let varArray = fmt.Deserialize( readFile ) :?> float[]
//                let cumArray = fmt.Deserialize( readFile ) :?> float[]
//                readFile.Close()

                let k = nClasses
                let rnd = new System.Random((DateTime.UtcNow).Millisecond)

                let AddArray = Array.map2 (fun a1 a2 -> a1+a2)

                let sampleWithoutReplacement s n =
                    s
                    |> Seq.sortBy (fun _ -> rnd.Next())
                    |> Seq.take n

                let SelectRandomSample (arr:'U[]) (size:int) = 
                    for i = 0 to size - 1 do
                        let swapIndex = rnd.Next(i,arr.Length)
                        if swapIndex <> i then
                            let t = arr.[i]
                            arr.[i] <- arr.[swapIndex]
                            arr.[swapIndex] <- t
                    let ret = Array.zeroCreate size
                    Array.Copy(arr,0,ret,0,size)
                    ret
                

                let CalcCentroidDist (centroids:float[][]) = 
                    let dist = Array.zeroCreate centroids.Length 
                    for i = 0 to centroids.Length-1 do
                        dist.[i] <- (Array.create<float> centroids.Length Double.MaxValue)

                    for i = 0 to centroids.Length-1 do
                         for j = i+1 to centroids.Length-1 do
                             dist.[i].[j] <- ArrayFunc.Distance centroids.[i] centroids.[j]
                    let minCentDist = dist |> Array.map (fun item -> (Array.min item) * 0.5)

                    dist, minCentDist

               

                
                let readFile = FileTools.CreateFileStreamForRead( localdir ) 
                let fmt = Formatters.Binary.BinaryFormatter()
                let meanArray = fmt.Deserialize( readFile ) :?> float[][]
                let varArray = fmt.Deserialize( readFile ) :?> float[]
                let cumArray = fmt.Deserialize( readFile ) :?> float[]
                readFile.Close()




                // gt:
                //centroids <- meanArray


                let CalcNewCenter (oldC: float[], weight1:int) (vect:float[],weight2:int) =
                    let newCenter = Array.zeroCreate<float> dim
                    Array.iteri2 (fun i a1 a2 -> 
                                                newCenter.[i] <- ( (a1 * ((float) weight1) + a2 * ((float)weight2 )) / (((float) weight1) + ((float)weight2 )) ) 
                                 )
                                 oldC vect
                    newCenter, (weight1+weight2)

                let CalcDist (v1:float[]) (v2:float[]) = 
                    let s = Array.fold2 ( fun sum f1 f2 -> sum + (f1-f2)*(f1-f2)) 0. v1 v2
                    Math.Sqrt s 



                let elkan = true
                let algorithm = 1




                let mutable minenerge = Double.MaxValue
                let mutable result = Unchecked.defaultof<_>
                let mutable r = 0
                let mutable centroids = Unchecked.defaultof<_>
                let mutable startDKV = DSet<int*float[]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                startDKV.StorageType <- StorageKind.HDD
                startDKV.NumParallelExecution <- numParallel
                while r < round do
                    printfn "New round:%d" r
                    r <- r + 1
                    
                   
                    let e1, e2 = startDKV |> DSet.bypass2
                    startDKV <- e2
                    let selectedSample = SelectRandomSample [|0.. num-1|] k |> Array.sort


//                    let mutable centroids = (e1 |> DKV.iter ( fun (seed, genV) -> 
//                                                    let samplearray = List<float[]>()
//                                                    if Array.exists (fun elem -> elem = seed) selectedSample then
//                                                        samplearray.Add(genV)
//                                                    samplearray )
//                                                               ( fun (c1:List<float[]>) (c2:List<float[]>) -> c1.AddRange(c2) 
//                                                                                                              c1)).ToArray ()


                    centroids<- (e1 |> DSet.fold (fun (sArr:List<float[]>) (seed, genV) -> 
                                                                                        let sampleArr = 
                                                                                            if Utils.IsNull sArr then 
                                                                                                (List<float[]>()) 
                                                                                            else 
                                                                                                sArr
                                                                                        if Array.exists (fun elem -> elem = seed) selectedSample then
                                                                                            sampleArr.Add(genV)
                                                                                        sampleArr)
                                                             (fun (gv1:List<float[]>) (gv2:List<float[]>)->  
                                                                                                             if Utils.IsNotNull gv1 && Utils.IsNotNull gv2 then
                                                                                                                gv1.AddRange(gv2)
                                                                                                                gv1
                                                                                                             elif Utils.IsNotNull gv1 then
                                                                                                                gv1
                                                                                                             elif Utils.IsNotNull gv2 then
                                                                                                                gv2
                                                                                                             else 
                                                                                                                null)
                                                            (null)
                                              ).ToArray()

                    printfn "centroids generated" 
                    match algorithm with
                    | 0 ->
                    //if not elkan then 
                        let mutable energe = 10000000.
                        let mutable flg = true
                        let mutable wDKV = e2
                        let mutable iter = 0
                        while flg && iter < maxIters do
                            let t1 = (DateTime.UtcNow)
                            iter <- iter + 1
                            let s1, s2, s3 = wDKV |> DSet.bypass3
                            wDKV <- s3
                            let cent = centroids
                            let newCentroids = s1 
                                               |> DSet.map ( fun (seed, genV) -> let center = findClosestFunc ( genV, cent)
                                                                                 let dict = new Dictionary<_,_>()
                                                                                 if not (dict.ContainsKey(center)) then
                                                                                     dict.Item(center) <- (Array.zeroCreate<float> dim, 0)
                                                                                 let item = dict.Item(center) 
                                                                                 dict.Item(center) <- (CalcNewCenter item (genV,1))
                                                                                 //dict.Item(center).Add(genV)
                                                                                 dict 
                                                                 )
                                                |> DSet.reduce ( fun (c1: Dictionary<_,_>) (c2: Dictionary<_,_>) -> if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
                                                                                                                        for item in c2 do 
                                                                                                                            let cl = item.Key
                                                                                                                            let item2 = item.Value
                                                                                                                            if c1.ContainsKey(cl) then 
                                                                                                                                let item1 = c1.Item(cl) 
                                                                                                                                c1.Item( cl ) <- CalcNewCenter item1 item2
                                                                                                                            else
                                                                                                                                c1.Item( cl ) <- item2
                                                                                                                        c1
                                                                                                                     elif Utils.IsNotNull c1 then 
                                                                                                                        c1
                                                                                                                     elif Utils.IsNotNull c2 then 
                                                                                                                        c2
                                                                                                                     else 
                                                                                                                        null
                                                            )

                            let nc = new List<float[]>()                       
                            for item in newCentroids do nc.Add (fst (item.Value))
                            centroids <- nc.ToArray() 
                            let cent = centroids
                            let newAssign = s2 |> DSet.map( fun (seed, vect) ->  let center = findClosestFunc ( vect, cent)
                                                                                 let dist = CalcDist cent.[center] vect
                                                                                 seed, (center, dist)
                                                     )
                            let ne = newAssign |> DSet.fold ( fun sum (seed, (center, dist)) -> (sum + dist)) (fun c1 c2 -> (c1 + c2)) 0.
                    
                            if Math.Abs(ne - energe) <=0.01 then
                                flg <- false
                            let t2 = (DateTime.UtcNow)

                            printfn "energe: %f ; diff: %f ; Time:%f ms" ne (Math.Abs(ne - energe)) ((t2-t1).TotalMilliseconds)
                            energe <- ne
                            if minenerge > energe then
                                minenerge <- energe
                                result <- cent

                    //else 
                    | 1 ->

                        let mutable energe = 10000000.
                        let mutable flg = true
                        let mutable wDKV = e2
                    
                        //d(c1,c2) ,  s(x)
                        let mutable iter = 0
                        while flg && iter < maxIters do
                            let t1 = (DateTime.UtcNow)
                            let l = Array.create 
                            iter <- iter + 1
                            wDKV.NumParallelExecution <- numParallel
                            let s1, s2, s3 = wDKV |> DSet.bypass3
                            s1.NumParallelExecution <- numParallel
                            s2.NumParallelExecution <- numParallel
                            wDKV <- s3
                            let cent = centroids

                            let centDist, minCentDist = CalcCentroidDist cent

//                            let assignDKV = s1 |> DSet.map(fun (k,v) -> let center, dist = findClosestFunc1 ( v, cent , minCentDist)
//                                                                       (k, (center, v))
//                                                         )

//                            let newCentroids = assignDKV |> DKV.iter ( fun (k,(c, genV)) -> 
//                                                                                    let dict = new Dictionary<_,_>()
//                                                                                    if not (dict.ContainsKey(c)) then
//                                                                                        dict.Item(c) <- (Array.zeroCreate<float> dim, 0)
//                                                                                    let item = dict.Item(c) 
//                                                                                    dict.Item(c) <- (CalcNewCenter item (genV,1))
//                                                                                    dict.Item(center).Add(genV)
//                                                                                    dict 
//                                                            )
//                                                            ( fun (c1: Dictionary<_,_>) (c2: Dictionary<_,_>) -> if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
//                                                                                                                    for item in c2 do 
//                                                                                                                        let cl = item.Key
//                                                                                                                        let item2 = item.Value
//                                                                                                                        if c1.ContainsKey(cl) then 
//                                                                                                                            let item1 = c1.Item(cl) 
//                                                                                                                            c1.Item( cl ) <- CalcNewCenter item1 item2
//                                                                                                                        else
//                                                                                                                            c1.Item( cl ) <- item2
//                                                                                                                    c1
//                                                                                                                 elif Utils.IsNotNull c1 then 
//                                                                                                                    c1
//                                                                                                                 elif Utils.IsNotNull c2 then 
//                                                                                                                    c2
//                                                                                                                 else 
//                                                                                                                    null
//                                                            )
                            
                            let newCentroids = s1 
                                               |> DSet.map ( fun (seed, genV) -> let center, dist = findClosestFunc1 ( genV, cent , minCentDist)
                                                                                 let dict = new Dictionary<_,_>()
                                                                                 if not (dict.ContainsKey(center)) then
                                                                                     dict.Item(center) <- (Array.zeroCreate<float> dim, 0)
                                                                                 let item = dict.Item(center) 
                                                                                 dict.Item(center) <- (CalcNewCenter item (genV,1))
                                                                                 //dict.Item(center).Add(genV)
                                                                                 dict 
                                                                 )
                                               |> DSet.reduce
                                                            ( fun (c1: Dictionary<_,_>) (c2: Dictionary<_,_>) -> if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
                                                                                                                    for item in c2 do 
                                                                                                                        let cl = item.Key
                                                                                                                        let item2 = item.Value
                                                                                                                        if c1.ContainsKey(cl) then 
                                                                                                                            let item1 = c1.Item(cl) 
                                                                                                                            c1.Item( cl ) <- CalcNewCenter item1 item2
                                                                                                                        else
                                                                                                                            c1.Item( cl ) <- item2
                                                                                                                    c1
                                                                                                                 elif Utils.IsNotNull c1 then 
                                                                                                                    c1
                                                                                                                 elif Utils.IsNotNull c2 then 
                                                                                                                    c2
                                                                                                                 else 
                                                                                                                    null
                                                            )

                            let nc = new List<float[]>()     
                            for item in newCentroids do nc.Add (fst (item.Value))
                            centroids <- nc.ToArray() 
                            let c = centroids
                            let newAssign = s2 |> DSet.map( fun (seed, vect) ->  let center,dist = findClosestWithdFunc ( vect, c)
                                                                                 seed, (center, dist)
                                                          )
                            newAssign.NumParallelExecution <- numParallel
                            let ne = newAssign |> DSet.fold ( fun sum (seed, (center, dist)) -> (sum + dist)) (fun c1 c2 -> (c1 + c2)) 0.
                    
                            if energe - ne <=0.01 then
                                flg <- false
                            let t2 = (DateTime.UtcNow)
                            printfn "energe: %f ; diff: %f ; Time:%f ms" ne (Math.Abs(ne - energe)) ((t2-t1).TotalMilliseconds)
                            energe <- ne
                            if minenerge > energe then
                                minenerge <- energe
                                result <- c
                    | _ ->
                        ()

//
//
//
//                        let mutable energe = 10000000.
//                        let mutable flg = true
//                        let mutable wDKV = e2
//                        let mutable iter = 0
//                        while flg && iter < maxIters do
//                            let t1 = (DateTime.UtcNow)
//                            iter <- iter + 1
//                            let s1, s2, s3 = wDKV |> DKV.bypass3
//                            wDKV <- s3
//                            let c = centroids
//                            let cent = Array2D.init c.Length dim (fun i j -> c.[i].[j])
//                            let tags = Array.init c.Length (fun i -> i)
//                            let tree = ALGKDTree.alglib.kdtreebuildtagged(cent, tags, dim, 0, 2)
//    //
//    //                        for i = 1 to 1000 do
//    //                            printf "%d " (ALGKDTree.alglib.kdtreequeryaknn (tree, c.[i], 1,0.99))
//    //
//
//
//                            let newCentroids = s1 |> DKV.iter ( fun (seed, genV) -> let center = findClosestANNFunc ( genV, tree)
//                                                                                    let dict = new Dictionary<_,_>()
//                                                                                    if not (dict.ContainsKey(center)) then
//                                                                                        dict.Item(center) <- (Array.zeroCreate<float> dim, 0)
//                                                                                    let item = dict.Item(center) 
//                                                                                    dict.Item(center) <- (CalcNewCenter item (genV,1))
//                                                                                    //dict.Item(center).Add(genV)
//                                                                                    dict 
//                                                            )
//                                                            ( fun (c1: Dictionary<_,_>) (c2: Dictionary<_,_>) -> if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
//                                                                                                                    for item in c2 do 
//                                                                                                                        let cl = item.Key
//                                                                                                                        let item2 = item.Value
//                                                                                                                        if c1.ContainsKey(cl) then 
//                                                                                                                            let item1 = c1.Item(cl) 
//                                                                                                                            c1.Item( cl ) <- CalcNewCenter item1 item2
//                                                                                                                        else
//                                                                                                                            c1.Item( cl ) <- item2
//                                                                                                                    c1
//                                                                                                                 elif Utils.IsNotNull c1 then 
//                                                                                                                    c1
//                                                                                                                 elif Utils.IsNotNull c2 then 
//                                                                                                                    c2
//                                                                                                                 else 
//                                                                                                                    null
//                                                            )
//
//                            let nc = new List<float[]>()                       
//                            for item in newCentroids do nc.Add (fst (item.Value))
//                            centroids <- nc.ToArray() 
//                            let cent = centroids
//                            let newAssign = s2 |> DSet.map( fun (seed, vect) ->  let center = findClosestFunc ( vect, cent)
//                                                                                let dist = CalcDist cent.[center] vect
//                                                                                seed, (center, dist)
//                                                     )
//                            let ne = newAssign |> DSet.fold ( fun sum (seed, (center, dist)) -> (sum + dist)) (fun c1 c2 -> (c1 + c2)) 0.
//                    
//                            if Math.Abs(ne - energe) <=0.01 then
//                                flg <- false
//                            let t2 = (DateTime.UtcNow)
//                            printfn "energe: %f ; diff: %f ; Time:%f ms" ne (Math.Abs(ne - energe)) ((t2-t1).TotalMilliseconds)
//                            energe <- ne
//                            if minenerge > energe then
//                                minenerge <- energe
//                                result <- cent



                bExecute <- true
                printfn "=================Finished=================="
                printfn "=============Min Energe %f=================" minenerge
                ()


        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
