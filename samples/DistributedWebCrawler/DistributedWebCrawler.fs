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
        DistributedWebCrawler.fs
  
    Description: 
        Distributed Web Crawler. Give a file where one of the column is a URL. The crawler will parallel launch N tasks to crawl the web and save the content to DKV. 
        You may choose to perform the distributed web crawl via either async workflow or tasks.

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

let Usage = "
    Usage: Distributed Web Crawler. Give a file where one of the column is a URL. The crawler will parallel launch N tasks to crawl the web and save the content to DKV. \n\
        You may choose to perform the distributed web crawl via either async workflow or tasks \n\
    Command line arguments:\n\
    -in         Copy into Prajna \n\
    -out        Copy outof Prajna \n\
    -local      Local directory. All files in the directories will be copy to (or from) remote \n\
    -remote     Name of the distributed Prajna folder\n\
    -ver V      Select a particular DKV with Version string: in format yyMMdd_HHmmss.fff \n\
    -rep REP    Number of Replication \n\
    -slimit S   # of record to serialize \n\
    -balancer B Type of load balancer \n\
    -nump N     Number of partitions \n\
    -speed S    Limiting speed of each peer to S bps\n\
    -upload     Upload a text file as DKV to the cluster. Each line of is split to a string[], the key is the #uploadKey field\n\
    -uploadKey  #uploadKey field\n\
    -uploadSplit Characters used to split the upload\n\
    -maxwait    Maximum time in millisecond to wait for a web request for activity, before consider it failed \n\
    -download LIMIT  Download files to the cluster. Using key as URL. LIMIT indicates # of parallel download allowed \n\
    -exe        Execute function in a separate exe\n\
    -task       Execute function in parallel tasks\n\
    -downloadChunk Chunk of Bytes to download\n\
    -countTag   Count how many images has a certain tag\n\
    "

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
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let localdir = parse.ParseString( "-local", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let bIn = parse.ParseBoolean( "-in", false )
    let bOut = parse.ParseBoolean( "-out", false )
    let nrep = parse.ParseInt( "-rep", 3 )
    let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
    let slimit = parse.ParseInt( "-slimit", 10 )
    let password = parse.ParseString( "-password", "" )
    let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
    let nump = parse.ParseInt( "-nump", 0 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
    let uploadKey = parse.ParseInt( "-uploadKey", 0 )
    let uploadFile = parse.ParseString( "-upload", "" )
    let uploadSplit = parse.ParseString( "-uploadSplit", "\t" )
    let downloadLimit = parse.ParseInt( "-download", -1 )
    let nMaxWait = parse.ParseInt( "-maxwait", 30000 )
    let bRetrieve = parse.ParseBoolean( "-retrieve", false )
    let bTask = parse.ParseBoolean( "-task", false )
    let downloadChunk = parse.ParseInt( "-downloadChunk", (1<<<20) )
    let countTag = parse.ParseInt( "-countTag", 0 )
    let countWords = parse.ParseInt( "-countWord", 0 )

    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        Cluster.Start( null, PrajnaClusterFile )
        let cluster = Cluster.GetCurrent()
        if true then
            if bExe then 
                JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask

            let mutable t1 = (DateTime.UtcNow)
            if countWords > 0 && remoteDKVname.Length>0 then
                let metaDKV = DSet<string*string[]> ( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                let metaDSet = metaDKV |> DSet.map ( fun (url, columns) -> columns ) 
                let word = metaDSet |> DSet.collect ( Seq.collect ( fun col -> col.Split(' ') ) )
                let wordDKV = word |> DSet.map( fun word -> word, 1 ) 
                let wordCount = wordDKV |> DKV.reduceByKey ( + )
                wordCount.Printfn ("%A")
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "Done reduce by word in %f sec" (elapse.TotalSeconds) ))
                bExecute <- true
            elif countTag > 0 && remoteDKVname.Length>0 then 
                let imageName = remoteDKVname + ".download.async"
                let metaName = remoteDKVname + ".meta.async"
                let imageDKV = DSet<string*byte[]> ( Name = imageName, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                let metaDKV = DSet<string*string[]> ( Name = metaName, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                let colDKV = metaDKV |> DSet.map( fun tuple -> let url, cols = tuple
                                                               url, cols.[countTag] )
                let mixedFunc (url0:string, tags:string) ( url1:string, bytes:byte[] ) =
                    tags, bytes.Length
                let mixedDKV = DSet.map2 mixedFunc colDKV imageDKV
                let foldFunc (dic:Dictionary<_,_>) ( tags:string, imgLen:int) = 
                    let useDic = if Utils.IsNull dic then Dictionary<_,_>() else dic
                    let tag = tags.Split( ";,\t".ToCharArray(), StringSplitOptions.RemoveEmptyEntries ) 
                    let taglower = tag |> Array.map( fun str -> str.ToLower() )
                    let tagall = Array.append taglower [| "zzzzzzzz" |]
                    for t in tagall do 
                        if not (useDic.ContainsKey( t )) then 
                            useDic.Item( t ) <- (1, int64 imgLen)
                        else
                            let c, totalLen = useDic.Item( t )
                            useDic.Item( t ) <- (c+1, totalLen + int64 imgLen)
                    useDic
                let aggrFunc (dic1:Dictionary<string,(int*int64)>) (dic2:Dictionary<string,(int*int64)>) =
                    for kvpair in dic1 do 
                        let tag = kvpair.Key
                        if dic2.ContainsKey( tag ) then 
                            let c1, totalLen1 = kvpair.Value
                            let c2, totalLen2 = dic2.Item( tag )
                            dic2.Item( tag ) <- (c2+c1, totalLen1 + totalLen2)
                        else
                            dic2.Item( tag ) <- kvpair.Value
                    dic2
                let retDic = mixedDKV.Fold(foldFunc, aggrFunc, null)
                let retArray = retDic :> seq<_> |> Seq.toArray 
                let sortedArray = retArray |> Array.sortBy ( fun kvpair -> let count, totalLen = kvpair.Value 
                                                                           (int64 count <<< 20) + totalLen / (int64 count ) )
                for kvpair in sortedArray do 
                    let tag = kvpair.Key
                    let count, totalLen = kvpair.Value
                    if tag <> "zzzzzzzz" then 
                        Logger.Log( LogLevel.Info, ( sprintf "Tag %s ........ %d images (avg length=%dB)" tag count (totalLen / int64 count) ))
                let countAll, totalAll = retDic.Item( "zzzzzzzz" )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "The dataset has %d Tags with %d images of %dMB total (avg length = %dB) processed in %f sec, throughput = %f MB/s" 
                                                       (sortedArray.Length-1) countAll (totalAll/1000000L) (totalAll/int64 countAll) (elapse.TotalSeconds) ((float totalAll)/elapse.TotalSeconds/1000000.) ))
                bExecute <- true
            elif uploadFile.Length>0 && remoteDKVname.Length>0 then 
                let store = DSet<string*string[]>( Name = remoteDKVname, NumReplications = nrep, 
                                Version = ver, SerializationLimit = slimit, 
                                Password = password, 
                                TypeOfLoadBalancer = typeOf, 
                                PeerRcvdSpeedLimit = rcvdSpeedLimit )
                if nump<>0 then 
                    store.NumPartitions <- nump
                let t1 = (DateTime.UtcNow)
                let total = ref 0L
                let lines = ref 0 
                use reader = new StreamReader( uploadFile )
                let toSave = 
                    seq { 
                        while not reader.EndOfStream do 
                            let line = reader.ReadLine()
                            let strArray = line.Split( uploadSplit.ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
                            if strArray.Length> uploadKey then 
                                yield ( strArray.[uploadKey], strArray )
                                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Upload %s" strArray.[uploadKey] ))
                                lines := !lines + 1
                                total := !total + ( int64 ( strArray.[uploadKey].Length + line.Length ))
                                if ( !lines % 1000 = 0 ) then 
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Upload %s" strArray.[uploadKey] ))
                    }
                toSave |> store.Store
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "Processed %d Lines with total %dB in %f sec, throughput = %f MB/s" !lines !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))
                bExecute <- true
// JinL: Task need to be implemented with Task.ContinueWith in F#
//            elif downloadLimit>0 && remoteDKVname.Length>0 && bTask then 
//                let downloadHttpTask( parti, serial, (url:string, value ) )= 
//                        try 
//                            let t1 = (DateTime.UtcNow)
//                            let req = WebRequest.Create(url)
//                            let respAsync = req.GetResponseAsync()
//                            // To automatically dispose respAsync when it is out of scope
//                            let bCompleted = respAsync.Wait( nMaxWait )
//                            if not bCompleted then 
//                                url, (value, null, (sprintf "URL %s Timeout at GetResponseAsync after %f sec" url ((DateTime.UtcNow).Subtract(t1).TotalSeconds) ) )
//                            else
//                                use resp =  respAsync.Result
//                                let contentLength = int resp.ContentLength
//                                let stream = resp.GetResponseStream()
//                                let rcvdBuffer = new ExpandableBuffer( (if contentLength < 0 then 1<<<20 else contentLength), 0 )
//                                let pos = ref 0
//                                let lastRead = ref 1024 
//                                let maxReadLen = downloadChunk
//                                while (!lastRead)>0 do
//                                    let readLen = if contentLength < 0 then maxReadLen else Math.Min( contentLength - rcvdBuffer.Tail, maxReadLen ) 
//                                    // It is guarantteed that we can read readLen bytes afterwards.
//                                    rcvdBuffer.TryExtendTail( readLen ) 
//                                    let buf, pos, _ = rcvdBuffer.GetBufferAfterTail()
//                                    let t2 = (DateTime.UtcNow)
//                                    let readtask = stream.ReadAsync( buf, pos, readLen ) 
//                                    let bCompleted = readtask.Wait( nMaxWait )
//                                    if bCompleted then 
//                                        let readin = readtask.Result
//                                        if readin > 0 then 
//                                            rcvdBuffer.ExtendTail( readin )
//                                        lastRead := readin
//                                    else
//                                        raise (System.TimeoutException( sprintf "URL %s Timeout at ReadAsync after %f sec with %dB download" url ((DateTime.UtcNow).Subtract(t2).TotalSeconds) (rcvdBuffer.Length) ) )
//                                let bytearr = rcvdBuffer.GetTruncatedBuffer()
//                                let msg = sprintf "%d, %d, download %s with %dB" parti serial url bytearr.Length
//                                url, ( value, bytearr, msg)
//                        with
//                        | e ->
//                            let errorMsg = sprintf "%d, %d, download %s error, with exception %A" parti serial url e
//                            Logger.Log(LogLevel.WildVerbose, errorMsg)
//                            url, ( value, null, errorMsg )
//                ()
//                Logger.LogF(LogLevel.Info, ( fun _ -> sprintf "Execute Parallel Download via Task<_> using async function call" ))
//                let srcDKV = DSet<string*string[]>( Name = remoteDKVname, 
//                                                        PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
//                // Calculating download limit
//                let httpDKV = srcDKV |> DSet.reorgWDegree downloadLimit |> DSet.parallelMapi downloadHttpTask
//                // Split the DKV into 2, one for retrieve status information, one to save to destination. 
//                let infoDKV, saveDKV = httpDKV |> DSet.bypass2             
//                let saveDKV1 = saveDKV |> DKV.choose ( fun x -> let url, tuple = x 
//                                                                let value, bytearr, msg = tuple
//                                                                if Utils.IsNotNull bytearr then 
//                                                                   Some ( url, (bytearr, value) )
//                                                                else
//                                                                   None )
//                let imgDKV, metaDKV = saveDKV1 |> DSet.split2 ( fun (url, (bytearray, value )) -> url, bytearray )
//                                                             ( fun (url, (bytearray, value )) -> url, value )
//                let imgStream = imgDKV |> DKV.saveToName (remoteDKVname + ".download") 
//                let metaStream = metaDKV |> DKV.saveToName ( remoteDKVname + ".meta" ) 
//                let msgDKV = infoDKV |> DSet.map ( fun (url, tuple) -> let _, bytearr, msg = tuple
//                                                                       msg, if Utils.IsNull bytearr then 0 else bytearr.Length )
//                let msgSeq = msgDKV.ToSeq()
//                let nDownloads = ref 0
//                let nTotal = ref 0UL
//                let t1 = (DateTime.UtcNow)
//                msgSeq 
//                |> Seq.iter ( 
//                    fun (msg, len ) -> 
//                        nTotal := !nTotal + (uint64 len ) 
//                        if len > 0 then 
//                            nDownloads := !nDownloads + 1
//                        Logger.LogF(LogLevel.Info, ( sprintf "%d,%dMB %s" !nDownloads (!nTotal>>>20) msg ))
//                    )
//                let t2 = (DateTime.UtcNow)
//                let elapseSecond = t2.Subtract(t1).TotalSeconds
//                Logger.Log(LogLevel.Info, ( sprintf "Total download %d files, %dMB in %f sec, throughput=%fMB/s" !nDownloads (!nTotal>>>20) elapseSecond (float (!nTotal>>>20)/elapseSecond) ))
//                bExecute <- true
            elif downloadLimit>0 && remoteDKVname.Length>0 then 
                let downloadHttp parti serial (url:string, value) = 
                    async {
                        try 
                            let req = WebRequest.Create(url)
                            let! respAsync = req.AsyncGetResponse()
                            // To automatically dispose respAsync when it is out of scope
                            use resp = respAsync
                            let contentLength = int resp.ContentLength
                            let stream = resp.GetResponseStream()
                            let rcvdBuffer = new ExpandableBuffer( (if contentLength < 0 then 1<<<20 else contentLength), 0 )
                            let pos = ref 0
                            let lastRead = ref 1024 
                            let maxReadLen = downloadChunk
                            while (!lastRead)>0 do
                                let readLen = if contentLength < 0 then maxReadLen else Math.Min( contentLength - rcvdBuffer.Tail, maxReadLen ) 
                                // It is guarantteed that we can read readLen bytes afterwards.
                                rcvdBuffer.TryExtendTail( readLen ) 
                                let buf, pos, _ = rcvdBuffer.GetBufferAfterTail()
                                let! readin = stream.AsyncRead( buf, pos, readLen ) 
                                if readin > 0 then 
                                    rcvdBuffer.ExtendTail( readin )
                                lastRead := readin
                            let bytearr = rcvdBuffer.GetTruncatedBuffer()
                            let msg = sprintf "%d, %d, download %s with %dB" parti serial url bytearr.Length
                            return url, ( value, bytearr, msg)
                        with
                        | e ->
                            let errorMsg = sprintf "%d, %d, download %s error, with exception %A" parti serial url e
                            Logger.Log( LogLevel.WildVerbose, errorMsg )
                            return url, ( value, null, errorMsg )
                    }
                let srcDKV = DSet<string*string[]>( Name = remoteDKVname, 
                                                        PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                // Calculating download limit
                let httpDKV = srcDKV |> DSet.reorgWDegree downloadLimit |> DSet.asyncMapi downloadHttp
                // Split the DKV into 2, one for retrieve status information, one to save to destination. 
                let infoDKV, saveDKV = httpDKV |> DSet.bypass2             
                let saveDKV1 = saveDKV |> DSet.choose ( fun x -> let url, tuple = x 
                                                                 let value, bytearr, msg = tuple
                                                                 if Utils.IsNotNull bytearr then 
                                                                    Some ( url, (bytearr, value) )
                                                                 else
                                                                    None )
                let imgDKV, metaDKV = saveDKV1 |> DSet.split2 ( fun (url, (bytearray, value )) -> url, bytearray )
                                                             ( fun (url, (bytearray, value )) -> url, value )
                let imgStream = imgDKV |> DSet.lazySaveToHDDByName (remoteDKVname + ".download.async") 
                let metaStream = metaDKV |> DSet.lazySaveToHDDByName ( remoteDKVname + ".meta.async" ) 
                let msgDKV = infoDKV |> DSet.map ( fun (url, tuple) -> let _, bytearr, msg = tuple
                                                                       msg, if Utils.IsNull bytearr then 0 else bytearr.Length )
                let msgSeq = msgDKV.ToSeq()
                let nDownloads = ref 0
                let nTotal = ref 0UL
                let t1 = (DateTime.UtcNow)
                msgSeq 
                |> Seq.iter ( 
                    fun (msg, len ) -> 
                        nTotal := !nTotal + (uint64 len ) 
                        if len > 0 then 
                            nDownloads := !nDownloads + 1
                        Logger.Log( LogLevel.Info, ( sprintf "%d,%dMB %s" !nDownloads (!nTotal>>>20) msg ))
                    )
                let t2 = (DateTime.UtcNow)
                let elapseSecond = t2.Subtract(t1).TotalSeconds
                Logger.Log( LogLevel.Info, ( sprintf "Total download %d files, %dMB in %f sec, throughput=%fMB/s" !nDownloads (!nTotal>>>20) elapseSecond (float (!nTotal>>>20)/elapseSecond) ))
                bExecute <- true

        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
