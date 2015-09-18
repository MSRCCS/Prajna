(*---------------------------------------------------------------------------
    Copyright 2013, Microsoft.	All rights reserved                                                      

    File: 
        DKVCopy.fs
  
    Description: 
        Copy a set of files into (and out of DKV) 

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
// open Prajna.Tools.Process
open Prajna.Core
open Prajna.Api.FSharp

let Usage = "
    Usage: Copy a folder in and out of DKV\n\
    Command line arguments:\n\
    -in         Copy into Prajna \n\
    -out        Copy outof Prajna \n\
    -cluster    Name of cluster used \n\
    -local      Local directory. All files in the directories will be copy to (or from) remote \n\
    -remote     Name of the distributed Prajna folder\n\
    -ver V      Select a particular DKV with Version string: in format yyMMdd_HHmmss.fff \n\
    -rep REP    Number of Replication \n\
    -slimit S   # of record to serialize \n\
    -balancer B Type of load balancer \n\
    -nump N     Number of partitions \n\
    -hash       Return hash of the file stored \n\
    -aggregate  Calculate for aggregation\n\
    -speed S    Limiting speed of each peer to S bps\n\
    -spattern   Search pattern for the local directory, for example, *.jpg \n\
    -ram        Make DKV persist to speed up followup execution (only used with -hash flag for testing). \n\
    -rec        if present, search through all directories\n\
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
    let uploadFile = parse.ParseString( "-upload", "" )
    let bCacheRAM = parse.ParseBoolean( "-ram", false )
    let nrep = parse.ParseInt( "-rep", 3 )
    let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
    let slimit = parse.ParseInt( "-slimit", 10 )
    let password = parse.ParseString( "-password", "" )
    let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
    let bHash = parse.ParseBoolean( "-hash", false )
    let bAggregate = parse.ParseBoolean( "-aggregate", false )
    let nump = parse.ParseInt( "-nump", 0 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
    let mutable bExecute = false
    let keyfile = parse.ParseString( "-keyfile", "" )
    let keyfilepwd = parse.ParseString( "-keypwd", "" )
    let pwd = parse.ParseString( "-pwd", "" )

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
        Cluster.StartCluster( PrajnaClusterFile )
        let cluster = Cluster.GetCurrent
        if not (keyfilepwd.Equals("")) then
            Cluster.Connects.InitializeAuthentication(pwd, keyfile, keyfilepwd)
        if true then
            if bExe then 
                JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask
            let mutable t1 = (DateTime.UtcNow)
            if ( bIn && localdir.Length>0 && remoteDKVname.Length>0 ) then 
                    let store = DSet<string*byte[]>( Name = remoteDKVname, NumReplications = nrep, 
                                    Version = ver, SerializationLimit = slimit, 
                                    Password = password, 
                                    TypeOfLoadBalancer = typeOf, 
                                    PeerRcvdSpeedLimit = rcvdSpeedLimit )
                    if nump<>0 then 
                        store.NumPartitions <- nump
                    let t1 = (DateTime.UtcNow)
                    let fseq, total, files = DSet.FolderRecursiveSeq( localdir, searchPattern, searchOption ) 
                    if not bHash then 
                        store.Store( fseq )
                    else
                        fseq |> 
                        Seq.map( fun (filename, bytearray) -> 
                            Logger.Log( LogLevel.Info, ( sprintf "File %s ... %dB (Hash %08x) " filename (filename.Length+bytearray.Length) ( sha512hash bytearray) )   )
                            filename, bytearray
                            )
                        |> store.Store
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" files.Length !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))
                    bExecute <- true
            elif ( bOut && localdir.Length>0 && remoteDKVname.Length>0 ) then 
                let t1 = (DateTime.UtcNow)
                let mutable curDKV = DSet<string*byte[]>( Name = remoteDKVname,
                                                  Version = ver, 
                                                  Password = password, 
                                                  PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                // add search pattern first. 
                if searchPattern <> defaultSearchPattern then 
                    curDKV <- curDKV |> DKV.filterByKey( fun fname -> System.Text.RegularExpressions.Regex.Match( fname , searchPattern ).Success )
                let numFiles, total = 
                    if bAggregate then 
                        let count, total_size = curDKV
                                                |> DSet.map (fun (fname, bytearray ) -> (1, uint64 bytearray.Length))
                                                |> DSet.reduce ( fun (v1,l1) (v2,l2) -> (v1+v2), (l1+l2))
                        count, total_size
                    elif not bHash then 
                        let o = curDKV.ToSeq()
                        DSet.RetrieveFolderRecursive( localdir, o )
                    else
                        let mutable hDKV = curDKV.MapByValue ( fun bytearray -> ( sha512hash bytearray), (bytearray.Length) ) 
                        if bCacheRAM then 
                            hDKV <- hDKV.CacheInMemory()
                        let hseq = hDKV.ToSeq()
                        let nFiles = ref 0
                        let nTotal = ref 0UL
                        hseq 
                        |> Seq.iter ( 
                            fun (filename, payload ) -> 
                                let hash, flen = payload
                                nTotal := !nTotal + (uint64 filename.Length) + (uint64 flen ) 
                                nFiles := !nFiles + 1
                                Logger.Log( LogLevel.Info, ( sprintf "File %s ... %dB (Hash %08x) " filename (filename.Length+flen) hash ))
                           )
                        ( !nFiles, !nTotal )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" numFiles total elapse.TotalSeconds ((float total)/elapse.TotalSeconds/1000000.) ))
                bExecute <- true

        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
