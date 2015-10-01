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
        PrajnaRobocopy.fs
  
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
open System.Collections.Concurrent
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.Serialization
open System.Threading.Tasks

open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.FileTools
open Prajna.Tools.BytesTools
open Prajna.Core
open Prajna.Api.FSharp


let Usage = "
    Usage: Copy a folder in and out of a remote node/cluster in Prajna (retry logic hasn't been implemented yet). \n\
    Command line arguments:\n\
    -in         Copy in to Prajna node/cluster\n\
    -out        Copy out of Prajna node/cluster \n\
    -cluster    Name of cluster used \n\
    -node       Node of the cluster \n\
    -local      Local directory to be copied to (or from) \n\
    -remote     Remote directory to be copied to (or from) \n\
    -hashdir    Remote hash directory. All file under this directory will be named via Hash of the file. We will not copy the file if the hash is the same. \n\
    -pattern    file pattern \n\
    -s          whether copy sub directory (default yes) \n\
    -XF         Exclude certain file to be copied \n\
    -XD         Exclude certain directory to be copied \n\
    "
/// Hold Interop Win32 system call 
module InteropWithKernel32 =
    open System.Runtime.InteropServices
    open Microsoft.FSharp.NativeInterop

    /// Establishes a hard link between an existing file and a new file. This function is only supported on the NTFS file system, and only for files, not directories.
    /// The two files will be considered the same once linked. 
    /// see https://msdn.microsoft.com/en-us/library/windows/desktop/aa363860(v=vs.85).aspx
    [<DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError=true)>]
    extern bool CreateHardLink(
        string lpFileName,
        string lpExistingFileName,
        IntPtr lpSecurityAttributes
    )

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

/// Building a local file list for copy, with information on excluded files & directories 
let rec buildLocalFileListOnce(localDir:string, searchPattern, searchOption, excludeFiles:string[], excludeDirs:string[], fileLst:ConcurrentDictionary<_, byte[]>) = 
    try 
        let curDirInfo = DirectoryInfo( localDir )
        for file in curDirInfo.GetFiles( searchPattern, SearchOption.TopDirectoryOnly ) do
            try
                let mutable bFindInExclude = false
                for ex in excludeFiles do 
                    if file.Name.IndexOf( ex, StringComparison.OrdinalIgnoreCase )>=0 then 
                        bFindInExclude <- true
                if not bFindInExclude then 
                    fileLst.Item( file ) <- null
            with
            | e ->
                // Skip files that fails to traverse 
                ()
        if searchOption = SearchOption.AllDirectories then 
            for dir in curDirInfo.GetDirectories( searchPattern, SearchOption.TopDirectoryOnly ) do
                try
                    let mutable bFindInExclude = false
                    for ex in excludeDirs do 
                        if dir.Name.IndexOf( ex, StringComparison.OrdinalIgnoreCase )>=0 then 
                            bFindInExclude <- true
                    if not bFindInExclude then 
                        buildLocalFileListOnce( dir.FullName, searchPattern, searchOption, excludeFiles, excludeDirs, fileLst )
                with 
                | e ->
                    // skip directory that fails to traverse
                    ()
    with 
    | e -> 
        // skip entire directory if traverse fails
        ()
/// Building a local file list for copy, with information on excluded files & directories 
let buildLocalFileList(localDir, searchPattern, searchOption, excludeFiles, excludeDirs) = 
    let fileLst = ConcurrentDictionary<_, byte[]>( )
    buildLocalFileListOnce( localDir, searchPattern, searchOption, excludeFiles, excludeDirs, fileLst ) 
    fileLst

/// <summary>
/// Convert file list to sequence
/// </summary>
let LstToSeq (localDir) (fileLst:ConcurrentDictionary<FileInfo, byte[]>) = 
    let pathRoot = Path.GetFullPath( localDir) 
    fileLst |> Seq.map( fun pair -> let fullname = pair.Key.FullName
                                    let idx = fullname.IndexOf( pathRoot, StringComparison.OrdinalIgnoreCase )
                                    let filename = if idx < 0 then fullname else fullname.Substring( idx + pathRoot.Length + 1 )
                                    filename, pair.Key.LastWriteTimeUtc.Ticks )

/// <summary>
/// Convert file list to hash dictionary
/// </summary>
let LstToHashDictionary (localDir) (fileLst:ConcurrentDictionary<FileInfo, byte[]>) = 
    let pathRoot = Path.GetFullPath( localDir) 
    let hashSeq = fileLst |> Seq.map( fun pair -> let fullname = pair.Key.FullName
                                                  let idx = fullname.IndexOf( pathRoot, StringComparison.OrdinalIgnoreCase )
                                                  let filename = if idx < 0 then fullname else fullname.Substring( idx + pathRoot.Length + 1 )
                                                  let hashFilename = BytesToHex(pair.Value) + ".hlnk"
                                                  hashFilename, filename )
    let dic = ConcurrentDictionary<_,_>(StringComparer.OrdinalIgnoreCase )
    let linkLst = List<_>()
    for en in hashSeq do 
        let hashFilename, filename = en
        let mappedFilename = dic.GetOrAdd( hashFilename, filename )
        if not (Object.ReferenceEquals( mappedFilename, filename )) then 
            linkLst.Add( hashFilename, filename )
    dic, linkLst

    

let buildFileListInSeq(localDir, searchPattern, searchOption, excludeFiles, excludeDirs) () = 
    LstToSeq localDir (buildLocalFileList(localDir, searchPattern, searchOption, excludeFiles, excludeDirs))

let totalFileSize( fileLst:ConcurrentDictionary<FileInfo, byte[]>) = 
    let mutable count = 0L
    for pair in fileLst do 
        count <- count + pair.Key.Length
    count

let calculateFileHash( fileLst:ConcurrentDictionary<FileInfo, byte[]>) = 
    let mutable count = 0L
    for pair in fileLst do 
        try
            let byt = ReadBytesFromFile pair.Key.FullName
            count <- count + int64 byt.Length
            let res32 = 
                use hasher = new System.Security.Cryptography.SHA256Managed()
                HashLengthPlusByteArray( hasher, byt )
            fileLst.Item( pair.Key ) <- res32
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "File %s Hash = %s" pair.Key.FullName (BytesToHex(res32)) ))
        with 
        | e -> 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to read and hash file %s, file is removed." pair.Key.FullName ))
            fileLst.TryRemove( pair.Key ) |> ignore
    count

let remoteBuildFileList( remoteDir, searchPattern, searchOption, excludeFiles, excludeDirs, cl:Cluster ) = 
    let dkvStart = DSet<_>( Name = "RemoteDir", Cluster = cl )
    let dkvFiles = dkvStart.Source (buildFileListInSeq(remoteDir, searchPattern, searchOption, excludeFiles, excludeDirs))
    dkvFiles.ToSeq() |> Seq.toArray

let showFileList( fileLst: seq<string*int64>) =
    for entry in fileLst do 
        let filename, ticks = entry
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "%s, %s" filename (VersionToString(DateTime(ticks))) ))

/// This function will execute in remote machine. 
let remoteCopyOps (hashPath:string) (remotePath:string) (copyContent:(string*int64*byte[])[])  () = 
    if not (Utils.IsNull copyContent) then 
        let examinedPath = ConcurrentDictionary<_,DirectoryInfo>(StringComparer.OrdinalIgnoreCase )
        if not (StringTools.IsNullOrEmpty( hashPath )) then 
            examinedPath.GetOrAdd( hashPath, DirectoryInfoCreateIfNotExists ) |> ignore
        for oneContent in copyContent do 
            let fname, ticks, byt = oneContent
            try 
                let filename = if StringTools.IsNullOrEmpty( remotePath) then fname else Path.Combine( remotePath, fname )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Copy file %s with %dB, timestamp %A" filename byt.Length (DateTime(ticks)) ))
                let path = Path.GetDirectoryName( filename )
                // Create Directory if not exist
                examinedPath.GetOrAdd( path, DirectoryInfoCreateIfNotExists ) |> ignore
                FileTools.WriteBytesToFileConcurrent filename byt
                File.SetLastWriteTimeUtc( filename, DateTime( ticks ) ) 
                if not (StringTools.IsNullOrEmpty( hashPath )) then 
                    let res32 = 
                        use hasher = new System.Security.Cryptography.SHA256Managed()
                        HashLengthPlusByteArray( hasher, byt ) 
                    let res32String = BytesToHex(res32) + ".hlnk"
                    let linkedFilename = Path.Combine( hashPath, res32String ) 
                    InteropWithKernel32.CreateHardLink( linkedFilename, filename, IntPtr.Zero ) |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Link file %s ---> %s " filename linkedFilename ))
            with 
            | e -> 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Error in Copy file %s with exception %A" fname e ))
    ()



let copyToRemoteOps (hashPath) (remotePath) (cur:Cluster) (copyContent)   = 
    let copyDSet = DSet<_>( Name = "CopyToRemote", Cluster = cur )
    copyDSet.Execute (remoteCopyOps hashPath remotePath copyContent  )

let copyToRemote (cur:Cluster) (localPath:string) (fileLst:(string*int64)[]) (remotePath) (hashPath) = 
    let partialLst = List<_>()
    let mutable totalFileLength = 0L 
    for i = 0 to fileLst.Length - 1 do
        try 
            let pName, Ticks = fileLst.[i]
            let filename = Path.Combine( localPath, pName )
            let byt = ReadBytesFromFile filename 
            partialLst.Add( pName, Ticks, byt ) 
            totalFileLength <- totalFileLength + byt.LongLength
        with 
        | e -> 
            // If fails to read, simply skip the file. 
            ()
        let bFlush = totalFileLength > (1L<<<27) || i = (fileLst.Length - 1)
        if bFlush then 
            let content = partialLst.ToArray()
            for c in content do 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let fname, ticks, byt = c
                                                              sprintf "Copy to remote %s (%d, %s)" fname (byt.Length) (VersionToString(DateTime(ticks))) ))
            copyToRemoteOps hashPath remotePath cur content 
            partialLst.Clear()
            totalFileLength <- 0L
    ()

let readOneFileRemote fname (remotePath:string) = 
    try
        let filename = if StringTools.IsNullOrEmpty( remotePath) then fname else Path.Combine( remotePath, fname )
        let byt = ReadBytesFromFile filename 
        let ticks = (File.GetLastAccessTimeUtc filename).Ticks
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read file %s with %dB, timestamp %A" filename byt.Length (DateTime(ticks)) ))
        ( byt, ticks )
    with 
    | e -> 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Fail to read file %s" fname ))
        ( null, 0L )

let copyFromRemoteOps (fileLst:(string*int64)[]) (remotePath:string) () = 
    seq {
        if not (Utils.IsNull fileLst) then 
            for oneEntry in fileLst do 
                let fname, _ = oneEntry
                let byt, ticks = readOneFileRemote fname remotePath
                if Utils.IsNotNull byt && byt.Length > 0 then 
                    yield ( fname, byt, ticks )
    }

let examinedPath = ConcurrentDictionary<_,DirectoryInfo>(StringComparer.OrdinalIgnoreCase )

let localCopyOps (localPath:string) (entry:string*byte[]*int64) = 
    let fname, byt, ticks = entry
    try
        let filename = if StringTools.IsNullOrEmpty( localPath) then fname else Path.Combine( localPath, fname )
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Copy file %s with %dB, timestamp %A" filename byt.Length (DateTime(ticks)) ))
        let path = Path.GetDirectoryName( filename )
        // Create Directory if not exist
        examinedPath.GetOrAdd( path, DirectoryInfoCreateIfNotExists ) |> ignore
        FileTools.WriteBytesToFileConcurrent filename byt
        File.SetLastWriteTimeUtc( filename, DateTime( ticks ) ) 
    with 
    | e -> 
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Error in Copy file %s with exception %A" fname e ))
        
let copyFromRemote (cur:Cluster) (fileLst:(string*int64)[]) (remotePath:string) (localPath:string)= 
    let copyFromDSet = DSet<_>( Name = "CopyFromRemote", Cluster = cur )
    let copyFromDSet2 = copyFromDSet.Source (copyFromRemoteOps fileLst remotePath)
    copyFromDSet2.ToSeq() |> Seq.iter ( localCopyOps localPath )

let linkAtRemoteOps (fileLst:(string*string)[]) (remotePath:string) (hashDir:string) ()=
    let examinedPath = ConcurrentDictionary<_,DirectoryInfo>(StringComparer.OrdinalIgnoreCase )
    for entry in fileLst do 
        let linkedName, fname = entry
        let filename = if StringTools.IsNullOrEmpty( remotePath) then fname else Path.Combine( remotePath, fname )
        let linkedFilename = if StringTools.IsNullOrEmpty( hashDir) then fname else Path.Combine( hashDir, linkedName )
        try
            let path = Path.GetDirectoryName( filename )
            // Create Directory if not exist
            examinedPath.GetOrAdd( path, DirectoryInfoCreateIfNotExists ) |> ignore
            InteropWithKernel32.CreateHardLink( filename, linkedFilename, IntPtr.Zero ) |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Link file %s --> %s" linkedFilename filename )   )
            Logger.Do( DeploymentSettings.ExecutionLevelTouchAssembly, ( fun _ -> TouchFile linkedFilename ))
        with 
        | e -> 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to link file %s to %s" linkedFilename filename )   )

let linkAtRemote (cur:Cluster) fileLst remotePath hashDir = 
    let linkAtDSet = DSet<_>( Name = "LinkAtRemote", Cluster = cur )
    linkAtDSet.Execute (linkAtRemoteOps fileLst remotePath hashDir)

// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let nodeName = parse.ParseString( "-node", "" )
    let localPath = parse.ParseString( "-local", "" )
    let remotePath = parse.ParseString( "-remote", "" )
    let bIn = parse.ParseBoolean( "-in", false )
    let bOut = parse.ParseBoolean( "-out", false )
    let hashdir = parse.ParseString( "-hashdir", (Path.Combine( DeploymentSettings.LocalFolder, DeploymentSettings.HashFolder )))
    let searchPattern = parse.ParseString( "-pattern", "*" )
    let excludeFilesList = List<_>()
    let mutable bFind = true
    while bFind do 
        let oneFile = parse.ParseString( "-XF", "" )
        if StringTools.IsNullOrEmpty( oneFile ) then 
            bFind <- false
        else
            excludeFilesList.Add( oneFile )
    let excludeFiles = excludeFilesList.ToArray()

    let excludeDirsList = List<_>()
    bFind <- true
    while bFind do 
        let oneDir = parse.ParseString( "-XD", "" )
        if StringTools.IsNullOrEmpty( oneDir ) then 
            bFind <- false
        else
            excludeDirsList.Add( oneDir )
    let excludeDirs = excludeDirsList.ToArray()
    let searchOption = if parse.ParseBoolean ("-s", true) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
    let bExe = parse.ParseBoolean( "-exe", false )
    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        Cluster.Start( null, PrajnaClusterFile )
        if not ( StringTools.IsNullOrEmpty( nodeName ) ) then 
            Cluster.GetCurrent().UseSingleNodeCluser( nodeName )
        if bExe then 
            JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask

        if bIn && not (StringTools.IsNullOrEmpty( localPath )) && not (StringTools.IsNullOrEmpty( remotePath )) then
            let mutable t1 = (DateTime.UtcNow)
            let fileLst = buildLocalFileList( localPath, searchPattern, searchOption, excludeFiles, excludeDirs )
            let total = ref 0L
            if not (StringTools.IsNullOrEmpty( hashdir ) ) then 
                total := calculateFileHash( fileLst ) 
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                t1 <- t2
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "hash %d Files with total %dB in %f sec, throughput = %f MB/s" 
                                                                       fileLst.Count !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))

            let localFileLst = ConcurrentDictionary<_,_>( (LstToSeq localPath fileLst |> Seq.map( fun (k,v) -> KeyValuePair<_,_>(k,v) )) , StringComparer.OrdinalIgnoreCase )
            let cluster = Cluster.GetCurrent()
            for node in cluster.Nodes do
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Copy to node %s" node.MachineName ))
                let cur = cluster.GetSingleNodeCluster( node.MachineName )
                let copyLst = ConcurrentDictionary<_,_>( localFileLst, StringComparer.OrdinalIgnoreCase )
                let mutable linkArr = null
                if not (StringTools.IsNullOrEmpty( hashdir ) ) then 
                    // Find those files that just need to be linked. 
                    let hashDic, linkLst = LstToHashDictionary localPath fileLst

                    let remoteHashLst = remoteBuildFileList( hashdir, "*.hlnk", SearchOption.TopDirectoryOnly, Array.empty, Array.empty, cur )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%d hard links at remote machine %s" remoteHashLst.Length node.MachineName ))
                    for remoteEntry in remoteHashLst do 
                        let remoteHash, _ = remoteEntry
                        let refFilename = ref Unchecked.defaultof<_>
                        let bExist = hashDic.TryGetValue( remoteHash, refFilename ) 
                        if bExist then 
                            let filename = !refFilename
                            linkLst.Add( remoteHash, filename )
                    for linkEntry in linkLst do 
                        let _, filename = linkEntry
                        copyLst.TryRemove( filename ) |> ignore
                    linkArr <- linkLst.ToArray()
                if true then 
                    let remoteLst = remoteBuildFileList( remotePath, searchPattern, searchOption, excludeFiles, excludeDirs, cur )
                    // showFileList( remoteLst )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "retrieve remote lists of %d files" remoteLst.Length ))
                    for remoteEntry in remoteLst do
                        let remoteFileName, remoteTicks = remoteEntry
                        let bExist, localTicks = copyLst.TryGetValue( remoteFileName ) 
                        if bExist && localTicks <= remoteTicks then 
                            copyLst.TryRemove( remoteFileName ) |> ignore
                let finalLst = copyLst |> Seq.map ( fun pair -> pair.Key, pair.Value ) |> Seq.toArray
                if finalLst.Length > 0 then 
                    copyToRemote cur localPath finalLst remotePath hashdir
                // First copy, then link, as link may refer back to its own file 
                if Utils.IsNotNull linkArr && linkArr.Length > 0 then 
                    linkAtRemote cur linkArr remotePath hashdir   
            let t2 = (DateTime.UtcNow)
            let elapse = t2.Subtract(t1)
            Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" fileLst.Count !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))
            bExecute <- true
        elif bOut then 
            let fileLst = buildLocalFileList( localPath, searchPattern, searchOption, excludeFiles, excludeDirs )
            let localFileLst = ConcurrentDictionary<_,_>( (LstToSeq localPath fileLst |> Seq.map( fun (k,v) -> KeyValuePair<_,_>(k,v) )) , StringComparer.OrdinalIgnoreCase )
            let cluster = Cluster.GetCurrent()
            for node in cluster.Nodes do
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Copy from node %s" node.MachineName ))
                let cur = cluster.GetSingleNodeCluster( node.MachineName )
                let fileLst = remoteBuildFileList( remotePath, searchPattern, searchOption, excludeFiles, excludeDirs, cur )
                // Form the list that needs to be copied back. 
                // showFileList( fileLst )
                let copyLst = List<_>() 
                for remoteEntry in fileLst do 
                    let remoteFileName, remoteTicks = remoteEntry
                    let bExist, localTicks = localFileLst.TryGetValue( remoteFileName ) 
                    if bExist && localTicks >= remoteTicks then 
                        ()
                    else
                        // need to copy back
                        localFileLst.Item( remoteFileName ) <- remoteTicks
                        copyLst.Add( remoteEntry )
                copyFromRemote cur (copyLst.ToArray()) remotePath localPath
            bExecute <- true


        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
