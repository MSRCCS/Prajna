(*---------------------------------------------------------------------------
    Copyright 2014, Microsoft.	All rights reserved                                                      

    File: 
        dependency.fs
  
    Description: 
        Files upon which a job depends

    Author:																	
        Sanjeev Mehrotra
        Revised by Jin Li
    Date:
        May 2014	
        Nov. 2014 (revision by Jin Li)
 ---------------------------------------------------------------------------*)
namespace Prajna.Core
open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.FileTools
open Prajna.Tools.FSharp

[<AllowNullLiteral>]
type internal JobDependency() = 
    member val Name = "" with get, set
    member val Location = "" with get, set
    member val Hash : byte[] = null with get, set

    override x.ToString() = 
        if Utils.IsNotNull x.Hash then 
            sprintf "%s, %s, %s" x.Name x.Location (BitConverter.ToString( x.Hash ))
        else
            sprintf "%s, %s" x.Name x.Location 

    member x.IsSame(dep : JobDependency) =
        if (x.Name <> dep.Name) then
            false
        else if (x.Hash.Length <> dep.Hash.Length) then
            false
        else
            let mutable bSame = true
            let mutable i = 0
            while (bSame && (i < x.Hash.Length)) do
                if (x.Hash.[i] <> dep.Hash.[i]) then
                    bSame <- false
                i <- i + 1
            bSame

    /// Compute the signature of the file
    /// we use the last 8B of Hash as signature
    member x.ComputeHash( buf: byte[] ) = 
        if (Utils.IsNull x.Hash) then
            x.Hash <- 
                use hasher = new System.Security.Cryptography.SHA256Managed()
                HashLengthPlusByteArray( hasher, buf )
        x.Hash

    /// Compute hash based on current file 
    member x.ComputeHash( ) = 
        x.ComputeHash(ReadBytesFromFile(x.Location))

    /// Pack file to stream 
    member x.Pack( ms:StreamBase<byte> ) =
        let bytearr = ReadBytesFromFile( x.Location )
        ms.WriteBytesWLen( bytearr )

    /// Construct folder name for file 
    static member ConstructLocation(name : string, hash ) =
//        let extensionIndex = name.LastIndexOf('.')
//        let mutable basename = ""
//        let mutable extname = ""
//        if (extensionIndex = -1) then
//            basename <- name
//        else
//            basename <- name.Substring(0, extensionIndex)
//            extname <- name.Substring(extensionIndex+1, name.Length-extensionIndex-1)
        Path.Combine(DeploymentSettings.LocalFolder, DeploymentSettings.HashFolder, BytesToHex(hash) + ".hlnk" )

    /// Is the file available locally?
    static member GetDependency( name, hash, bCreateIfNonExist ) = 
        let location = JobDependency.ConstructLocation(name, hash)
        if File.Exists location || bCreateIfNonExist then 
            JobDependency( Name = name, 
                                 Location = location, 
                                 Hash = hash )
        else
            null

    /// Try to represent Job Dependency as assembly
    member internal x.ToAssembly(jobLocation) =
        AssemblyCollection.Current.ReflectionOnlyLoadFrom( jobLocation, x.Hash, true )

    /// Unpack file from stream 
    member x.Unpack( ms:StreamBase<byte> ) = 
        let bytearr = ms.ReadBytesWLen()
        try
            WriteBytesToFileConcurrentCreate x.Location bytearr
        with
        | :? System.IO.IOException as e -> 
            Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "(May be OK) Failure to write assembly %s, other daemon is writing the same file, msg %s" x.Location e.Message ))

//    static member CleanJobDirOld( jobDir:string ) =
//        let baseJobDir = Path.GetDirectoryName(jobDir)
//        let jobVersion = Path.GetFileName(jobDir)
//        let jobLocation = Path.GetFullPath(Path.Combine([|DeploymentSettings.LocalFolder; DeploymentSettings.JobFolder ; baseJobDir|]))
//        let jobFullPath = Path.GetFullPath(Path.Combine([|DeploymentSettings.LocalFolder; DeploymentSettings.JobFolder |]))
//        // jobLocation must be udner jobFullPath
//        if (jobLocation.IndexOf(jobFullPath) <> 0) then
//            failwith "JobLocation cannot contain directoryies outside the job folder"
//        // JinL: create a job directory if not exist
//        StringTools.DirectoryInfoCreateIfNotExists jobLocation |> ignore
//        let dirs = Directory.GetDirectories(jobLocation)
//        // Remove related versions. 
//        for dir in dirs do
//            try 
//                let ver = Path.GetFileName(dir)
//                if (not (ver.ToLower().Equals(jobVersion.ToLower()))) then
//                    Directory.Delete(dir, true)
//            with 
//            | e -> 
//                Logger.LogF( LogLevel.Info,  fun _ -> sprintf "(May be OK) Failted to delete directory %s, with exception %A" dir e  )

    static member CleanJobDir( jobDir:string ) =
        let jobFullPath = Path.GetFullPath(Path.Combine([|DeploymentSettings.LocalFolder; DeploymentSettings.JobFolder + DeploymentSettings.ClientPort.ToString()|]))
        // JinL: create a job directory if not exist
        DirectoryInfoCreateIfNotExists jobFullPath |> ignore
        let dirs = Directory.GetDirectories(jobFullPath)
        // Remove related versions. 
        for dir in dirs do
            try 
                Directory.Delete(dir, true)
            with 
            | e -> 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "(May be OK) Failted to delete directory %s, with exception %A" dir e ))


    /// Load file into job directory
    member x.LoadJobDirectory( remoteMappingDir:string ) = 
        if Utils.IsNotNull x.Location && x.Location.Length>0 then
            try
                let bEntryExist = AssemblyCollection.Current.ContainsKey( x.Location, x.Hash )
                if not bEntryExist then 
                    Logger.Do(DeploymentSettings.ExecutionLevelTouchAssembly, ( fun _ -> TouchFile x.Location ))
            with e->
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "(May be OK) Fail to touch file %s" x.Location ))
            let jobLocation = 
                if IsRelativePath remoteMappingDir then 
                    Path.Combine([|DeploymentSettings.LocalFolder; DeploymentSettings.JobFolder + DeploymentSettings.ClientPort.ToString(); remoteMappingDir; x.Name|])
                else
                    Path.Combine( remoteMappingDir, x.Name )
            if File.Exists jobLocation then
                try 
                    System.IO.File.Delete( jobLocation ) |> ignore 
                with 
                | e -> 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "(May be OK) Fail to delete file %s" jobLocation )                    )
            if true then 
                // copy file
                //WriteBytesToFileCreate jobLocation (ReadBytesFromFile x.Location)
                // create hard/soft link instead of copying
                let dirpath = Path.GetDirectoryName(jobLocation)
                if Utils.IsNotNull dirpath then
                    // Create directory if necessary
                    DirectoryInfoCreateIfNotExists (dirpath) |> ignore
                let res, msg = LinkFile jobLocation x.Location
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let fileinfo = System.IO.FileInfo( x.Location ) 
                                                               sprintf "Create hard link %s <-- %s (%A, %dB): %s" 
                                                                           jobLocation x.Location 
                                                                           fileinfo.LastWriteTime fileinfo.Length
                                                                           msg
                                                                           ))
                // symbolic link (aka soft link) requires admin priveleges to create, so does not work well
                //let res = InteropWithKernel32.CreateSymbolicLink(jobLocation, x.Location, 0u)
                if not res then
                    failwith (sprintf "Cannot create hard link %s to %s" x.Location jobLocation)
            // try adding this as assembly resolver if it as an assembly
            let assem = x.ToAssembly(jobLocation)
            if (Utils.IsNotNull assem) then
                AssemblyResolver.AllAssembly.Item(assem.FullName) <- (jobLocation)

/// Enumeration class that controls how a remote container for the job/service is executed. 
type JobTaskKind = 
    /// Default, the remote daemon determines how to launch job/service
    | None = 0                      
    /// Attempt to launch a Read Job within daemon
    | ReadOne = 1                   // a read task,  
    /// Attempt to launch a Light computation job within daemon
    | Computation = 2               // a computation task, 
    /// Remote container may use in RAM data 
    | InRAMDataMask   = 0x04000000  // Has in Ram data
    /// Remote container mask used internally 
    | JobActionMask   = 0x07ffffff  // Portion that describes job type. 
    /// Mask that govern close of remote container 
    | PersistentMask  = 0x08000000  // Mask indicate that the job should not be terminated when the last connection is closed from client. 
    /// Mask that govern how a remote container is launched 
    | ExecActionMask =  0xf0000000  // How to execute
    /// Attempt to launch the remote container as an AppDomain attached to Daemon
    | AppDomainMask =   0x10000000  // Mask indicate that the job should be launched in a different AppDomain 
    /// Attempt to launch the remote container as an Application attached to Daemon
    | ApplicationMask = 0x20000000  // Mask indicate that the job should be launched in another Application (separate exe). 
    /// Attempt to launch the remote container as a Release Application
    | ReleaseMask     = 0x40000000  // Mask indicate that the job should be launched in another Application (separate exe). 
    /// Attempt to launch the remote container as a Release Application
    | ReleaseApplicationMask = 0x60000000  // Mask indicate that the job should be launched in another Application and in release mode. 
    /// Attempt to launch the remote container as a light job
    | LightCPULoad    = 0x00000000  // The job is of light CPU load (so multiple jobs may be started in parallel)
    /// Attempt to launch the remote container as a light read job
    | StartReadOne    = 0x00000001  // A job that read one task 


/// <summary>
/// Remote execution roster, which contains a list of file (dlls, data, environment variables) that is required for the job 
/// </summary>
[<AllowNullLiteral>]
type JobDependencies() =
    // File Dependency Hash
    let mutable depHash = [||]
    let mutable bFinal = false
    /// Current execution roster
    static member val Current = JobDependencies() with get, set
    static member val internal LaunchIDVersion = 0L with get, set
    static member val internal AssemblyHash : byte[] = null with get, set
    /// Assign a name to the remote container, 
    /// All services & data analytical jobs with the same jobName will be put into the same container for execution, and 
    /// calling between services and/or data analytical jobs within the container is a native functional call. 
    static member setCurrentJob jobName =
        let newJob = JobDependencies( JobName = jobName )
        // Default: use the jobName as JobDirectory
        newJob.JobDirectory <- jobName
        JobDependencies.Current <- newJob
        JobDependencies.LaunchIDVersion <- 0L
        newJob
    /// If any new dependency has been added to the file.
    /// if 1: there are dependency changes. 
    /// if 0: there are no dependency change
    /// 1 -> 0 is a lock, which multiple process can compete in launch the remote execution container.  
    member val internal nDependencyChanged= ref 1 with get
    member val internal bJobLauched = false with get, set
    member val internal Hash = null with get, set
    /// Remote container name 
    member val JobName : string = null with get, set
    /// Job Directory of the remote container 
    member val JobDirectory = "" with get, set
    member val internal FileDependencies = new List<JobDependency>() with get
    /// Additional Environmental setting required by the remote container 
    /// It will be used to set ProcessStartInfo.EnvironmentVariables during the launch of the remote container
    member val EnvVars : List<string*string> = new List<string*string>() with get
    
    /// Job File Dependencies Hash
    member internal x.JobDependencySHA256() =
        use ms = new MemStream( ) 
        for dep in x.FileDependencies do 
            ms.WriteBytes( System.Text.Encoding.UTF8.GetBytes(dep.Name) ) |> ignore 
            ms.WriteBytes( dep.Hash ) |> ignore 
        let (buf, pos, cnt) = ms.GetBufferPosLength()
        x.Hash <- ms.ComputeSHA256(int64 pos, int64 cnt)
        x.Hash

    /// Get Hash version string
    member internal x.GetVerStr() =
        let hash = x.JobDependencySHA256()
        BytesToHex( x.Hash ) 

    member internal x.JobDependencyContains(jobDep : JobDependency) =
        let mutable bFound = false
        // check if it exists in JobDependencies
        for a in x.FileDependencies do
            if (not bFound && a.IsSame(jobDep)) then
                bFound <- true
        bFound

    /// Add a mapped file between remote and local. 
    /// The first item of the tuple is the local file, and the second item of the tuple is the remote file name. 
    /// The local file will be sent to remote file during the launch of the remote container. 
    member x.AddLocalRemote(dep : string*string) =
        if (bFinal) then
            failwith "Cannot add any more file dependencies after finalization"
        // remoteLocation is relative to executing directory
        let (local, remote) = dep
        if (not (File.Exists local)) then
            failwith (sprintf "File dependency not found")
        //JobDependencies.FileDependencies.Add(dep)
        let jobDep = JobDependency(Name = remote,
                                         Location = local)
        jobDep.ComputeHash() |> ignore
        if (not (x.JobDependencyContains(jobDep))) then
            x.FileDependencies.Add(jobDep)
            System.Threading.Interlocked.Increment( x.nDependencyChanged ) |> ignore 

    /// Add a list of mapped file between remote and local. 
    /// The first item of each tuple is the local file, and the second item of each tuple is the remote file name. 
    /// The local file will be sent to remote file during the launch of the remote container. 
    member x.AddTo(deps : (string*string)[]) =
        for dep in deps do
            x.AddLocalRemote(dep)
    /// Add a list of mapped file between remote and local. 
    /// The remote file will be under the current JobDirectory, with the same file name and extension of the local file (the path of the local file is ignored). 
    member x.Add(locals : string[]) =
        for local in locals do
            let remote = Path.GetFileName(local)
            x.AddLocalRemote((local, remote))   
    
    /// Add referenced assemblies of the current program into dependency. The remote assembly will be located under current JobDirectory.
    /// If bAddPdb flag is true, PDB file is added to the remote  JobDirectory too. 
    member x.AddRefAssem(bAddPdb : bool) =
        let assems = AssemblyEx.GetAssemAndRefAssem()
        for assem in assems do
            let local = assem.Location
            let remote = Path.GetFileName(local)
            x.AddLocalRemote((local, remote))
            if (bAddPdb) then
                x.AddPdbsTo([|(local, remote)|])

    /// Add corresponding pdbs if they exist
    member internal x.AddPdbsTo(deps : (string*string)[]) =
        for dep in deps do
            let (local, remote) = dep
            let pdb = Path.ChangeExtension(local, "pdb")
            if (File.Exists pdb) then
                let pdbRemote = Path.ChangeExtension(remote, "pdb")
                x.AddLocalRemote((pdb, pdbRemote))
    member internal x.AddPdbs(locals : string[]) =
        for local in locals do
            let remote = Path.GetFileName(local)
            x.AddPdbsTo([|(local, remote)|])

    /// Additional Environmental setting required by the remote container 
    /// It will be used to set ProcessStartInfo.EnvironmentVariables during the launch of the remote container
    member x.AddEnvVars(envvar : string, value : string) =
        if x.bJobLauched then 
            failwith (sprintf "Cannot add environmental variable %s (%s) after job is launched." envvar value)
        else
            x.EnvVars.Add((envvar, value))

    /// Port used by job, if 0us, a port will be assigned by the daemon
    member val internal JobPort = 0us with get, set 

    /// Set Job to use a certain port 
    member x.SetJobPort( port ) =
        x.JobPort <- port 

    /// <summary> 
    /// Add all data files in the directory for remote execution. The program/service that is executed remote should use remote prefix to access the data file. 
    /// </summary>
    /// <param name="prefix"> Defines what is the start of the remote name. </param>
    /// <param name="dirname"> the directory underwhich all data file should be added. </param>
    /// <param name="searchPattern">  The search string to match against the names of files in path. This parameter can contain a combination of valid literal 
    /// path and wildcard (* and ?) characters (see Remarks), but doesn't support regular expressions. </param> 
    /// <param name="searchOption"> One of the enumeration values that specifies whether the search operation should include all subdirectories or only the current directory. </param>
    /// <return>
    /// remoteprefix to be used to access datafile in the directory. 
    /// </return>    
    member x.AddDataDirectoryWithPrefix( prefix: string, dirname:string, altname: string, searchPattern, searchOption ) = 
        let lenAltName = if Utils.IsNull altname then 0 else altname.Length
        let lenPrefix = if Utils.IsNull prefix then 0 else prefix.Length
        let useDirName, remotePrefix = 
            if lenPrefix > 0 then 
                let idx = dirname.IndexOf( prefix, StringComparison.OrdinalIgnoreCase ) 
                if idx >= 0 then 
                    if lenAltName <=0 then 
                        dirname, dirname.Substring( idx + prefix.Length + 1 ).Trim()
                    else
                        dirname, altname
                else 
                    if lenAltName <=0 then 
                        Path.Combine( prefix, dirname ), ""
                    else
                        Path.Combine( prefix, dirname ), altname
            else
                // No prefix
                if lenAltName <=0 then 
                    dirname, ""
                else
                    dirname, altname
        for fname in Directory.GetFiles( useDirName, searchPattern, searchOption ) do
            let idx = fname.IndexOf( useDirName, StringComparison.OrdinalIgnoreCase ) 
            let remoteName = fname.Substring( idx + useDirName.Length + 1 ) 
            let remoteNameWithDir = if remotePrefix.Length > 0 then Path.Combine( remotePrefix, remoteName ) else remoteName
            x.AddLocalRemote((fname, remoteNameWithDir))  
            Logger.LogF(DeploymentSettings.TraceLevelMonitorRemotingMapping, ( fun _ -> sprintf "Remoting %s --> %s" fname remoteNameWithDir ))
        remotePrefix

    /// <summary> 
    /// Add all data files in the directory for remote execution. The program/service that is executed remote should use remote prefix to access the data file. 
    /// </summary>
    /// <param name="dirname"> the directory underwhich all data file should be added. </param>
    /// <param name="searchPattern">  The search string to match against the names of files in path. This parameter can contain a combination of valid literal 
    /// path and wildcard (* and ?) characters (see Remarks), but doesn't support regular expressions. </param> 
    /// <param name="searchOption"> One of the enumeration values that specifies whether the search operation should include all subdirectories or only the current directory. </param>
    /// <return>
    /// remoteprefix to be used to access datafile in the directory. 
    /// </return>    
    member x.AddDataDirectory( dirname:string, searchPattern, searchOption ) = 
        x.AddDataDirectoryWithPrefix( null, dirname, null, searchPattern, searchOption )

    /// <summary> 
    /// Add all data files in the current directory for remote execution. The program/service that is executed remote should use remote prefix to access the data file. 
    /// </summary>
    /// <param name="dirname"> the directory underwhich all data file should be added. </param>
    /// <param name="searchPattern">  The search string to match against the names of files in path. This parameter can contain a combination of valid literal 
    /// path and wildcard (* and ?) characters (see Remarks), but doesn't support regular expressions. Only file in the currently directory is added. 
    /// That is, SearchOption.TopDirectoryOnly is used to traverse the local directory.  </param> 
    /// <return>
    /// remoteprefix to be used to access datafile in the directory. 
    /// </return>    
    member x.AddDataDirectory( dirname:string, searchPattern ) = 
        x.AddDataDirectoryWithPrefix( null, dirname, null, searchPattern, SearchOption.TopDirectoryOnly )

    /// <summary> 
    /// Add all data files in the directory for remote execution. The program/service that is executed remote should use remote prefix to access the data file. 
    /// </summary>
    /// <param name="dirname"> the directory underwhich all data file should be added. The function only adds the file of the current directory, and does not perform recursive 
    /// mapping. </param>
    /// <return>
    /// remoteprefix to be used to access datafile in the directory. 
    /// </return>    
    member x.AddDataDirectory( dirname:string ) = 
        x.AddDataDirectoryWithPrefix( null, dirname, null, "*", SearchOption.TopDirectoryOnly )
    /// Collection of Customized Allocator
    member val internal MemoryManagerCollection = ConcurrentDictionary<Guid,_>() with get
    /// Collection of Customized Serializer 
    member val internal SerializerCollection = ConcurrentDictionary<Guid,_>() with get
    /// Collection of Customized Deserializer by Guid
    member val internal DeserializerCollection = ConcurrentDictionary<Guid,_>() with get
    /// <summary>
    /// Install a customized Memory Manager. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the memory manager in the bytestream. </param>
    /// <param name="allocFunc"> Customized function to grab an obect of 'Type.  </param>
    /// <param name="preallocFunc"> PreAlloc N objects. If 0 is given as input, the function will free all objects in the pool.  </param>
    /// <param name="bAllowReplication"> Whether allow memory manager to be installed repeatedly. </param>
    member x.InstallMemoryManager<'Type when 'Type :> System.IDisposable >( id: Guid, allocFunc: unit->'Type, preallocFunc: int -> unit,  bAllowReplicate ) =  
        let wrappedAllocFunc () = 
            allocFunc ( ) :> Object
        x.InstallWrappedMemoryManager( id, typeof<'Type>.FullName, wrappedAllocFunc, preallocFunc, bAllowReplicate)
    member internal x.InstallWrappedMemoryManager(id, fullname, wrappedAllocFunc, preallocFunc, bAllowReplicate) =
        let tuple = fullname, wrappedAllocFunc, preallocFunc
        let oldTuple = x.MemoryManagerCollection.GetOrAdd( id, tuple )
        if not (Object.ReferenceEquals( oldTuple, tuple )) then 
            let oldName, _, _ = oldTuple
            if String.Compare( oldName, fullname, StringComparison.Ordinal )<>0 then 
                if not bAllowReplicate then 
                    failwith (sprintf "Slot %A, a customized memory manager of type %s has already been installed, fail to install another for type %s"
                                    id oldName fullname)
                else
                    x.MemoryManagerCollection.Item( id ) <- tuple 
                    CustomizedMemoryManager.InstallMemoryManager( fullname, wrappedAllocFunc, preallocFunc )
                    System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a customized memory manager of type %s has already been installed, reinstall another for type %s"
                                                                   id oldName fullname ))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a duplicated customized memory manager of type %s has already been installed"
                                                               id oldName ))
        else
            CustomizedMemoryManager.InstallMemoryManager( fullname, wrappedAllocFunc, preallocFunc )
            System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore

    /// <summary>
    /// Install a customized serializer, with a unique GUID that identified the use of the serializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="encodeFunc"> Customized Serialization function that encodes the 'Type to a bytestream.  </param>
    member x.InstallSerializer<'Type >( id: Guid, encodeFunc: 'Type*Stream->unit, bAllowReplicate ) =  
        let wrappedEncodeFunc (o:Object, ms ) = 
            encodeFunc ( o :?> 'Type, ms )    
        x.InstallWrappedSerializer( id, typeof<'Type>.FullName, wrappedEncodeFunc, bAllowReplicate)
    member internal x.InstallWrappedSerializer(id, fullname, wrappedEncodeFunc , bAllowReplicate) =
        let tuple = fullname, wrappedEncodeFunc
        let oldTuple = x.SerializerCollection.GetOrAdd( id, tuple )
        if not (Object.ReferenceEquals( oldTuple, tuple )) then 
            let oldName, _ = oldTuple
            if String.Compare( oldName, fullname, StringComparison.Ordinal )<>0 then 
                if not bAllowReplicate then 
                    failwith (sprintf "Slot %A, a customized serializer of type %s has already been installed, fail to install another for type %s"
                                    id oldName fullname)
                else
                    x.SerializerCollection.Item( id ) <- tuple 
                    CustomizedSerialization.InstallSerializer( id, fullname, wrappedEncodeFunc )
                    System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a customized serializer of type %s has already been installed, reinstall another for type %s"
                                                                   id oldName fullname ))

            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a duplicated customized serializer of type %s has already been installed"
                                                               id oldName ))
        else
            CustomizedSerialization.InstallSerializer( id, fullname, wrappedEncodeFunc )
            System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore
    /// <summary> 
    /// InstallSerializerDelegate allows language other than F# to install its own type serialization implementation. 
    /// </summary> 
    /// <param name="id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name="fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer. </param>
    /// <param name="del"> An action delegate that perform the serialization function. </param>
    member x.InstallSerializerDelegate( id: Guid, fulltypename, del: CustomizedSerializerAction, bAllowReplicate ) = 
        let wrappedEncodeFunc = del.Invoke
        x.InstallWrappedSerializer( id, fulltypename, wrappedEncodeFunc, bAllowReplicate)
    /// <summary>
    /// Install a customized deserializer, with a unique GUID that identified the use of the deserializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the deserializer in the bytestream. </param>
    /// <param name="decodeFunc"> Customized Deserialization function that decodes bytestream to 'Type.  </param>
    member x.InstallDeserializer<'Type>( id: Guid, decodeFunc: Stream -> 'Type, bAllowReplicate ) = 
        let wrappedDecodeFunc (ms) = 
            decodeFunc ( ms ) :> Object
        x.InstallWrappedDeserializer( id, typeof<'Type>.FullName, wrappedDecodeFunc, bAllowReplicate)
    member internal x.InstallWrappedDeserializer( id, fullname, wrappedDecodeFunc, bAllowReplicate) =
        let tuple = fullname, wrappedDecodeFunc
        let oldTuple = x.DeserializerCollection.GetOrAdd( id, tuple )
        if not (Object.ReferenceEquals( oldTuple, tuple )) then 
            let oldName, _ = oldTuple
            if String.Compare( oldName, fullname, StringComparison.Ordinal )<>0 then 
                if not bAllowReplicate then 
                    failwith (sprintf "Slot %A, a customized deserializer of type %s has already been installed, fail to install another for type %s"
                                        id oldName fullname)
                else
                    x.DeserializerCollection.Item( id ) <- tuple 
                    CustomizedSerialization.InstallDeserializer( id, fullname, wrappedDecodeFunc )
                    System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a customized deserializer of type %s has already been installed, reinstall another for type %s"
                                                                           id oldName fullname))


            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Slot %A, a duplicated customized deserializer of type %s has already been installed"
                                                               id oldName ))
        else
            CustomizedSerialization.InstallDeserializer( id, fullname, wrappedDecodeFunc )
            System.Threading.Interlocked.Increment ( x.nDependencyChanged ) |> ignore
    /// The serializer collection is always coded to a separate bytestream, to be wrapped for delivery 
    /// The bytestream is only deserialized at the remote execution container, not at daemon. 
    member internal x.PackCustomizedFuncCollection( ) = 
        let ms = new MemStream()
        let arrMemoryManager = x.MemoryManagerCollection |> Seq.toArray
        ms.WriteVInt32( arrMemoryManager.Length )
        for pair in arrMemoryManager do 
            let id = pair.Key
            let name, wrappedAllocFunc, preallocFunc = pair.Value
            ms.WriteGuid( id )
            ms.WriteStringV( name )
            ms.Serialize( wrappedAllocFunc )
            ms.Serialize( preallocFunc )
        let arrSerializer = x.SerializerCollection |> Seq.toArray
        ms.WriteVInt32( arrSerializer.Length )
        for pair in arrSerializer do 
            let id = pair.Key
            let name, wrappedEncodeFunc = pair.Value
            ms.WriteGuid( id )
            ms.WriteStringV( name )
            ms.Serialize( wrappedEncodeFunc )
        let arrDeSerializer = x.DeserializerCollection |> Seq.toArray
        ms.WriteVInt32( arrDeSerializer.Length )
        for pair in arrDeSerializer do 
            let id = pair.Key
            let name, wrappedDecodeFunc = pair.Value
            ms.WriteGuid( id )
            ms.WriteStringV( name )
            ms.Serialize( wrappedDecodeFunc )
        ms
    /// The deserializer collection is always coded to a separate bytestream, to be wrapped for delivery 
    /// The bytestream is only deserialized at the remote execution container, not at daemon. 
    member internal x.UnPackCustomizedFuncCollection( ms: MemStream ) = 
        let arrMemoryManager = ms.ReadVInt32( )
        for i=0 to arrMemoryManager - 1 do 
            let mutable bSuccess = false
            let id = ms.ReadGuid()
            let name = ms.ReadStringV()
            let objAllocFunc = ms.Deserialize()
            let objPreallocFunc = ms.Deserialize()
            if Utils.IsNotNull objAllocFunc && Utils.IsNotNull objPreallocFunc then 
                match objAllocFunc with 
                | :? (unit->Object) as wrappedAllocFunc -> 
                    match objPreallocFunc with 
                    | :? ( int -> unit ) as preallocFunc -> 
                        x.InstallWrappedMemoryManager( id, name, wrappedAllocFunc, preallocFunc, true )
                    | _ -> 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Unpack MemoryManager %A of type %s, but the prealloc function of memory manager is not a function of int->unit "
                                                                   id name ))
                | _ -> 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Unpack MemoryManager %A of type %s, but the alloc function of memory manager is not a function of unit->Object "
                                                                   id name ))

        let lenSerializer = ms.ReadVInt32( )
        for i = 0 to lenSerializer - 1 do 
            let id = ms.ReadGuid()
            let name = ms.ReadStringV()
            let obj = ms.Deserialize()
            if Utils.IsNotNull obj then 
                match obj with 
                | :? (Object * Stream->unit) as wrappedEncodeFunc -> 
                    x.InstallWrappedSerializer( id, name, wrappedEncodeFunc, true )
                | _ -> 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Unpack Serializer %A of type %s, but the serializer is not a function of Object*MemStream->unit "
                                                                   id name ))
        let lenDeSerializer = ms.ReadVInt32( )
        for i = 0 to lenDeSerializer - 1 do 
            let id = ms.ReadGuid()
            let name = ms.ReadStringV()
            let obj = ms.Deserialize()
            if Utils.IsNotNull obj then 
                match obj with 
                | :? ( Stream->Object) as wrappedDecodeFunc -> 
                    x.InstallWrappedDeserializer( id, name, wrappedDecodeFunc, true )
                | _ -> 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Unpack Deserializer %A of type %s, but the deserializer is not a function of MemStream->Object "
                                                                   id name ))

    /// <summary> 
    /// InstallDeserializerDelegate allows language other than F# to install its own type deserialization implementation. 
    /// </summary> 
    /// <param name = "id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name = "fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer </param>
    /// <param name = "del"> A function delegate that perform the deserialization function. </param>
    member x.InstallDeserializerDelegate( id: Guid, fulltypename, del: CustomizedDeserializerFunction, bAllowReplicate ) = 
        let wrappedDecodeFunc = del.Invoke
        x.InstallWrappedDeserializer( id, fulltypename, wrappedDecodeFunc, bAllowReplicate )
    /// <summary>
    /// Install a customized serializer, with a unique GUID that identified the use of the serializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="encodeFunc"> Customized Serialization function that encodes the 'Type to a bytestream.  </param>
    static member InstallSerializer<'Type >( id: Guid, encodeFunc: 'Type*Stream->unit ) =    
        JobDependencies.Current.InstallSerializer<_>(id, encodeFunc, false )   
        
    /// <summary>
    /// Install a customized deserializer, with a unique GUID that identified the use of the deserializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the deserializer in the bytestream. </param>
    /// <param name="decodeFunc"> Customized Deserialization function that decodes bytestream to 'Type.  </param>
    static member InstallDeserializer<'Type>( id: Guid, decodeFunc: Stream -> 'Type ) = 
        JobDependencies.Current.InstallDeserializer<_>( id, decodeFunc, false )
    /// <summary> 
    /// InstallSerializerDelegate allows language other than F# to install its own type serialization implementation. 
    /// </summary> 
    /// <param name="id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name="fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer. </param>
    /// <param name="del"> An action delegate that perform the serialization function. </param>
    static member InstallSerializerDelegate( id: Guid, fulltypename, del: CustomizedSerializerAction ) = 
        JobDependencies.Current.InstallSerializerDelegate( id, fulltypename, del, false )
    /// <summary> 
    /// InstallDeserializerDelegate allows language other than F# to install its own type deserialization implementation. 
    /// </summary> 
    /// <param name = "id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name = "fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer </param>
    /// <param name = "del"> A function delegate that perform the deserialization function. </param>
    static member InstallDeserializerDelegate( id: Guid, fulltypename, del: CustomizedDeserializerFunction ) = 
        JobDependencies.Current.InstallDeserializerDelegate( id, fulltypename, del, false )

    /// Remote container execute mode control
    static member val DefaultTypeOfJobMask = JobTaskKind.None with get, set
