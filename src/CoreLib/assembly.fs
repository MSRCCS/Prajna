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
        assembly.fs
  
    Description: 
        Define Assembly in Prajna. 
        An assembly is a set of DLLs, library to be used 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
 // Assemblies wraps around a set of DLL, datas that consists of Prajna
namespace Prajna.Core
open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Reflection
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.FileTools
open Prajna.Tools.FSharp


type internal AssemblyKind = 
    | None = 0
    | ManagedDLL = 1
    | UnmanagedDir = 2
    | ManagedDir = 3
    | IsRelease = 64            // Mask for Release Mode
    | IsSystem = 128            // This flag indicates whether the DLL is part of Prajna system (thus, there is no need to send to the client).
    | SystemDLL = 129

    | File = 0x00               // For packing & unpacking, whether there is one file or more than one file
    | Directory = 0x02
    | MaskForFileType = 0x02
    | MaskForLoad = 0x03        // For Loading

    | MaskForExecutionType = 0x01
    | ExecutionTypeManaged = 0x01
    | ExecutionTypeUnmanaged = 0x00


type internal AssemblyResolver() = 
    static let a = 
        AppDomain.CurrentDomain.add_AssemblyResolve( new ResolveEventHandler( AssemblyResolver.Resolve ) )
    // Assembly that is available. 
    static member val AllAssembly = ConcurrentDictionary<string,string>(StringComparer.Ordinal) with get, set
    static member Resolve (o:Object) (e: ResolveEventArgs) = 
        Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "AssemblyResolver.Resolve: try to resolve assembly %s from %A" e.Name o))
        let refValue = ref Unchecked.defaultof<_>
        let mutable found = AssemblyResolver.AllAssembly.TryGetValue( e.Name, refValue )
        if not found && not (e.Name.Contains(",")) && DeploymentSettings.RunningOnMono then
            // Mono Note: AppDomain.AssemblyResolve event should yield e.Name as a Full assembly name. 
            // However, on Mono 4.0.1 on Ubuntu e.Name is simple name instead of full name. This behavior does
            // not match the spec: http://docs.go-mono.com/?link=P%3aSystem.ResolveEventArgs.Name
            // More investigation is needed. 
            // For now, we will workaround it for Mono. However, it means if there are assemblies of the same name but different versions, it won't work
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Mono: try to resolve assembly %s from %A using simple name" e.Name o))
            found <- AssemblyResolver.AllAssembly.ToArray()
                     |> Array.exists (fun x -> 
                                         let simpleName = (new AssemblyName(x.Key)).Name
                                         if simpleName = e.Name then
                                             refValue := x.Value
                                             true
                                         else
                                             false
                                     )
        if found then 
            let nameLoaded = !refValue
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Dynamically load assembly %s from %s" e.Name nameLoaded ))
            try
                Reflection.Assembly.LoadFrom( nameLoaded )
            with ex ->
                Logger.Log( LogLevel.Info, ( sprintf "Assembly resolve load failed on %s with file %s with exception %A. A likely cause is some unmanaged DLL is required for the managed DLL for execution. Please check if current directory, JobDependencies have been set correctly to load the relevant DLLs " e.Name nameLoaded ex ))
                null                
        else
            let cnt = ref 0
            for pair in AssemblyResolver.AllAssembly do 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Assem %d, %s -> %s" !cnt pair.Key ( pair.Value) ))
                cnt := !cnt + 1 
            let msg = sprintf  "Fail to resolve for assembly %s from source %A" e.Name o 
            Logger.Log( LogLevel.Error, msg )
            failwith msg

and internal AssemblyCollection() = 
    static member val Current = AssemblyCollection() with get
    /// If any new dependency has been added to the file.. 
    member val internal bNewAssembly=true with get, set
    member val internal ReflectionOnlyLoadAssemblyCollection = ConcurrentDictionary< string, AssemblyEx > ( StringComparer.OrdinalIgnoreCase ) with get
    member val private LoadedAssemblyCollection = ConcurrentDictionary< string, Reflection.Assembly > ( StringComparer.OrdinalIgnoreCase ) with get
    member val internal PDBCollection = ConcurrentDictionary< string, int ref >(StringComparer.OrdinalIgnoreCase ) with get
    /// Add Assembly
    member x.AddAssembly( assem: Reflection.Assembly ) = 
        let fullname = assem.FullName
        if not (x.LoadedAssemblyCollection.ContainsKey( fullname )) then 
            let addedAssembly = x.LoadedAssemblyCollection.GetOrAdd( fullname, assem ) 
            if Object.ReferenceEquals( addedAssembly, assem ) then 
                x.bNewAssembly <- true
                try 
                    let pdbName = Path.ChangeExtension( assem.Location, "pdb ")
                    if File.Exists pdbName then 
                        x.PDBCollection.GetOrAdd( pdbName, ref 0 ) |> ignore 
                with 
                | e -> 
                    ()
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Add Assembly %s 1st time, examine its referenced assemblies"
                                                               fullname ))
                /// Newly Added Assembly 
                let assemRefName = assem.GetReferencedAssemblies()
                for aRefName in assemRefName do
                        try 
                            let bExist, aRef = AssemblyCollection.Current.LoadedAssemblyCollection.TryGetValue( aRefName.FullName ) 
                            if not bExist then 
                                let newAsm = Reflection.Assembly.ReflectionOnlyLoad(aRefName.FullName)
                                if Utils.IsNotNull newAsm then 
                                    x.AddAssembly( newAsm ) 
                        with 
                        | e -> 
                            x.LoadedAssemblyCollection.Item( aRefName.FullName) <- null 
                            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Fail to reflection only load reference %s. Prajna will attemp to skip this assembly in remote execution, but it is possible that the remote program may fail due to miss of this assembly. Exception is %A" aRefName.FullName e ))
    /// Get All Assemblies
    member x.GetAllAssemblies() = 
        x.LoadedAssemblyCollection.Values |> Seq.filter ( fun assem -> not (Utils.IsNull assem ) )
    /// Try retrieve Assembly
    member x.TryRetrieveAssembly( fullname ) = 
        let bExist, asm = x.LoadedAssemblyCollection.TryGetValue( fullname ) 
        if bExist then 
            asm
        else
            null


    /// Overload Reflection.Assembly.ReflectionOnlyLoadFrom, so that each assembly is only attempted to be loaded once. 
    member x.ReflectionOnlyLoadFrom( location, hash, bCheckExt ) = 
        let loadFunc fname = 
            try
                let bReflectionLoad = 
                    if not bCheckExt then 
                        true
                    else
                        let ext = Path.GetExtension( fname )
                        String.Compare( ext, ".dll", StringComparison.OrdinalIgnoreCase )=0 || 
                            String.Compare( ext, ".exe", StringComparison.OrdinalIgnoreCase )=0
                if bReflectionLoad then 
                    let assem = Reflection.Assembly.ReflectionOnlyLoadFrom( fname )
                    let asm = AssemblyEx ( TypeOf = AssemblyKind.ManagedDLL,
                                     Name = assem.GetName().Name,
                                     FullName = assem.FullName,
                                     Location = fname,
                                     Hash = hash )
                    AssemblyResolver.AllAssembly.Item( assem.FullName) <- ( fname )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let fileinfo = System.IO.FileInfo( fname )
                                                                   sprintf "Successful to ReflectionOnlyLoadFrom file %s as assembly %s (%A, %dB)" 
                                                                               fname asm.FullName 
                                                                               fileinfo.LastWriteTime fileinfo.Length
                                                                               ))
                    asm
                else
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Data file %s is not considered to contain assembly " fname ))
                    null
            with e->
                // This is OK, some of the DLL is just not managed DLL
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Failed to ReflectionOnlyLoadFrom data file %s" location ))
                null
        x.ReflectionOnlyLoadAssemblyCollection.GetOrAdd( location, loadFunc )
    member x.ContainsKey( location, hash ) = 
        let bEntryExist, _ = x.ReflectionOnlyLoadAssemblyCollection.TryGetValue( location ) 
        bEntryExist

and [<AllowNullLiteral>]
 internal AssemblyEx() = 
    // TODO: Currently, we send all the DLLs to setup the container. Therefore, the concept "IsSystem" may not be useful. 
    //       Consider removing the concept from the code.
    static let systemDLLSet = Dictionary<_,_>( StringComparer.OrdinalIgnoreCase ) 
    static let coreLibDLLSet = List<_>( [| "Prajna"; "Prajna.Tools" |] )
    member val Name = "" with get, set
    member val FullName = "" with get, set
    member val TypeOf = AssemblyKind.None with get, set
    member val Location = "" with get, set
    /// For .Net DLL, this can be the public Key token of the DLL. 
    member val Hash : byte[] = null with get, set
    member x.IsSystem with get() = systemDLLSet.ContainsKey( x.Name )
    member x.IsSame(assem : AssemblyEx) =
        if (x.FullName <> assem.FullName) then
            false
        else if (x.Hash.Length <> assem.Hash.Length) then
            false
        else
            let mutable bSame = true
            let mutable i = 0
            while (bSame && (i < x.Hash.Length)) do
                if (x.Hash.[i] <> assem.Hash.[i]) then
                    bSame <- false
                i <- i + 1
            bSame
    member x.IsCoreLib with get() = coreLibDLLSet.Contains( x.Name )
    override x.ToString() = 
        if Utils.IsNotNull x.Hash then 
            sprintf "%s, %s, %A, %s, %s" x.Name x.FullName x.TypeOf x.Location (BitConverter.ToString( x.Hash ))
        else
            sprintf "%s, %s, %A, %s" x.Name x.FullName x.TypeOf x.Location 
    /// Compute hash based on current file, the hash needs to exactly match what is implemented via Job: StreamToBlob 
    member x.ComputeHash( ) = 
        if Utils.IsNull x.Hash then 
            use ms = new MemStream() 
            x.Pack( ms )  
            x.Hash <- HashByteArrayWithLength( ms.GetBufferPosLength()  )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ ->  let fileinfo = System.IO.FileInfo( x.Location ) 
                                                           sprintf "Pack assembly %s with file %s (%A:%dB) Hash=%s"
                                                                   x.Name x.Location
                                                                   fileinfo.LastWriteTime fileinfo.Length 
                                                                   (BytesToHex( x.Hash ))
                                                                   ))
    /// Pack assembly to stream 
    member x.Pack( ms:MemStream ) = 
//        ms.WriteVInt32( int x.TypeOf )            
//        ms.WriteString( x.Name )
//        ms.WriteString( x.FullName )
        let typeOf = x.TypeOf &&& AssemblyKind.MaskForFileType
        match typeOf with 
        | AssemblyKind.File ->
            let bytearr = ReadBytesFromFile( x.Location )
            ms.WriteBytesWLen( bytearr )
        | AssemblyKind.Directory ->
            let fnames = Directory.GetFiles( x.Location, "*", SearchOption.AllDirectories ) |> Array.sort
            ms.WriteVInt32( fnames.Length )
            for fname in fnames do
                let idx = fname.IndexOf( x.Location ) 
                let usename = 
                    if idx>=0 then 
                        fname.Substring( idx + x.Location.Length + 1 )
                    else
                        fname 
                ms.WriteString( usename )
                use fstream = new FileStream( fname, FileMode.Open, FileAccess.Read )
                let initialLen = fstream.Seek( 0L, SeekOrigin.End )
                let mutable len = initialLen
                fstream.Seek( 0L, SeekOrigin.Begin ) |> ignore
                ms.WriteInt64( len ) 
                let mutable readLen=1024*1024
                let readbuf = Array.zeroCreate<byte> readLen
                while readLen > 0 && len > 0L do
                    readLen <- fstream.Read( readbuf, 0, readbuf.Length )
                    if readLen > 0 then 
                        ms.Write( readbuf, 0, readLen ) 
                        len <- len - int64 readLen
                if len <> 0L then 
                    let msg = sprintf "Error in AssemblyEx.Pack, length of file is different to what is returned by SeekOrigin.End: %s, %d (%d)" fname initialLen len
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
        | _ ->
            let msg = sprintf "Error in AssemblyEx.Pack, Unknown type %A" typeOf
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// Construct folder name for assembly 
    static member ConstructLocation( name, hash ) =
//        let maskedTypeOf = typeOf &&& AssemblyKind.MaskForFileType 
//        match maskedTypeOf with 
//        | _ ->           
            Path.Combine( DeploymentSettings.LocalFolder, DeploymentSettings.HashFolder, BytesToHex(hash) + ".hlnk" )
//        | _ ->
//            let msg = sprintf "Error in AssemblyEx.ConstructLocation, Unknown type %A" typeOf
//            Logger.Log(LogLevel.Error, msg)
//            failwith msg
    /// Is the assembly available locally?
    static member GetAssembly( name, hash, typeOf, bCreateIfNonExist ) = 
        let location = AssemblyEx.ConstructLocation( name, hash )
        let maskedTypeOf = typeOf &&& AssemblyKind.MaskForFileType 
        match maskedTypeOf with 
        | AssemblyKind.File -> 
            if File.Exists location || bCreateIfNonExist then 
                AssemblyEx( Name = name, 
                    FullName = name, 
                    TypeOf = typeOf, 
                    Location = location, 
                    Hash = hash )
            else
                null
        | AssemblyKind.Directory ->
            if Directory.Exists location || bCreateIfNonExist then 
                AssemblyEx( Name = name, 
                    FullName = name, 
                    TypeOf = typeOf, 
                    Location = location, 
                    Hash = hash )
            else
                null
        | _ ->
            null
    /// Unpack assembly from stream 
    member x.Unpack( ms:MemStream ) = 
        let typeOf = x.TypeOf &&& AssemblyKind.MaskForFileType
        match typeOf with 
        | AssemblyKind.File ->
            let bytearr = ms.ReadBytesWLen()
            try
                WriteBytesToFileConcurrentCreate x.Location bytearr
            with
            | :? System.IO.IOException as e -> 
                Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "(May be OK) Failure to write assembly %s, other daemon is writing the same file, msg %s" x.Location e.Message ))

        | AssemblyKind.Directory ->
            let mutable numFiles = ms.ReadVInt32()
            while numFiles>0 do 
                numFiles <- numFiles - 1 
                let usename = ms.ReadString()
                let fname = Path.Combine( x.Location, usename )
                let pathname = Path.GetDirectoryName( fname )
                DirectoryInfoCreateIfNotExists pathname |> ignore
                use fstream = CreateFileStreamForWrite( fname )
                let initialLen = ms.ReadInt64()
                let mutable len = initialLen
                let mutable readLen=1024*1024
                let readbuf = Array.zeroCreate<byte> readLen
                while readLen > 0 && len > 0L do
                    readLen <- ms.Read( readbuf, 0, readbuf.Length )
                    if readLen > 0 then 
                        fstream.Write( readbuf, 0, readLen ) 
                        len <- len - int64 readLen
                fstream.Flush()
                fstream.Close()
                if len <> 0L then 
                    let msg = sprintf "Error in AssemblyEx.UnPack, length of stream is different to what is returned by size: %s, %d (%d)" usename initialLen len
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
        | _ ->
            let msg = sprintf "Error in AssemblyEx.Pack, Unknown type %A" typeOf
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// Load Assembly
    member x.Load( ) = 
        try
            let typeOf = x.TypeOf &&& AssemblyKind.MaskForLoad
            match typeOf with 
            | AssemblyKind.ManagedDLL ->
                if Utils.IsNotNull x.Location && x.Location.Length>0 then 
//                    let assem = Reflection.Assembly.LoadFile( x.Location ) 
//                    x.Name <- assem.GetName().Name
//                    x.FullName <- assem.FullName
//                    let bytearr = ReadBytesFromFile x.Location
//                    let assem = AppDomain.CurrentDomain.Load( bytearr ) 
//                    x.Name <- assem.GetName().Name
//                    x.FullName <- assem.FullName
//                    Reflection.Assembly.Load( x.FullName ) |> ignore
//                    Reflection.Assembly.Load( bytearr ) |> ignore
                    try
                        Logger.Do(DeploymentSettings.ExecutionLevelTouchAssembly, ( fun _ -> TouchFile x.Location ))
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Fail to touch assembly %s, used by debugger?" x.Location ))
                    // let assem = Reflection.Assembly.ReflectionOnlyLoadFrom( x.Location )
                    // We use the function of AssemblyCollection.Current.ReflectionOnlyLoadFrom so that each assembly is loaded at most once. 
                    let assem = AssemblyCollection.Current.ReflectionOnlyLoadFrom( x.Location, x.Hash, false )
                    if not (Utils.IsNull assem) then 
                        x.Name <- assem.Name
                        x.FullName <- assem.FullName
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Use assembly %s at location %s with signature %s " assem.FullName x.Location (BytesToHex(x.Hash)) ))
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Fail to load assembly at %s with hash %s" x.Location (BytesToHex(x.Hash)) ))
                else
                    failwith ( sprintf "AssemblyEx.Load, Managed DLL, Location is empty" )
            | _ ->
                failwith ( sprintf "Don't know how to load Assembly of type %A" x.TypeOf )
        with
        | e ->
            let msg = sprintf "Error in Assembly.Load with exception %A" e
            Logger.Log( LogLevel.Error, msg )
            
    static member GetAssemAndRefAssem() =
        for assem in AppDomain.CurrentDomain.GetAssemblies() do 
            AssemblyCollection.Current.AddAssembly( assem ) 
        AssemblyCollection.Current.GetAllAssemblies() |> Seq.filter ( fun assem -> not (systemDLLSet.ContainsKey( assem.GetName().Name ) ) )
