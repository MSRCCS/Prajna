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
        file.fs
  
    Description: 
        Filestream interface of Prajna. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Concurrent
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

type internal StorageStructureKind = 
    | Directory = 0
    | Blob = 1

/// Common Base Class for Storage in 
[<AbstractClass; AllowNullLiteral>]
type internal StorageStream() = 
    inherit Stream()
    /// Recommended size of the wrapper Buffered Stream for performance optimization
    abstract member RecommendedBufferSize : int with get
    /// Does this storage element support persistency (e.g., Azure Blob is considered persistent
    abstract member Persistent : bool with get 
    /// List the content of the directory 
    /// parameter ( [| path1, path2, ..., pathn |] )
    /// Return: 
    /// an array of items in the directory
    /// each item contains its name, type (directory or blob), and the size of the content (0: for directory)
    abstract member List: string[] * string -> ( string * StorageStructureKind * int64 )[]
    /// Delete blobs
    /// Parameter: ( [| path1, path2, ..., pathn |], [|name1, name2, ..., namei|] )
    abstract member Delete: string[] * string -> unit
    /// Create blobs
    /// Parameter: ( [| path1, path2, ..., pathn |], [|name1, name2, ..., namei|], File.Mode )
    /// Return:
    /// An array of StorageStream to be written upon
    abstract member Create: string[] * string[] -> StorageStream[]
    /// Open blobs for read
    /// Parameter: ( [| path1, path2, ..., pathn |], [|name1, name2, ..., namei|], File.Mode )
    /// Return:
    /// An array of StorageStream to be written upon
    abstract member Open: string[] * string[] -> StorageStream[]
    /// Is a certain file exist
    abstract member Exist: string[] * string[] -> bool[]
    /// Choose files to be created and written upon
    /// Parameter: ( [| path1, path2, ..., pathn |], [|name1, name2, ..., namei|])
    /// Return:
    /// An array of files to be created and written upon
    abstract member Choose: string[] * string[] -> string[]

    /// Is this a local store
    abstract member IsLocal : bool with get

    member x.List( path1, pattern ) = x.List( [| path1 |], pattern )
    member x.List( path1, path2, pattern ) = x.List( [| path1; path2 |], pattern )
    member x.List( path1, path2, path3, pattern ) = x.List( [| path1; path2; path3 |], pattern )
    member x.Delete( path1:string, pattern:string ) = x.Delete( [| path1 |], pattern )
    member x.Delete( path1:string, path2, pattern:string ) = x.Delete( [| path1; path2 |], pattern )
    member x.Delete( path1:string, path2, path3, pattern:string ) = x.Delete( [| path1; path2; path3 |], pattern )
    member x.Create( pathes:string[], fname:string ) = 
        let streamArray = x.Create( pathes, [| fname |] )
        streamArray.[0]
    member x.Create( path1:string, fname:string ) = x.Create( [| path1 |] , fname )
    member x.Create( path1:string, path2, fname:string ) = x.Create( [| path1; path2 |] , fname )
    member x.Create( path1:string, path2, path3, fname:string ) = x.Create( [| path1; path2; path3 |] , fname )
    member x.Create( path1:string, fnames:string[] ) = x.Create( [| path1 |], fnames )
    member x.Create( path1:string, path2, fnames:string[] ) = x.Create( [| path1; path2 |], fnames )
    member x.Create( path1:string, path2, path3, fnames:string[] ) = x.Create( [| path1; path2; path3 |], fnames )
    member x.Open( pathes:string[], fname:string ) = 
        let streamArray = x.Open( pathes, [| fname |] )
        streamArray.[0]
    member x.Open( path1:string, fname:string ) = x.Open( [| path1 |] , fname )
    member x.Open( path1:string, path2, fname:string ) = x.Open( [| path1; path2 |] , fname )
    member x.Open( path1:string, path2, path3, fname:string ) = x.Open( [| path1; path2; path3 |] , fname )
    member x.Open( path1:string, fnames:string[] ) = x.Open( [| path1 |], fnames )
    member x.Open( path1:string, path2, fnames:string[] ) = x.Open( [| path1; path2 |], fnames )
    member x.Open( path1:string, path2, path3, fnames:string[] ) = x.Open( [| path1; path2; path3 |], fnames )
    member x.Exist( pathes:string[], fname:string ) = 
        x.Exist( pathes, [| fname |] )       
    member x.Exist( path1:string, path2, fname:string ) = x.Exist( [| path1; path2 |] , fname )
    member x.Exist( path1:string, path2, path3, fname:string ) = x.Exist( [| path1; path2; path3 |] , fname )
    member x.Exist( path1:string, fnames:string[] ) = x.Exist( [| path1 |], fnames )
    member x.Exist( path1:string, path2, fnames:string[] ) = x.Exist( [| path1; path2 |], fnames )
    member x.Exist( path1:string, path2, path3, fnames:string[] ) = x.Exist( [| path1; path2; path3 |], fnames )
    member x.Choose( pathes:string[], fname:string ) = 
        let files = x.Choose( pathes, [| fname |] )
        files.[0]
    member x.Choose( path1:string, fname:string ) = x.Choose( [| path1 |] , fname )
    member x.Choose( path1:string, path2, fname:string ) = x.Choose( [| path1; path2 |] , fname )
    member x.Choose( path1:string, path2, path3, fname:string ) = x.Choose( [| path1; path2; path3 |] , fname )
    member x.Choose( path1:string, fnames:string[] ) = x.Choose( [| path1 |], fnames )
    member x.Choose( path1:string, path2, fnames:string[] ) = x.Choose( [| path1; path2 |], fnames )
    member x.Choose( path1:string, path2, path3, fnames:string[] ) = x.Choose( [| path1; path2; path3 |], fnames )

    default x.Persistent = false
    default x.RecommendedBufferSize = 1024 * 1024
    default x.IsLocal = true

[<AllowNullLiteral>]
type internal HDDStream() = 
    inherit StorageStream()
    let mutable fStream : FileStream = null
    member x.Stream with get() = fStream
                     and set( s ) = fStream <- s
    override x.CanRead = fStream.CanRead
    override x.CanWrite = fStream.CanWrite
    override x.CanSeek = fStream.CanSeek
    override x.Length = fStream.Length
    override x.Position with get() = fStream.Position
                        and set( pos ) = fStream.Position <- pos
    override x.Flush() = if Utils.IsNotNull fStream then fStream.Flush()
    override x.Close() = if Utils.IsNotNull fStream then fStream.Close()
    override x.Seek( offset, origin ) = fStream.Seek( offset, origin)
    override x.SetLength( len ) = fStream.SetLength( len ) 
    override x.Read( buf, pos, len ) = fStream.Read( buf, pos, len ) 
    override x.Write( buf, pos, len ) = fStream.Write( buf, pos, len )
    override x.BeginWrite( buf, offset, count, callback, state ) = 
        fStream.BeginWrite( buf, offset, count, callback, state )
    override x.EndWrite( ar ) =
        fStream.EndWrite( ar ) 
    override x.BeginRead( buf, offset, count, callback, state ) = 
        fStream.BeginRead( buf, offset, count, callback, state )
    override x.EndRead( ar ) = 
        fStream.EndRead( ar ) 
    override x.Dispose( disposing ) = 
        x.Close()
        GC.SuppressFinalize(x)
    override x.Finalize() =
        x.Close()   
    override x.ReadAsync( buffer, offset, count, cancellationToken  ) = 
        fStream.ReadAsync( buffer, offset, count, cancellationToken )
    override x.WriteAsync( buffer, offset, count, cancellationToken  ) = 
        fStream.WriteAsync( buffer, offset, count, cancellationToken )
    override x.FlushAsync( cancellationToken ) = 
        fStream.FlushAsync( cancellationToken )

    member private x.ConstructPathNameInternal( pathnames, bCreate ) = 
        let allPathnames = seq { 
//                            for drive in DriveInfo.GetDrives() do
//                                if drive.IsReady && drive.DriveType=DriveType.Fixed && drive.Name.Length>1 && Char.ToUpper(drive.Name.Chars(0))<>'C' then  
                              for drive in DeploymentSettings.GetAllDataDrivesInfo() do
                                    let nameArray = seq { 
                                        yield drive.Name
                                        yield DeploymentSettings.DataFolder
                                        yield! pathnames }
                                    let patharr = nameArray |> Seq.toArray
                                    /// Create Directory along the way if any subdirectory doesn't exist. 
                                    /// We call StringTools.DirectoryInfoCreateIfNonExist so that the 
                                    /// directory created is always accessable by everyone.                                     
                                    if bCreate then 
                                        let bDirExist = ref true
                                        for i=1 to patharr.Length-1 do
                                            let pathname = Path.Combine( patharr.[0..i] )
                                            let bDirInfo = FileTools.DirectoryInfoCreateIfNotExists( pathname )
                                            bDirExist := bDirInfo.Exists
                                        if !bDirExist then 
                                            yield ( Path.Combine( patharr) , drive.TotalFreeSpace )
                                    else
                                        let fullpathname = Path.Combine( patharr)
                                        if Directory.Exists(fullpathname) then 
                                            yield ( fullpathname, drive.TotalFreeSpace )
                        }
        allPathnames |> Seq.toArray
    member val private cachedPathnames = ConcurrentDictionary<_,(string*int64)[]>() with get
    member x.ConstructPathName( pathnames, bCreate ) =
        let fullpath = Path.Combine( pathnames |> Seq.toArray )
        // From ConcurrentDictionary
        // If you call GetOrAdd simultaneously on different threads, addValueFactory may be called multiple times, but its key/value pair might not be added to the dictionary for every call. 
        x.cachedPathnames.GetOrAdd( fullpath, fun _ -> x.ConstructPathNameInternal( pathnames, bCreate ) )
    override x.List( pathnames, pattern ) = 
        let dirInfo = x.ConstructPathName( pathnames, false )
        let allEntries = seq {
            for dirEntry in dirInfo do
                let dirname, freespace = dirEntry
                let dirInfo = DirectoryInfo( dirname )
                let allDirs = if Utils.IsNull pattern then dirInfo.GetDirectories() else dirInfo.GetDirectories( pattern )
                for dir in allDirs do
                    yield ( dir.Name, StorageStructureKind.Directory, 0L )
                let allFiles = if Utils.IsNull pattern then dirInfo.GetFiles() else dirInfo.GetFiles( pattern )
                for file in allFiles do
                    yield ( file.Name, StorageStructureKind.Blob, file.Length )
        }
        allEntries |> Seq.toArray
    override x.Delete( pathnames, pattern ) = 
        let dirInfo = x.ConstructPathName( pathnames, false )
        for dirEntry in dirInfo do
            let dirname, freespace = dirEntry
            let dirInfo = DirectoryInfo( dirname )
            let allFiles = if Utils.IsNull pattern then dirInfo.GetFiles() else dirInfo.GetFiles( pattern )
            for file in allFiles do
                file.Delete()
            let remainingFiles = dirInfo.GetFileSystemInfos()
            if remainingFiles.Length<=0 then 
                dirInfo.Delete()

    override x.Choose( pathnames, filenames ) =
        // Seed Random with different number, so that 
        // different file slot to different drive (because highly synchronized thread execution, the default seed by time behavior 
        // caused the random generator to generate same random number across threads. 
        let rnd = RandomWithSalt( filenames.[0].GetHashCode() )
        let orgDirInfo = x.ConstructPathName( pathnames, true )
        let dirInfo = orgDirInfo |> Array.filter ( fun ( _, freespace ) -> freespace > (1000L<<<20) ) // We don't write to driver with less than 1GB free space, 
        let files = Array.zeroCreate<string> filenames.Length
        if dirInfo.Length>0 then 
            let pathnames = dirInfo |> Array.map ( fun ( pathname, _ ) -> pathname )
            let freespaces = dirInfo |> Array.map ( fun ( _, freespace ) -> int (freespace>>>20) ) /// Free space in MB, this is good to 2PB, should be pretty good enough. 
            let totalspaces = freespaces |> Array.fold ( fun sum s -> sum+s ) 0
            for i=0 to filenames.Length-1 do 
                let fname = filenames.[i]
                // The following code randomly pick a driver to hold the current file based on the capacity of the 
                let pntrnd = rnd.Next( totalspaces )
                let mutable pnt = pntrnd
                let dr = ref 0
                while pnt >= freespaces.[!dr] && !dr<=freespaces.Length-1 do 
                    pnt <- pnt - freespaces.[!dr]
                    dr := !dr + 1
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Random %d of %d, save %s to drive %s" pntrnd totalspaces fname (pathnames.[!dr].Substring(0,2)) ))
                let usefilename = Path.Combine( pathnames.[!dr], fname )
                files.[i] <- usefilename
        else
            failwith "Can't find a drive with enough capacity to perform the operation"
        files
                    
    override x.Create( pathnames, filenames ) =
        let files = x.Choose(pathnames, filenames)
        files |> Array.map (fun f -> new HDDStream( Stream=FileTools.CreateFileStreamForWrite( f ) ) :> StorageStream)

    override x.Open( pathnames, filenames ) =
        let dirInfo = x.ConstructPathName( pathnames, false )
        let streamArray = Array.zeroCreate<StorageStream> filenames.Length
        if dirInfo.Length>0 then 
            let pathnames = dirInfo |> Array.map ( fun ( pathname, _ ) -> pathname )
            for i=0 to filenames.Length-1 do 
                let fname = filenames.[i]
                // The following code randomly pick a driver to hold the current file based on the capacity of the 
                let mutable dr = 0
                let mutable bFind = false
                while dr<pathnames.Length && not bFind do 
                    let usename = Path.Combine( pathnames.[dr], fname )
                    if File.Exists usename then 
                        bFind <- true    
                    else
                        dr <- dr + 1
                if bFind then 
                    let usename = Path.Combine( pathnames.[dr], fname )
                    streamArray.[i] <- new HDDStream( Stream=FileTools.CreateFileStreamForRead( usename ) ) :> StorageStream
        else
            failwith "Can't find a drive with enough capacity to perform the operation"
        streamArray
    override x.Exist( pathnames, filenames ) = 
        let listArray = x.List( pathnames, null ) |> Array.map( fun ( name, _, _ ) -> name )
        let bFind = Array.create filenames.Length false
        for i=0 to filenames.Length - 1 do
            for j=0 to listArray.Length - 1 do
                if String.Compare( filenames.[i], listArray.[j], true )=0 then
                    bFind.[i] <- true
        bFind
            
    interface IDisposable with
        member x.Dispose() = 
            if Utils.IsNotNull fStream then
                fStream.Dispose()
            GC.SuppressFinalize(x);
