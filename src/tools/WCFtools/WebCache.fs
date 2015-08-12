(*---------------------------------------------------------------------------
	Copyright 2014 Microsoft

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
		WebCache.fs
  
	Description: 
		Precache web files for in memory serving. 

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Nov. 2014
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.FSharp

open System
open System.Threading
open System.Threading.Tasks
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent

type internal WebCacheFormat() = 
    static member val ImageFormats = 
        [| [| "Bitmap"; "image/bmp"; "bmp" |];
           [| "Graphic interchange format"; "image/gif"; "gif" |];
           [| "JPEG image"; "image/jpeg";  "jpeg" |];
           [| "JPEG image"; "image/jpeg";  "jpg" |];
           [| "JPEG file interchange format"; "image/pipeg"; "jfif" |];
           [| "scalable vector graphic"; "image/svg+xml"; "svg" |];
           [| "TIF image"; "image/tiff"; "tif" |];
           [| "TIF image"; "image/tiff"; "tiff" |];
           [| "icon"; "image/x-icon"; "ico" |];
           [| "portable any map image"; "image/x-portable-anymap"; "pnm" |];
           [| "portable bitmap image"; "image/x-portable-bitmap"; "pbm" |];
           [| "portable graymap image"; "image/x-portable-graymap"; "pgm" |];
           [| "portable pixmap image"; "image/x-portable-pixmap"; "ppm" |];
           [| "RGB bitmap"; "image/x-rgb"; "rgb" |];
           [| "X11 bitmap"; "image/x-xbitmap"; "xbm" |];
           [| "X11 pixmap"; "image/x-xpixmap"; "xpm" |];
           [| "X-Windows dump image"; "image/x-xwindowdump"; "xwd" |] 
        |] with get, set

/// Helper class for caching web object. 
[<AllowNullLiteral>]
type WebCache() = 
    static member val internal Current = new WebCache() with get
    static member val internal ImageFormatsDic = 
        let dic = Dictionary<_,_>(StringComparer.Ordinal)
        for fmt in WebCacheFormat.ImageFormats do 
            let extName = fmt.[2].Trim().ToLower()
            let ty = fmt.[1].Trim().ToLower()
            dic.Add( fmt.[2], fmt.[1] )
        dic with get
    static member internal GetFileType( fname ) = 
        let (|InvariantEqual|_|) (str:string) arg = 
          if String.Compare(str, arg, StringComparison.OrdinalIgnoreCase) = 0
            then Some() else None
        let extWithDot = Path.GetExtension( fname ).Trim().ToLower()
        if extWithDot.Length > 1 then 
            let ext = extWithDot.Substring( 1 )
            let meta = ref "text/html"
            let bImage = WebCache.ImageFormatsDic.TryGetValue( ext, meta )
            if bImage then 
                bImage, !meta
            else
                bImage, "text/html"
        else
            false, "text/html"
    static member internal GetFileExt( meta:string ) =
        let mutable ext = ""
        let useMeta = meta.Trim().ToLower()
        for line in WebCacheFormat.ImageFormats do 
            if String.Compare( line.[1], useMeta, StringComparison.Ordinal )=0 then 
                ext <- "."+line.[2]
        ext
        
    static member internal GetFileMetaType( fname ) = 
        let _, meta = WebCache.GetFileType( fname ) 
        meta
    member val internal Cache = ConcurrentDictionary<_,_>(StringComparer.Ordinal) with get, set
    /// Read the content of a file using async call, and return Task<string> 
    static member TaskReadFile( fileName:string ) = 
        if Utils.IsNotNull fileName then 
            try 
                let logF = new FileStream( fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                let flen = logF.Length
                let readStream = new StreamReader( logF ) 
                let logBuf = readStream.ReadToEndAsync()
                logBuf.ContinueWith( fun (inp:Task<string>) -> readStream.Close()
                                                               logF.Close()
                                                               inp.Result )
            with 
            | e -> 
                let msg = sprintf "Error in reading file %s: %A" fileName e
                new Task<_>( fun _ -> msg )
        else
            new Task<_>( fun _ -> "No File " + fileName )
    static member internal TaskReadFileToBytes( fileName:string ) = 
        if Utils.IsNotNull fileName then 
            try 
                let fstream = new FileStream( fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                let flen = int fstream.Length
                let content = Array.zeroCreate<_> flen
                fstream.ReadAsync( content, 0, flen ).ContinueWith( fun (inp:Task<int>) -> fstream.Close()
                                                                                           if inp.Result = flen then content else null )
            with 
            | e -> 
                null
        else
            null
    member internal x.UpdateItem( name:string, content:byte[] ) = 
        let useName = name.Replace( '\\', '/' ).Trim().ToLower()
        let meta = WebCache.GetFileMetaType( useName )
        x.Cache.Item( useName) <- (content, meta) 
    static member internal updateItem( name, content ) = 
        WebCache.Current.UpdateItem( name, content )
    member internal x.AddItem( name:string, content:byte[] ) = 
        let useName = name.Replace( '\\', '/' ).Trim().ToLower()
        let addFunc (content:byte[]) name = 
            let meta = WebCache.GetFileMetaType( name )
            (content, meta) 
        x.Cache.GetOrAdd( useName, addFunc content ) |> ignore
    /// Add a key-value pair of name, content to WebCache, the metadata of content is determined by the extension of name, 
    static member addItem( name, content ) = 
        WebCache.Current.AddItem( name, content )
    /// Add a key-value pair of name, content to WebCache, with metadata of content explicitly specified 
    member x.AddItemWithMetadata( name:string , content, meta ) = 
        let useName = name.Replace( '\\', '/' ).Trim().ToLower()
        x.Cache.Item( useName) <- (content, meta) 
    /// Remove a key-value pair from WebCache
    member x.RemoveItem( name:string ) = 
        let useName = name.Replace( '\\', '/' ).Trim().ToLower()
        x.Cache.TryRemove( useName, ref Unchecked.defaultof<_> ) |> ignore
    static member internal RemovePrefix( path: string, fname : string ) = 
        let idx = fname.IndexOf( path )
        if idx >=0 then fname.Substring( idx+path.Length+1) else fname
    /// <summary> Add a directory of files to WebCache. The metadata of the file is determined by file extension. 
    /// </summary> 
    /// <param name="path"> The relative or absolute path to the directory to search. This string is not case-sensitive. </param> 
    /// <param name="searchPattern"> The search string to match against the names of files in path. This parameter can contain a combination of valid literal path and wildcard (* and ?) characters (see Remarks), but doesn't support regular expressions.. </param> 
    /// <param name="searchOption"> System.IO.SearchOption
    /// One of the enumeration values that specifies whether the search operation should include all subdirectories or only the current directory. 
    /// </param> 
    member x.AddRoot( path, searchPattern, searchOption ) = 
        let taStartArray = 
             Directory.GetFiles( path,  searchPattern, searchOption ) 
             |> Array.map( fun fname -> fname, WebCache.TaskReadFileToBytes fname )
        let taskArray = 
            taStartArray 
            |> Array.map( fun (fname, ta ) ->    ta.ContinueWith( fun (inp:Task<byte[]>) ->     let content = inp.Result 
                                                                                                if not(Utils.IsNull content) then 
                                                                                                    let nameOnly = WebCache.RemovePrefix( path, fname )
                                                                                                    x.AddItem( nameOnly, content )
                                                                                                    ) )
        Task.WaitAll( taskArray )
    /// <summary> Add a directory of files to WebCache. The metadata of the file is determined by file extension. 
    /// </summary> 
    /// <param name="path"> The relative or absolute path to the directory to search. This string is not case-sensitive. </param> 
    /// <param name="searchPattern"> The search string to match against the names of files in path. This parameter can contain a combination of valid literal path and wildcard (* and ?) characters (see Remarks), but doesn't support regular expressions.. </param> 
    /// <param name="searchOption"> System.IO.SearchOption
    /// One of the enumeration values that specifies whether the search operation should include all subdirectories or only the current directory. 
    /// </param> 
    static member addRoot( path, searchPattern, searchOption ) = 
        WebCache.Current.AddRoot( path, searchPattern, searchOption )
    /// Retrieve a content, meta entry from WebCache with key name. 
    member x.Retrieve( name:string ) = 
        let useName = name.Replace( '\\', '/' ).Trim().ToLower()
        let valRef = ref Unchecked.defaultof<_>
        if x.Cache.TryGetValue( useName, valRef ) then 
            !valRef 
        else
            null, "text/html"
    /// Retrieve a content, meta entry from WebCache with key name.
    static member retrieve (name ) = 
        WebCache.Current.Retrieve( name ) 
    /// Provided a snapshot of all entries in the WebCache sorted by key. 
    member x.SnapShot() = 
        let info = x.Cache.ToArray()
        let sortedInfo = info |> Array.sortBy( fun pair -> pair.Key ) 
        sortedInfo
    /// Provided a snapshot of all entries in the WebCache sorted by key. 
    static member snapShot() = 
        WebCache.Current.SnapShot()

type internal WebCacheRegisterable( cache: WebCache) = 
    static member val Current = new WebCacheRegisterable(null) with get
    member val Cache = if Utils.IsNull cache then WebCache.Current else cache with get, set
    member val RefCounting = ConcurrentDictionary<_,_>( BytesCompare() ) with get
    member x.RegisterItem( prefix:string, content:byte[], meta ) = 
        let hashcode = HashByteArray( content )
        let url = if Utils.IsNull prefix then 
                        hashcode.ToString() + WebCache.GetFileExt( meta ) 
                  else
                        prefix + "/" + hashcode.ToString() + WebCache.GetFileExt( meta ) 
        let refCount = x.RefCounting.GetOrAdd( hashcode, ref 0) 
        if Interlocked.Increment( refCount ) = 1 then 
            // First time, register
            x.Cache.AddItemWithMetadata( url, content, meta )
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Register %s count = %d" (BytesToHex hashcode) (!refCount) ) )
        hashcode, url
    member x.UnregisterItem( hashcode, url ) = 
        let refC = ref Unchecked.defaultof<_>
        if x.RefCounting.TryGetValue( hashcode, refC ) then 
            let refCount = !refC
            if Interlocked.Decrement( refCount ) = 0 then 
                // Remove Item
                x.Cache.RemoveItem( url )
                x.RefCounting.TryRemove( hashcode, refC ) |> ignore
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Unregister %s count = %d" (BytesToHex hashcode) (!refCount) ) )

    static member registerItem( prefix, content, meta ) = 
        WebCacheRegisterable.Current.RegisterItem( prefix, content, meta ) 
    static member unregisterItem( hashcode, url ) = 
        WebCacheRegisterable.Current.UnregisterItem( hashcode, url ) 
            
