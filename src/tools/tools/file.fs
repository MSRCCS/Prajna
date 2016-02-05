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
        File.fs
  
    Description: 
        Helper function for file operation

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013
    
 ---------------------------------------------------------------------------*)
 
namespace Prajna.Tools

open System
open System.IO
open System.Security.AccessControl
open System.Security.Principal

open System.Text

open Prajna.Tools.FSharp

/// <summary>
/// A set of helper routine for file operations
/// </summary>
module  FileTools =

    /// Return DirectoryInfo, create the directory if it doesn't exist. 
    /// The directory created this way will allow access control by everyone to ease use in cluster scenario. 
    let DirectoryInfoCreateIfNotExists dir =
        DirUtils.DirectoryInfoCreateIfNotExists dir

    /// Get drive letter 
    let GetDrive (dir:string) =
        let pos = dir.IndexOf(':')
        if pos > 0 then 
            dir.Substring( 0, pos )
        else
            ""

    /// Read a byte[] from files
    let ReadBytesFromFile (filename ) = 
        use fileStream= new FileStream( filename, FileMode.Open, FileAccess.Read )
        let len = fileStream.Seek( 0L, SeekOrigin.End )
        fileStream.Seek( 0L, SeekOrigin.Begin ) |> ignore
        if len > int64 Int32.MaxValue then 
            failwith "ReadBytesFromFile isn't capable to deal with file larger than 2GB"
        let bytes = Array.zeroCreate<byte> (int len)
        let readLen = fileStream.Read( bytes, 0, int len )
        if readLen < int len then 
            failwith (sprintf "ReadBytesFromFile failed to read to the end of file (%dB), read in %dB" len readLen )
        bytes

    /// <summary>
    /// Make the file accessible to everyone, so that other process can manage the file. 
    /// </summary>
    let internal MakeFileAccessible (fname ) = 
        if not Runtime.RunningOnMono then
            let fSecurity = File.GetAccessControl( fname ) 
            let everyoneSid = SecurityIdentifier( WellKnownSidType.WorldSid, null )
            fSecurity.AddAccessRule( new FileSystemAccessRule( everyoneSid, FileSystemRights.FullControl, AccessControlType.Allow ) )
            File.SetAccessControl( fname, fSecurity )
        else
            // Mono Note: Mono does not support File.GetAccessControl on linux. To-be-investigated
            ()

    /// <summary>
    /// Create a file stream to write, and make the file accessible to everyone (for shared use in a cluster)
    /// Caller should be responsible for dispose the returned stream.
    /// </summary>
    let CreateFileStreamForWrite( fname ) = 
        let fStream = new FileStream( fname, FileMode.Create, Security.AccessControl.FileSystemRights.FullControl, 
                                      FileShare.Read, (1<<<20), FileOptions.Asynchronous )
        // File created are given full control by everyone, this eases the job of executing file under multiuser scenario
        MakeFileAccessible (fname )
        fStream

    // Caller needs to be responsible for dispose the returned fStream
    let internal CreateFileStreamForWriteWOBuffer( fname ) = 
        let fStream = new FileStream( fname, FileMode.Create, Security.AccessControl.FileSystemRights.FullControl, 
                                      FileShare.Read, 0, FileOptions.WriteThrough )
        // File created are given full control by everyone, this eases the job of executing file under multiuser scenario
        MakeFileAccessible (fname )
        fStream

    /// <summary>
    /// Create a file stream to read
    /// Only when the file is still hold by another process, wait for a small file to try to read. 
    /// </summary>
    let CreateFileStreamForRead( fname ) = 
        let mutable nTry = 3 
        let mutable ret = null 
        while Utils.IsNull ret && nTry > 0 do 
            try 
                ret <- new FileStream( fname, FileMode.Open, FileAccess.Read, FileShare.Read, (1<<<20), FileOptions.SequentialScan )
                if Utils.IsNull ret then 
                    nTry <- 0 
            with 
            | :? System.IO.IOException as ex -> 
                if ex.Message.IndexOf( "being used by another process") > 0 then 
                    System.Threading.Thread.Sleep( 10 )
                    nTry <- nTry - 1
                else
                    nTry <- 0 
                    reraise()
            | ex -> 
                reraise()
        ret

    let internal CreateFileStreamForReadWOBuffer( fname ) = 
        let mutable nTry = 3 
        let mutable ret = null 
        while Utils.IsNull ret && nTry > 0 do 
            try 
                ret <- new FileStream( fname, FileMode.Open, FileAccess.Read, FileShare.Read, 0, FileOptions.SequentialScan )
                if Utils.IsNull ret then 
                    nTry <- 0 
            with 
            | :? System.IO.IOException as ex -> 
                if ex.Message.IndexOf( "being used by another process") > 0 then 
                    System.Threading.Thread.Sleep( 10 )
                    nTry <- nTry - 1
                else
                    nTry <- 0 
                    reraise()
            | ex -> 
                reraise()
        ret 

    let internal WriteBytesToFileP filename bytes offset len = 
        use fileStream = CreateFileStreamForWrite( filename )
        fileStream.Write( bytes, offset, len )
        fileStream.Close() 

    /// Write a byte[] to file 
    let WriteBytesToFile filename bytes = 
        WriteBytesToFileP filename bytes 0 bytes.Length

    /// Append a string to file
    let AppendToFile (filename:string) (content:string) = 
        use file = File.AppendText(filename)
        file.Write(content)
        file.Close()

    /// Touch a file, change its last write time to now. 
    let TouchFile fileName = 
        File.SetLastWriteTimeUtc(fileName, (PerfDateTime.UtcNow()));

    /// Create a link of a file on Windows using Interop
    let private LinkFileWindows dstFileName existingFileName = 
        let sb = StringBuilder()
        if File.Exists dstFileName then
            try 
                File.Delete( dstFileName ) |> ignore 
            with 
            | e -> 
                sb.AppendLine(sprintf "(May be OK) Fail to delete file %s" dstFileName) |> ignore                 
        try
            let res = InteropWithKernel32.CreateHardLink(dstFileName, existingFileName, IntPtr.Zero)
            if not res then
                sb.AppendLine(sprintf "Create Hard Link Failed with error code 0x%x" (Runtime.InteropServices.Marshal.GetLastWin32Error())) |> ignore
                false, sb.ToString()
            else
                try
                    TouchFile existingFileName
                    true, sb.ToString()
                with 
                | e -> 
                        sb.AppendLine(sprintf "(May be OK) Fail to touch file %s/%s" dstFileName existingFileName) |> ignore
                        false, sb.ToString()
        with 
        | e -> 
            sb.AppendLine(sprintf "Failed to Create hard link %s <-- %s with exception %A" dstFileName existingFileName e ) |> ignore
            false, sb.ToString()

    /// Copy file to a destination 
    let internal CopyFile dstFileName existingFileName = 
        let sb = StringBuilder()
        if File.Exists dstFileName then
            try 
                File.Delete( dstFileName ) |> ignore 
            with 
            | e -> 
                 sb.AppendLine(sprintf "(May be OK) Fail to delete file %s" dstFileName )  |> ignore
        try
            File.Copy(existingFileName, dstFileName)
            try
                TouchFile existingFileName
                true, sb.ToString()
            with 
            | e -> 
                   sb.AppendLine(sprintf "(May be OK) Fail to touch file %s/%s" dstFileName existingFileName) |> ignore
                   false, sb.ToString()
        with 
        | e -> 
            sb.AppendLine(sprintf "Failed to Copy File %s <-- %s with exception %A" dstFileName existingFileName e ) |> ignore
            false, sb.ToString()

    /// Create a link of an existing file
    let internal LinkFile dstFileName existingFileName = 
        if not Runtime.RunningOnMono then
            LinkFileWindows dstFileName existingFileName
        else
            // Mono: copy the file instead.
            // TODO: explore the use of Mono.Unix.Native.Syscall.link method 
            // http://api.xamarin.com/?link=M%3aMono.Unix.Native.Syscall.link(System.String%2cSystem.String)
            CopyFile dstFileName existingFileName

    /// Recursively traverse directories
    let rec internal RecurGetFiles dir = 
        seq { yield! Directory.GetFiles(dir)
              for subdir in Directory.GetDirectories(dir) do yield! RecurGetFiles subdir }

    /// Is file of a certain extension
    let internal IsExts exts (file:string) = 
        exts |> Array.exists (fun ext -> file.EndsWith(ext, true, null))

    /// Is file of a relative path
    let internal IsRelativePath (fname:string) =         
        if Utils.IsNull fname then 
            true
        else
            not (Path.IsPathRooted(fname))

    /// Filter extensions. 
    let internal FilterExts exts files =
        files |> Seq.filter ( IsExts exts )

    /// Recurisive traverse directories with filter
    let internal RecursiveGetFiles dir exts = 
        RecurGetFiles dir |> FilterExts exts |> Seq.toArray


    /// Write a byte[] to file, create the directory of the file if it doesn't exist
    let WriteBytesToFileCreate (filename : string) bytes =
        let dirpath = Path.GetDirectoryName( filename )
        if Utils.IsNotNull dirpath then
            // Create directory if necessary
            DirectoryInfoCreateIfNotExists (dirpath) |> ignore
        WriteBytesToFile filename bytes

    /// Write a byte[] to file, with possibility of multiple processes are writing the exact same file at the same 
    /// time. Such situation occurs for the Prajna daemon to write cluster metadata, DKV metadata, 
    /// Assembly, dependencies file, etc.. WriteBytesToFileConcurrentPCompare may check (if bComp=true) if the file has been written concurrently, 
    /// if that is the case, one of the process will verify (read) the file and make sure that is the same content 
    /// that is to be written. 
    let internal WriteBytesToFileConcurrentPCompare filename bytes offset len bComp = 
        let bExist = ref false
        let message = ref ""
        if File.Exists( filename ) then 
            bExist := true 
        else
            Logger.LogF(LogLevel.MediumVerbose,  fun _ -> sprintf "WriteBytesToFileConcurrentPCompare: create and write file '%s'" filename)
            try 
                use fileStream = CreateFileStreamForWrite( filename )
                fileStream.Write( bytes, offset, len )
                fileStream.Close() 
            with 
            | e -> 
                message := e.Message
                bExist := true
            Logger.LogF(LogLevel.MediumVerbose,  fun _ -> sprintf "WriteBytesToFileConcurrentPCompare: complete create and write file '%s' (exists = %b)" filename !bExist)
        if !bExist && bComp then 
            Logger.LogF(LogLevel.MediumVerbose,  fun _ -> sprintf "WriteBytesToFileConcurrentPCompare: verify file '%s'" filename)
            let nVerified = ref 0
            let mutable lastTicksRead = (PerfADateTime.UtcNowTicks())
            let mutable backOff = 10.
            let maxVerifiedOnce = 1<<<20
            let bufread = Array.zeroCreate<_> maxVerifiedOnce
            /// Try to verify the bytestream being written to the new file 
            while !nVerified >= 0 && !nVerified < len do
                use fileStream= 
                    let mutable bDone = false
                    let mutable retFileStream = null
                    while not bDone do 
                        try 
                            retFileStream <- new FileStream( filename, FileMode.Open, FileAccess.Read )
                            bDone <- true
                        with 
                        | e -> 
                            // Can't open for read, may wait
                            let curTicks = (PerfADateTime.UtcNowTicks())
                            let secondsElapse = int (( curTicks - lastTicksRead ) / TimeSpan.TicksPerSecond)
                            if secondsElapse > 5 + ( len >>> 20 ) then 
                                message := sprintf "WriteBytesToFileConcurrentP: timeout to wait for file %s to be readable (towrite=%dB, wait %d sec), last exception is %A"
                                                        filename len secondsElapse e
                                nVerified := -1 
                                bDone <- true
                            else
                                Threading.Thread.Sleep( TimeSpan.FromMilliseconds(backOff) )
                                backOff <- backOff * 2.
                    retFileStream
                let mutable bDoneVerify = (Utils.IsNull fileStream)
                while not bDoneVerify do 
                    let filelen = int (fileStream.Seek( 0L, SeekOrigin.End ))
                    if filelen > len then 
                        bDoneVerify <- true
                        nVerified := -1
                        message := sprintf "The length of file %s is %d, which is larger than %dB written requested by WriteBytesToFileConcurrentP" 
                                            filename filelen len
                    else
                        if filelen > !nVerified then 
                            /// Something to read
                            fileStream.Seek( int64 !nVerified, SeekOrigin.Begin ) |> ignore
                            while not bDoneVerify do 
                                let nMaxRead = Math.Min( maxVerifiedOnce, filelen - !nVerified )
                                let readlen = fileStream.Read( bufread, 0, nMaxRead )
                                if readlen > 0 then 
                                    let mutable cmp = 0
                                    let mutable bComp = true
                                    while bComp && cmp < readlen do
                                        if bytes.[ !nVerified + cmp ]=bufread.[cmp] then 
                                            cmp <- cmp + 1
                                        else
                                            bComp <- false
                                    bDoneVerify <- not bComp
                                    if not bDoneVerify then 
                                        // Verification still continues 
                                        nVerified := !nVerified + readlen
                                        bDoneVerify <- !nVerified >= filelen
                                    else
                                        message := sprintf "The file %s is different from what is written to WriteBytesToFileConcurrentP at byte %d (filelen=%d, towrite=%d)." 
                                                            filename (!nVerified + cmp ) filelen len
                                        nVerified := -1 
                            lastTicksRead <- (PerfADateTime.UtcNowTicks())
                        else
                            let curTicks = PerfADateTime.UtcNowTicks()
                            let elapseTicks = curTicks - lastTicksRead
                            if elapseTicks > TimeSpan.TicksPerSecond then 
                                message := sprintf "The file %s is of length %d and hasn't been written for more than 1 seconds, the expected length of WriteBytesToFileConcurrentP is %d." 
                                                    filename filelen len
                                nVerified := -1 
                            else
                                Threading.Thread.Sleep( 5 )
            Logger.LogF(LogLevel.MediumVerbose,  fun _ -> sprintf "WriteBytesToFileConcurrentPCompare: done verify file '%s' (verified = %d), detail: %s" filename !nVerified !message)
            if !nVerified < 0 then 
                failwith !message

    /// Write a byte[] to file, with possibility of multiple processes are writing the exact same file at the same 
    /// time. Such situation occurs for the Prajna daemon to write cluster metadata, DKV metadata, 
    /// Assembly, dependencies file, etc.. WriteBytesToFileConcurrent checks if the file has been written concurrently, 
    /// if that is the case, one of the process will verify (read) the file and make sure that is the same content 
    /// that is to be written. 
    let internal WriteBytesToFileConcurrentP filename bytes offset len = 
        WriteBytesToFileConcurrentPCompare filename bytes offset len true

    /// Write a byte[] to file, with possibility of multiple processes are writing to the exact same file at the same 
    /// time. Such situation occurs for the Prajna daemon to write cluster metadata, DKV metadata, 
    /// Assembly, dependencies file, etc.. WriteBytesToFileConcurrent checks if the file has been written concurrently, 
    /// if that is the case, one of the process will verify (read) the file and make sure that is the same content 
    /// that is to be written. 
    let WriteBytesToFileConcurrent filename bytes = 
        WriteBytesToFileConcurrentP filename bytes 0 bytes.Length

    /// Write a byte[] to file, create the directory of the file if it doesn't exist. The call deal with possibility of multiple processes are writing to the exact same file at the same 
    /// time. Such situation occurs for the Prajna daemon to write cluster metadata, DKV metadata, 
    /// Assembly, dependencies file, etc.. WriteBytesToFileConcurrent checks if the file has been written concurrently, 
    /// if that is the case, one of the process will verify (read) the file and make sure that is the same content 
    /// that is to be written. 

    let WriteBytesToFileConcurrentCreate (filename : string) (bytes:byte[]) =
        let dirpath = Path.GetDirectoryName( filename )
        // Create directory if necessary
        DirectoryInfoCreateIfNotExists (dirpath) |> ignore
        WriteBytesToFileConcurrent filename bytes
    
    /// read a string from file
    let ReadFromFile (filename:string) =
        use file = new StreamReader(filename)
        let ret = file.ReadToEnd()
        ret
    
    /// save a string to file, do not write if we find that the file is not writable.
    let SaveToFile (filename:string) (content:string) = 
        let bytes = System.Text.UTF8Encoding().GetBytes( content )
        WriteBytesToFileConcurrentPCompare filename bytes 0 bytes.Length false
