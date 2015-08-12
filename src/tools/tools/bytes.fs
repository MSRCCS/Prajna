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
        Bytes.fs
  
    Description: 
        Helper functions for byte[] operations

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013
    
 ---------------------------------------------------------------------------*)

namespace Prajna.Tools

open System
open System.Security.Cryptography
open System.IO
open System.Security.AccessControl
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Runtime.Serialization

/// <summary>
/// Compare byte[], note that the default comparison of byte[] is Reference Equality 
/// </summary>
type BytesCompare = 
    /// Construct a comparer for byte[] that compares their content. 
    new () = {}
    interface IEqualityComparer<byte[]> with 
        override this.Equals (x, y) = 
            if Utils.IsNull x then 
                Utils.IsNull y
            else
                if Utils.IsNull y then 
                    false
                else
                    System.Linq.Enumerable.SequenceEqual( x, y)
         override this.GetHashCode (x) = 
            if Utils.IsNull x then 
                0
            else
                let mutable hash = 17
                for v in x do 
                    hash <- ( hash * 31 ) ^^^ ( int v)
                hash
                
/// <summary>
/// A set of helper routine for byte[] operations
/// </summary>
module  BytesTools =

    /// Read an entire stream to byte[]
    let ReadToEnd (stream:Stream ) = 
        let InitialLength = 1024 * 1024
        let mutable buf = Array.zeroCreate<byte> InitialLength
        let mutable bContinueRead = true
        let mutable pos = 0 
        while bContinueRead do
            let readLen = stream.Read( buf, pos, buf.Length - pos ) 
            if readLen = 0 then 
                bContinueRead <- false
            else
                pos <- pos + readLen 
                // If we read the whole buffer, we need to extend the current byte array
                if pos = buf.Length then 
                    let newBuffer = Array.zeroCreate<byte> (buf.Length*2)
                    Buffer.BlockCopy( buf, 0, newBuffer, 0, buf.Length ) 
                    buf <- newBuffer
        let resultBuffer = Array.zeroCreate<byte> pos
        Buffer.BlockCopy( buf, 0, resultBuffer, 0, pos ) 
        resultBuffer
    
    let internal Serialize obj (hintLen:int) =
        use ms = new MemoryStream( hintLen ) 
        let fmt = Formatters.Binary.BinaryFormatter()
        fmt.Serialize( ms, obj )
        let res = Array.zeroCreate<byte> (int ms.Length)
        Buffer.BlockCopy( ms.GetBuffer(), 0, res, 0, int ms.Length )
        res

    let internal Deserialize (buffer:byte[]) =
        use ms = new MemoryStream( buffer )
        let fmt = Formatters.Binary.BinaryFormatter()
        fmt.Deserialize( ms )

    /// Compute Hash of the bytearray
    let HashByteArray( data: byte[] ) = 
        let sha256managed = new SHA256Managed()
        let result = sha256managed.ComputeHash( data )
        result

    /// <summary>
    /// Calculate a hash that matches the file hash in PrajnaRemote execution roster. The calculdated hash include 
    /// length of byte[] plus the content of the byte[].
    /// </summary>
    let inline HashLengthPlusByteArray( data: byte[] ) = 
        let sha256managed = new SHA256Managed()
        let len = if Utils.IsNull data then 0 else data.Length
        let lenarr = BitConverter.GetBytes( len ) 
        sha256managed.TransformBlock( lenarr, 0, lenarr.Length, lenarr, 0 ) |> ignore
        if len > 0 then 
            sha256managed.TransformBlock( data, 0, len, data, 0 ) |> ignore
        sha256managed.TransformFinalBlock( [||], 0, 0 ) |> ignore
        sha256managed.Hash

    /// Compute Hash of the bytearray, and use first 16B of hash to form a GUID
    let inline HashByteArrayToGuid( data: byte[] ) = 
        let resultLong = HashByteArray( data )
        let result = Array.sub resultLong 0 16
        System.Guid( result )

    let internal HashByteArrayWithLength( data: byte[], offset, count ) = 
        let sha256managed = new SHA256Managed()
        let result = sha256managed.ComputeHash( data, offset, count )
        result

    let inline internal HashByteArrayToGuidWithLength( data: byte[], offset, count ) = 
        let resultLong = HashByteArrayWithLength( data, offset, count ) 
        let result = Array.sub resultLong 0 16
        System.Guid( result )

    /// <summary>
    /// Hash input data via Sha512, truncate first 32B as the hash signature. 
    /// This is the algorithm used in Primary Data deduplication in Windows Server 2012 R2. 
    /// </summary>
    let inline internal HashByteArraySha512( data: byte[], offset, count ) = 
        use sha = new SHA512Managed()
        let fullResult = sha.ComputeHash( data, offset, count )
        let result = Array.zeroCreate<byte> 32
        Buffer.BlockCopy( fullResult, 0, result, 0, 32 )
        result
        
    /// <summary>
    /// Show byte[] in hexidecimal format. 
    /// </summary>  
    let BytesToHex bytes = 
        if Utils.IsNotNull bytes then 
            bytes 
            |> Array.map (fun (x : byte) -> System.String.Format("{0:X2}", x))
            |> String.concat System.String.Empty
        else
            "<null>"
