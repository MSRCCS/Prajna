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
		custommemory.fs
  
	Description: 
		Custom Memory Management

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
        Sanjeev Mehrotra, Principal Software Architect

    Date:
        Aug. 2015	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Collections.Concurrent

/// <summary>
/// Prajna allows user to implement customized memory manager. The corresponding class 'Type implement a 
///     allocFunc: () -> 'Type to grab an object of 'Type (from a pool of preallocated object). 
///     resetFunc: int -> () to return an object to the pool. 
/// Jin Li: This class is to be internalized, with a corresponding external implementation at JobDependencies. 
/// </summary> 
type internal CustomizedMemoryManager() = 
    static member val internal MemoryManagerCollectionByName = ConcurrentDictionary<string, _>() with get
    /// <summary>
    /// Install a customized Memory Manager, in raw format of storage and no checking
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="fullname"> Type name of the object. </param>
    /// <param name="wrappedEncodeFunc"> Customized Serialization function that encodes an Object to a bytestream.  </param>
    static member InstallMemoryManager( fullname, allocFunc: unit -> Object, preallocFunc: int -> unit ) = 
        CustomizedMemoryManager.MemoryManagerCollectionByName.Item( fullname ) <- ( allocFunc, preallocFunc )
    static member GetAllocFunc<'Type when 'Type :> IDisposable >() = 
        let bExist, tuple = CustomizedMemoryManager.MemoryManagerCollectionByName.TryGetValue( typeof<'Type>.FullName )
        if bExist then 
            let allocFunc, _ = tuple 
            let wrappedAllocFunc() =
                allocFunc() :?> 'Type
            wrappedAllocFunc
        else 
            failwith (sprintf "Can't find customized memory manager for type %s" typeof<'Type>.FullName )
    static member GetPreallocFunc<'Type when 'Type :> IDisposable >() =
        let bExist, tuple = CustomizedMemoryManager.MemoryManagerCollectionByName.TryGetValue( typeof<'Type>.FullName )
        if bExist then 
            let _, preallocFunc = tuple 
            preallocFunc
        else 
            failwith (sprintf "Can't find customized memory manager for type %s" typeof<'Type>.FullName )

