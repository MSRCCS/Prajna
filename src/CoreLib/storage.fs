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
        storage.fs
  
    Description: 
        Storage interface of Prajna. 
        Depending on the type of the storage, it will contains a storage interface for 
        local HDD, local SSD and Azure Storage. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System.IO

/// Type of Storage used by DSet
type StorageKind = 
    /// Should not be used 
    | None = 0
    /// Hard drive 
    | HDD = 0
    /// SSD 
    | SSD = 1
    /// In-memory, instantiated object. 
    | RAM = 2
    /// Azure storage 
    | Azure = 3
    /// no storage, the DSet content is only instantiated during start up 
    | Passthrough = 4               // Passthrough is a special type, in which the DSet doesn't retain information in memory, just applied functions. 
    /// Mask to retrieve storage type 
    | StorageMediumMask = 0x3f      // StorageMedia

/// PrajnaCacheTye only kicks in for StorageType.RAM
/// It is only used for in job DSet
type CacheKind = 
    /// No cached access
    | None = 0x0000                     
    /// The cached data can be retrieved via a Enumerable Operator
    | EnumerableRetrieve = 0x0001       
    /// The cached data can be accessed via index
    | IndexByKey  = 0x0002              
    /// The cached data can be accessed via a Concurrent Dictionary
    | ConcurrectDictionary = 0x0004     
    /// The cached data is sorted 
    | SortedRetrieve = 0x0008       
    /// The data is sampled, with past sampling recorded. 
    | SampledWithReplacement = 0x0010   
    /// One Cache spans all partition, if UnifiedCache is On, only one partition cache will be created for the entire DSet
    | UnifiedCache = 0x0020             
    /// EnumerableRetrieve, no need for KV Cache
    | ConstructEnumerableCacheMask = 0x0001     
    /// IndexByKey | ConcurrectDictionary | SortedRetrieve all need KV Cache
    | ConstructKVCacheMask = 0x0006             

type internal StorageStreamBuilder = 
    static member Create( typeOf ) =  
        let fStream = 
            let ty = typeOf &&& StorageKind.StorageMediumMask
            match typeOf with
            | StorageKind.HDD 
            | _ ->
                new HDDStream(  ) :> StorageStream
#if LINK_TO_AZURE
            | StorageType.Azure ->
                new AzureBlobStream() :> StorageStream
#endif
//            | _ ->
//                failwith (sprintf "Undefined StorageStreamBuilder Type %A" typeOf)
        fStream
