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
        partition.fs
  
    Description: 
        Define Partition, the partition function used in Prajna. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013 (First)
        Mar. 2014 (Revised) 
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Generic
open System.Runtime.Serialization
open Microsoft.FSharp.Core

type internal PartitionerKind = 
    | Standard = 0
    | HasCustomInitialization = 32
    | HasCustomPartitioningFunc = 64   

[<Serializable>]
type internal Partitioner( typeOf:PartitionerKind ref , bPartByKey: bool ref, nump:int ref )  = 
    member val internal typeOfInternal = typeOf with get, set
    member val internal  partitionByKeyInternal = bPartByKey with get, set
    member val numPartitionsInternal = nump with get, set
    member x.TypeOf with get() = !x.typeOfInternal
                     and set(t) = x.typeOfInternal <- ref t
    member x.bPartitionByKey with get() = !x.partitionByKeyInternal 
                              and set(k) = x.partitionByKeyInternal <- ref k
    member x.NumPartitions with get() = !x.numPartitionsInternal
                            and set( n ) = x.numPartitionsInternal <- ref n
    new () = 
        Partitioner( ref PartitionerKind.Standard, ref false, ref -1 )
    new ( part: Partitioner, nump: int ) = 
        Partitioner( part.typeOfInternal, part.partitionByKeyInternal, ref nump )
    new ( part: Partitioner, bPartByKey: bool ) = 
        Partitioner( part.typeOfInternal, ref bPartByKey, part.numPartitionsInternal )
