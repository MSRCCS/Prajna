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
        command.fs
  
    Description: 
        Define NetworkCommand

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System

[<Serializable>]
/// Verb to be used for message passing between Prajna nodes
/// Only advance programmer that need to write additional Prajna service should use this data structure
type ControllerVerb = 
    /// Get 
    | Get = 0 
    /// Set
    | Set = 1
    /// Store
    | Store = 2
    /// List
    | List = 3
    /// Echo
    | Echo = 4
    /// EchoReturn
    | EchoReturn = 5
    /// Echo2
    | Echo2 = 6
    /// Echo2Return
    | Echo2Return = 7
    /// Read
    | Read = 10
    /// Write
    | Write = 11
    /// Close
    | Close = 12
    /// WriteAndReplicate
    | WriteAndReplicate = 13
    /// ReplicateWrite
    | ReplicateWrite = 14
    /// ReplicateClose
    | ReplicateClose = 15
    /// Use
    | Use = 16
    /// Availability
    | Availability = 17
    /// Start
    | Start = 18
    /// ConfirmStart
    | ConfirmStart = 19
    /// Link
    | Link = 20 
    /// Ready
    | Ready = 21
    /// Forward
    | Forward = 22
    /// Fold
    | Fold = 23
    /// WriteGV
    | WriteGV = 24
    /// Open
    | Open = 25
    /// Report
    | Report = 26
    /// Update
    | Update = 27
    /// InfoNode
    | InfoNode = 28
    /// ClosePartition
    | ClosePartition = 29
    /// ConfirmClose
    | ConfirmClose = 30
    /// ConfirmClosePartition
    | ConfirmClosePartition = 31
    /// ReportPartition
    | ReportPartition = 32
    /// ReportClose
    | ReportClose = 33
    /// UpdateParam
    | UpdateParam = 34
    /// WriteMetadata
    | WriteMetadata = 35
    /// ReadToNetwork
    | ReadToNetwork = 36
    /// SyncWrite
    | SyncWrite = 37
    /// SyncClosePartition
    | SyncClosePartition = 38
    /// SyncClose
    | SyncClose = 39
    /// Delete
    | Delete = 40
    /// Request
    | Request = 41
    /// Reply
    | Reply = 42
    /// Stop
    | Stop = 43
    /// ConfirmStop
    | ConfirmStop = 44
    /// TimeOut
    | TimeOut = 45
    /// Register
    | Register = 46
    /// FailedReply
    | FailedReply = 47      // To reply provider: error 
    /// FailedRequest
    | FailedRequest = 48    // To requestor: error
    /// Information of the connecting container
    | ContainerInfo = 49
    /// LimitSpeed
    | LimitSpeed = 64        // network speed
    /// Decrypt
    | Decrypt = 65
    /// NonExist
    | NonExist = 247
    /// Verbose
    | Verbose = 248         // Carry information that is not that important. 
    /// Duplicate
    | Duplicate = 249
    /// Info
    | Info = 250
    /// Nothing
    | Nothing = 251
    /// Warning
    | Warning = 252
    /// Acnowledge
    | Acknowledge = 253
    /// Error
    | Error = 254
    /// Unknown
    | Unknown = 255         // They serve as wild card for callback matching


[<Serializable>]
/// Noun to be used for message passing between Prajna nodes
/// Only advance programmer that need to write additional Prajna service should use this data structure
type ControllerNoun = 
    /// ClusterInfo
    | ClusterInfo = 0 
    /// KeyValue
    | KeyValue = 1
    /// Message
    | Message = 2
    /// Assembly
    | Assembly = 3
    /// DSet
    | DSet = 4
    /// Metadata
    | Metadata = 5 
    /// Partition
    | Partition = 6
    /// Job
    | Job = 7
    /// Blob
    | Blob = 8
    /// GV
    | GV = 9
    /// Program
    | Program = 10
    /// DStream
    | DStream = 11
    /// Dependency
    | Dependency = 12
    /// Service
    | Service = 13
    /// Connection
    | Connection = 14
    /// Buffer
    | Buffer = 15
    /// QueryReply
    | QueryReply = 16
    /// Contract
    | Contract = 17
    /// ClientInfo
    | ClientInfo = 20
    /// All
    | All = 254
    /// Unknown
    | Unknown = 255         // They serve as wild card for callback matching

[<Serializable>]
/// Verb-Noun pair that forms one Prajna message. 
/// Only advance programmer that need to write additional Prajna service should use this data structure
type ControllerCommand = 
    struct
        /// Verb in the Prajna message
        val Verb : ControllerVerb
        /// Noun in the Prajna message
        val Noun : ControllerNoun
        /// Construct a Command word with a verb and a noun pair
        new (v, n ) = { Verb = v; Noun = n }
        override x.ToString() = x.Verb.ToString() + "," + x.Noun.ToString()
    end

