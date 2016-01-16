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
        blockingqueue.fs
  
    Description: 
        Helper function for blocking queue

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2016
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools.Queue

open System
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open Prajna.Tools
open Prajna.Tools.FSharp

/// The base class for a concurrent queue structures with and without flow control logic and 
/// with support of cancellation. 
[<AbstractClass>]
type BlockingQueue<'T>( capacity: int, token: CancellationToken ) =
    /// Enqueue an object
    abstract Enqueue: 'T -> unit 
    /// Dequeue an object
    abstract Dequeue: unit -> 'T
