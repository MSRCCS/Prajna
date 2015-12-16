(*---------------------------------------------------------------------------
    Copyright 2015 Microsoft

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
        builtinfunction.fs
  
    Description: 
        A set of build in distributed functions. 

    Author:																	
        Jin Li, Partner Researcher Manager
        Microsoft 
        Email: jinl at microsoft dot com
    Date:
        Oct. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service

open System
open System.IO
open System.Net
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Runtime.Serialization.Json
open Prajna.Tools
open Prajna.Tools.Network
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core

/// A set of built in distributed functions. 
type internal DistributedFunctionBuiltIn() = 
    static member val internal GetContainerFunctionName = "GetContainer" with get
    /// Retrieve information of the local App/container, in the form of 
    /// machine name, container name 
    static member internal GetContainerLocal( ) = 
        RemoteExecutionEnvironment.MachineName, RemoteExecutionEnvironment.ContainerName

 /// Initialization of the Distributed function execution environment. 
type internal DistributedFunctionEnvironment() = 
    static let init = lazy (
        let builtInProvider = DistributedFunctionBuiltInProvider()
        DistributedFunctionStore.Current.RegisterProvider( builtInProvider )
        DistributedFunctionStore.Current.RegisterFunction<_>( DistributedFunctionBuiltIn.GetContainerFunctionName, DistributedFunctionBuiltIn.GetContainerLocal ) |> ignore
        DistributedFunctionStore.Current.NullifyProvider( )
    )
    static member Init () =
        init.Force()
