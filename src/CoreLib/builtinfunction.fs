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

module DistributedFunctionBuiltInInitializationModule = 
    let lazyUnitialized<'TResult> () = 
        lazy ( fun _ -> 
                failwith "This prajna function requires Environment.Init() to be called at the beginning of the program." 
                Unchecked.defaultof<'TResult>
            )
    let lazySeqUnitialized<'TResult> () = 
        lazy ( fun _ -> 
                failwith "This prajna function requires Environment.Init() to be called at the beginning of the program." 
                Seq.empty : seq<'TResult>
            )
/// A set of built in distributed functions. 
type DistributedFunctionBuiltIn() = 
    static let initialized = lazy ( DistributedFunctionBuiltIn.InitOnce() )
    static let builtInStore = DistributedFunctionStore.Current
            
    static let mutable getConnectedContainerLazy = DistributedFunctionBuiltInInitializationModule.lazySeqUnitialized<string*string>()
    static let mutable triggerRemoteExceptionLazy = DistributedFunctionBuiltInInitializationModule.lazySeqUnitialized<string>()
    static member val internal GetContainerFunctionName = "GetContainer" with get
    static member val internal TriggerRemoteExceptionFunctionName = "TriggerException" with get 
    /// Retrieve information of the local App/container, in the form of 
    /// machine name, container name 
    static member private GetContainerLocal( ) = 
        RemoteExecutionEnvironment.MachineName, RemoteExecutionEnvironment.ContainerName
    static member val RemoteExceptionString = "Preconfigured remote exception, for testing purpose only" with get 
    /// Remote Exception 
    static member private RemoteExceptionLocal() = 
        let ex = System.Exception( DistributedFunctionBuiltIn.RemoteExceptionString )
        raise( ex )
        ( null: string )
    static member private InitOnce() = 
        let builtInProvider = DistributedFunctionBuiltInProvider()
        builtInStore.RegisterProvider( builtInProvider )
        let env = RemoteExecutionEnvironment.GetExecutionEnvironment() 
        match env with 
        | ContainerEnvironment -> 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register Built-In function API for container %s" RemoteExecutionEnvironment.ContainerName )
            builtInStore.RegisterFunction<_>( DistributedFunctionBuiltIn.GetContainerFunctionName, DistributedFunctionBuiltIn.GetContainerLocal ) |> ignore
            builtInStore.RegisterFunction<_>( DistributedFunctionBuiltIn.TriggerRemoteExceptionFunctionName, DistributedFunctionBuiltIn.RemoteExceptionLocal ) |> ignore
            // ToDo: Register other functions of container 
            NetworkCommandQueue.AddSystemwideRecvProcessor( "DistributedFunctionParser@Container", NetworkCommandQueueType.AnyDirection, DistributedFunctionStore.ParseDistributedFunction )
            NetworkCommandQueue.AddSystemwideDisconnectProcessor( "DistributedFunctionDisconnectProcessor@Container", NetworkCommandQueueType.AnyDirection, DistributedFunctionStore.DisconnectProcessor )
            let serverInfo = ContractServersInfo()
            serverInfo.AddDaemon()
            builtInStore.ExportTo( serverInfo )
        | DaemonEnvironment -> 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register Built-In function API for daemon %s" RemoteExecutionEnvironment.ContainerName )
            NetworkCommandQueue.AddSystemwideRecvProcessor( "DistributedFunctionParser@Daemon", NetworkCommandQueueType.AnyDirection, DistributedFunctionStore.ParseDistributedFunctionCrossBar )
        | ClientEnvironment ->
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register Built-In function API for client %s" RemoteExecutionEnvironment.ContainerName )
            getConnectedContainerLazy <-  lazy( DistributedFunctionStore.Current.TryImportSequenceFunction<string*string>( DistributedFunctionBuiltIn.GetContainerFunctionName ) )
            triggerRemoteExceptionLazy <- lazy( DistributedFunctionStore.Current.TryImportSequenceFunction<string>( DistributedFunctionBuiltIn.TriggerRemoteExceptionFunctionName ) )
            // ToDo: Register other functions of container 
            NetworkCommandQueue.AddSystemwideRecvProcessor( "DistributedFunctionParser@Client", NetworkCommandQueueType.AnyDirection, DistributedFunctionStore.ParseDistributedFunction )
            NetworkCommandQueue.AddSystemwideDisconnectProcessor( "DistributedFunctionDisconnectProcessor@Client", NetworkCommandQueueType.AnyDirection, DistributedFunctionStore.DisconnectProcessor )
    /// Get information of containers that is connected with the current clients. 
    static member GetConnectedContainers() = 
        let func = getConnectedContainerLazy.Value
        func()
    /// Trigger an exception in remote function, for testing purpose only 
    static member TriggerRemoteException() = 
        let func = triggerRemoteExceptionLazy.Value
        func()

    static member internal Init () =
        initialized.Force()
