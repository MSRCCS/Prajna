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


/// A store that holds distributed value
type internal DistributedValueStore () = 
    static member val Current = DistributedValueStore () with get
    member val Store = ConcurrentDictionary<Guid, Object>() with get

type internal DistributedValueStoreAPI() = 
    static member val StoreValueFuncName = "StoreValue" with get
    static member val LazySetValue = lazy ( fun (id: Guid, o: Object ) -> ( null : Object ) ) with get, set
    /// Remote function to be actually executed. 
    /// Store an object into a slot of objectID
    /// Note that this is a function: (return a null object) 
    /// This make sure that the value is actually stored at the return of the function. 
    static member StoreValueLocal( objectID: Guid, v: Object ) : Object = 
        DistributedValueStore.Current.Store.Item( objectID ) <- v
        null 
    /// Remote function to be actually executed. 
    /// Store an object into a slot of objectID
    static member SetValue( objectID: Guid, v: Object ) = 
        /// The function will only return when the set value succeeds. 
        DistributedValueStoreAPI.LazySetValue.Value( objectID, v ) |> ignore 
    /// Get value 
    static member GetValue( objectID: Guid ) = 
        let bExist, obj = DistributedValueStore.Current.Store.TryGetValue( objectID )    
        if bExist then 
            obj
        else
            null
/// A Distributed Value which can be used to
/// hold object in a cluster to be used during the data analytic routine
/// Distributed Value should only be set in the App, and get from remote. 
type DistributedValue<'T> internal ( objectID: Guid) = 
    member internal x.ObjectID with get() = objectID 
    /// Set a distributed value
    member x.Value with set( v: 'T ) = DistributedValueStoreAPI.SetValue( x.ObjectID, v )
                    and get() = 
                            let ret = DistributedValueStoreAPI.GetValue( x.ObjectID ) 
                            if Utils.IsNull ret then Unchecked.defaultof<_> else ret :?> 'T
        


/// A set of built in distributed functions. 
type DistributedFunctionBuiltIn() = 
    static let pool = SystemThreadPool()

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
        ThreadTracking.ContainerName <- RemoteExecutionEnvironment.ContainerName
        match env with 
        | ContainerEnvironment -> 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register Built-In function API for container %s" RemoteExecutionEnvironment.ContainerName )
            // Use Prajna Serializer for the following functions. 
            DistributedFunctionStore.UseSerializerTag( DefaultSerializerForDistributedFunction.PrajnaSerializer )
            builtInStore.RegisterFunction<_,_>( DistributedValueStoreAPI.StoreValueFuncName, DistributedValueStoreAPI.StoreValueLocal ) |> ignore
            DistributedFunctionStore.PopSerializerTag()
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
            DistributedValueStoreAPI.LazySetValue <- lazy ( DistributedFunctionStore.Current.TryImportFunction<Guid*Object, Object>( DistributedValueStoreAPI.StoreValueFuncName ) )
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
    /// Trigger an exception in remote function, for testing purpose only 
    /// The performance is measured using the particular function of GetConnectedContainers()
    static member GetBuiltInFunctionPerformance() = 
        let perf = builtInStore.GetPerformanceFunction<string*string>( DistributedFunctionBuiltIn.GetContainerFunctionName )
        perf
    static member internal Init () =
        initialized.Force()
