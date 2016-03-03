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
        RemoteRunner.fs
  
    Description: 
        RemoteRunner uses Distributed function to execute a closure (Action<_>, Func<_,_> or FuncTask<_,_>)
    at one or all of remote node. The main difference between DistributedFunction and RemoteRunner is that the
    RemoteRunner will setup the remote execution environment, and run the closure at remote. While DistributedFunction
    only execute pre-installed code at remote (so that the code is executed more like a service) 

    Author:																	
        Jin Li, Partner Researcher Manager
        Microsoft 
        Email: jinl at microsoft dot com
    Date:
        Feb. 2016
 ---------------------------------------------------------------------------*)
namespace Prajna.Service

open System
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Core
open Prajna.Service



/// The engine that executes the closure
type internal RemoteRunnerEngine() = 
    static let daemonInitialized = lazy( RemoteRunnerEngine.RegisterAtDaemon() )
    static member val RunActionName = "RunAction" with get, set
    static member val RunFunctionName = "RunFunction" with get, set
    static member RunAction( runner : Object -> unit, o: Object ) = 
        runner( o )
    static member RunFunction( runner : Object -> Object, o: Object ) = 
        runner( o ) 
    static member RegisterAtDaemon() = 
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register RemoteRunner API for container %s" RemoteExecutionEnvironment.ContainerName )
        // Use Prajna Serializer for the following functions. 
        DistributedFunctionStore.UseSerializerTag( DefaultSerializerForDistributedFunction.PrajnaSerializer )
        let reg1 = DistributedFunctionStore.Current.RegisterAction<(Object->unit)*Object>( RemoteRunnerEngine.RunActionName, RemoteRunnerEngine.RunAction ) 
        let reg2 = DistributedFunctionStore.Current.RegisterFunction<(Object->Object)*Object, Object>( RemoteRunnerEngine.RunFunctionName, RemoteRunnerEngine.RunFunction ) 
        DistributedFunctionStore.PopSerializerTag()
    static member InitAtDaemon() = 
        daemonInitialized.Value |> ignore         

/// Remote Runner, the closure will be run on one or more remote nodes. 
type RemoteRunner(serverInfo: ContractServerInfoLocal ) = 
    let runActionOnOneLazy = lazy( DistributedFunctionStore.Current.TryImportAction<(Object->unit)*Object>( serverInfo, RemoteRunnerEngine.RunActionName ) )
    let runFunctionOnOneLazy = lazy( DistributedFunctionStore.Current.TryImportFunction<(Object->Object)*Object, Object>( serverInfo, RemoteRunnerEngine.RunFunctionName ) )
    new( cl: Cluster ) = 
        // Execute a light job, make sure that the cluster is properly setup
        RemoteRunner.InitClusterFunc( cl ) 
        let serverInfo = ContractServerInfoLocal.ConstructFromCluster(cl)
        RemoteRunner( serverInfo )
    static member val internal InitClusterFunc = fun ( cl: Cluster ) -> failwith "Environment.Init() needs to be called before RemoteRunner is used. "
                                                                        () with get, set
    /// Run action on one remote node. 
    member x.RunActionOnOne<'T>( runner: 'T -> unit, param: 'T ) = 
        let wrappedRunner( o: Object ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T
            runner( runObject )
        runActionOnOneLazy.Value( wrappedRunner, box( param ) )
    member x.RunUnitActionOnOne( runner: unit -> unit ) = 
        let wrappedRunner( o: Object ) = 
            runner( )
        runActionOnOneLazy.Value( wrappedRunner, null )
    /// Run action on one remote node. 
    member x.RunFunctionOnOne<'T, 'TResult>( runner: 'T -> 'TResult, param: 'T ) = 
        let wrappedRunner( o: Object ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T
            box ( runner( runObject ) )
        let ret = runFunctionOnOneLazy.Value( wrappedRunner, box( param ) )
        if Utils.IsNull ret then Unchecked.defaultof<'TResult> else ret :?> 'TResult
    member x.RunFunctionOnOne<'TResult>( runner: unit -> 'TResult ) = 
        let wrappedRunner( o: Object ) = 
            box ( runner( ) )
        let ret = runFunctionOnOneLazy.Value( wrappedRunner, null )
        if Utils.IsNull ret then Unchecked.defaultof<'TResult> else ret :?> 'TResult
    

