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
        service.fs
  
    Description: 
        Service

    Message Used:
        Start, Service:         Launch a service
        ConfirmStart, Service:  Return status of the launch service
        Stop, Service:          Stop a service
        ConfirmStop, Service:   Return status of the stop service
    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Dec. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service

open System
open System.IO
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Api.FSharp

open Prajna.Core

/// <summary>
/// WorkerRoleInstance can be extended to run a Prajna worker service. Unlike WorkerRoleEntryPoint, WorkerRoleInstance is to be instantiated at the remote end 
/// so it is class member is not serialized. 
/// </summary>
[<AbstractClass; AllowNullLiteral>]
type WorkerRoleInstance<'StartParamType>() = 
    inherit WorkerRoleInstance()
    override x.OnStartByObject (o:Object) = 
        try
            if Utils.IsNull o then 
                x.OnStart (Unchecked.defaultof<_>)
            else
                match o with 
                | :? 'StartParamType as param -> 
                    x.OnStart param
                | _ -> 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "WorkerRoleInstance start parameter %A is not of type %s, OnStartByObject failed" 
                                                                       (o)
                                                                       (typeof<'StartParamType>.FullName) ))
                    false
            with 
            | e -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "WorkerRoleInstance OnStartByObject failed with exception %A" e ))
                false
    /// bool OnStart(): Run once when the service started for all thread, it will be runned in each thread. 
    abstract OnStart: 'StartParamType -> bool

/// <summary>
/// WorkerRoleInstance can be extended to run a Prajna worker service. Unlike WorkerRoleEntryPoint, WorkerRoleInstance is to be instantiated at the remote end 
/// so it is class member is not serialized. 
/// </summary>
type internal WorkerRoleInstanceGeneric<'StartParamType> = WorkerRoleInstance<'StartParamType>

/// <summary>
/// WorkerRoleEntryPoint can be extended to run a Prajna worker service. The worker service will extend the class WorkerRoleEntryPoint with the following functions:
/// Please note that the extended class will be serialized to the remote end. It is crucial to consider the data footprint in serialization for efficient execution. 
/// </summary>
[<AbstractClass; Serializable>]
type internal WorkerRoleEntryPoint<'StartParamType>() = 
    inherit WorkerRoleEntryPoint()
    /// bool OnStart(): Run once when the service started
    abstract OnStart: 'StartParamType -> bool
    override x.OnStartByObject (o:Object) = 
            try
                if Utils.IsNull o then 
                    x.OnStart (Unchecked.defaultof<_>)
                else
                    match o with 
                    | :? 'StartParamType as param -> 
                        x.OnStart param
                    | _ -> 
                        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "WorkerRoleEntryPoint start parameter %A is not of type %s, OnStartByObject failed" 
                                                                   (o)
                                                                   (typeof<'StartParamType>.FullName) ))
                        false
            with 
            | e -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "WorkerRoleEntryPoint OnStartByObject failed with exception %A" e ))
                false

/// <summary>
/// WorkerRoleEntryPoint can be extended to run a Prajna worker service. The worker service will extend the class WorkerRoleEntryPoint with the following functions:
/// Please note that the extended class will be serialized to the remote end. It is crucial to consider the data footprint in serialization for efficient execution. 
/// </summary>
// [<AbstractClass; Serializable>]
type WorkerRoleEntryPointUnit = WorkerRoleEntryPoint<unit>

/// <summary>
/// This class contains the parameter used to start a multithreaded WorkerRoleInstance. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type WorkerRoleInstanceStartParam() =
    /// <summary> 
    /// Number of threads of that runs WorkerRoleInstance. 
    /// </summary>
    member val NumThreads = 1 with get, set
    /// <summary> 
    /// Timeout at Thread close (in millisecond)
    /// </summary>
    member val ThreadCloseTimeoutInMS = 10000 with get, set
    /// <summary>
    /// User will need to override this function to instantiate the right front end instance to be used for service. 
    /// </summary>
    member val NewInstanceFunc : unit -> WorkerRoleInstance = 
        ( fun _ -> Logger.LogF( LogLevel.Error,  fun _ -> sprintf "Please extend NewInstanceFunc. "  )
                   null ) with get, set

/// <summary>
/// The code abstract running WorkerRoleInstance. The Prajna machine at remote will instantiate one WorkerRoleInstance. For the instance. 
/// 1. OnStart will be called once, to start the instance. 
/// 2. Run() will be called N times, depending on NumThreads that runs the instance. 
/// 3. OnStop will be called once, if the instance needed to be shutdown. 
/// </summary> 
type internal WorkerRoleInstanceService<'StartParamType when 'StartParamType :> WorkerRoleInstanceStartParam >() =
    inherit WorkerRoleEntryPoint<'StartParamType>()
    [<field:NonSerialized>]
    let mutable serviceInternal = null
    [<field:NonSerialized>]
    let mutable numThreads = 0
    [<field:NonSerialized>]
    let mutable threadCloseTimeoutInMS = 0
    [<field:NonSerialized>]
    let mutable threads = null
    [<field:NonSerialized>]
    member val bTerminate = false with get, set
    override x.OnStart (param) = 
        serviceInternal <- param.NewInstanceFunc()
        x.bTerminate <- Utils.IsNull serviceInternal
        if not x.bTerminate then 
            x.bTerminate <- not (serviceInternal.OnStartByObject( param ))
            numThreads <- param.NumThreads
            threadCloseTimeoutInMS <- param.ThreadCloseTimeoutInMS
        not x.bTerminate
    override x.Run() = 
        try
            if not (Utils.IsNull serviceInternal) then 
                if numThreads > 1 then 
                    threads <- Array.zeroCreate<_> ( numThreads - 1 )
                    for i = 1 to numThreads - 1 do
                        threads.[i-1] <- ThreadTracking.StartThreadForFunctionWithCancelation (serviceInternal.OnStop) ( fun _ -> sprintf "Thread %d for WorkerRoleInstance" i) (serviceInternal.Run)
                    /// Thread 0 is running on the Service thread. 
                serviceInternal.Run()
                while not x.bTerminate && serviceInternal.IsRunning() do 
                    x.bTerminate <- serviceInternal.EvTerminated.WaitOne( WorkerRoleInstance.CheckRoleInstanceAliveInMs )    
                x.bTerminate <- true
        with
        | e -> 
            serviceInternal <- null
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!! Unexpected exception !!! Message: %s, %A" e.Message e ))
            x.bTerminate <- true            
    override x.OnStop() = 
        // Cancel all pending jobs. 
        x.bTerminate <- true
        if Utils.IsNotNull serviceInternal then 
            serviceInternal.OnStop()
            serviceInternal.EvTerminated.Set() |> ignore
            /// Wait for service to stop, up to threadCloseTimeoutInMS
            if numThreads > 1 && not (Utils.IsNull threads ) then 
                let t1 = (PerfDateTime.UtcNow())
                for i = 1 to numThreads - 1 do
                    let t2 = (PerfDateTime.UtcNow()).Subtract(t1).TotalMilliseconds
                    let maxWait = Math.Min( 1, threadCloseTimeoutInMS - int t2 )
                    threads.[i-1].Join(maxWait) |> ignore
    override x.IsRunning() = 
        not x.bTerminate


[<AllowNullLiteral>]
type internal DSetStartServiceAction<'StartParamType>(cl:Cluster, serviceName:string, serviceClass:WorkerRoleEntryPoint, param:'StartParamType)=
    inherit DSetAction()
    member x.StartService( ) = 
        // Bind DSet to the service, the DSet version is always the same, 
        let useCluster = if not (Utils.IsNull cl) then cl else Cluster.GetCurrent()
        if (Utils.IsNull useCluster ) then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Need to specify cluster to start service " ))
        else
            // Bind a DSet name, and allow same service name to be launched on different cluster
            let serviceDSet = DSet<unit>( Name = serviceName, 
                                         Cluster = useCluster )
            serviceDSet.NumPartitions <- useCluster.NumNodes
            serviceDSet.NumReplications <- 1
            serviceDSet.Mapping <- Array.init useCluster.NumNodes ( fun i -> Array.create 1 i )
            serviceDSet.Dependency <- Source
    // Trigger serviceDSet to be a passthrough DSet
    //        serviceDSet.StorageType <- StorageType.Passthrough
    //        serviceDSet.Function <- Function()
            x.Param <- serviceDSet

            if x.ParameterList.Count<>1 then 
                let msg = sprintf "DSetStartServiceAction should take a single DSet parameter, while %d parameters are given" x.ParameterList.Count
                Logger.Log( LogLevel.Error, msg )
                failwith msg
            x.FurtherDSetCallback <- x.LaunchServiceCallback
        
            x.BeginAction()
            let useDSet = x.ParameterList.[0]
            if x.Job.ReadyStatus && useDSet.NumPartitions>0 then 
                x.GetJobInstance(useDSet).RemappingCommandCallback <- x.RemappingCommandToLaunchService
                // Send out the fold command. 
                x.RemappingDSet() |> ignore   
                while not (x.Timeout()) && not (x.GetJobInstance(useDSet).AllDSetsRead()) do
                    x.RemappingDSet() |> ignore
                    // Wait for result to come out. 
                    Thread.Sleep( 3 )
                if x.Timeout() then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Timeout for DSetFoldAction ............." ))
                x.OrderlyEndAction()
            else
                x.CloseAndUnregister()
                let status, msg = x.Job.JobStatus()
                if status then
                    Logger.Log( LogLevel.Info, msg )
                else
                    Logger.Log( LogLevel.Warning, msg )
                    failwith msg
    member x.RemappingCommandToLaunchService( queue, peeri, peeriPartitionArray:int[], curDSet:DSet ) = 
        use msPayload = new MemStream( 1024 )
        msPayload.WriteGuid( x.Job.JobID )
        msPayload.WriteString( curDSet.Name )
        msPayload.WriteInt64( curDSet.Version.Ticks )
        msPayload.WriteString( serviceName )
        msPayload.Serialize( serviceClass ) // Don't use Serialize From
        msPayload.SerializeObjectWithTypeName( param )
        Logger.LogF( x.Job.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Start, Service issued to peer %d partition %A" peeri peeriPartitionArray ))
        queue.ToSend( ControllerCommand( ControllerVerb.Start, ControllerNoun.Service), msPayload )
    member x.LaunchServiceCallback( cmd, peeri, msRcvd, jobID, name, verNumber, cl ) = 
        try
            let curDSet = x.ResolveDSetByName( name, verNumber )
            if Utils.IsNotNull curDSet then 
                let q = cl.Queue( peeri )
                match ( cmd.Verb, cmd.Noun ) with 
                | ( ControllerVerb.ConfirmStart, ControllerNoun.Service ) ->
                    let bSuccess = msRcvd.ReadBoolean()
                    x.GetJobInstance(curDSet).PeerCmdComplete( peeri ) 
                    if not bSuccess then 
                        x.GetJobInstance(curDSet).bPeerFailed.[peeri] <- true
                    Logger.LogF( x.Job.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Receive ConfirmStart, Service for service %s from peer %d with %A" curDSet.Name peeri bSuccess ))
                | _ ->
                    ()
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSetFoldAction.DSetFoldCallback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
        true    

[<AllowNullLiteral>]
type internal DSetStopServiceAction(cl:Cluster, serviceName:string)=
    inherit DSetAction()
    member x.StopService( ) = 
        // Bind DSet to the service, the DSet version is always the same, 
        let useCluster = if not (Utils.IsNull cl) then cl else Cluster.GetCurrent()
        if Utils.IsNull useCluster then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Need to specify cluster to start service " ))
        else
            // Bind a DSet name, and allow same service name to be launched on different cluster
            let serviceDSet = DSet<unit>( Name = serviceName, 
                                         Cluster = useCluster )
            serviceDSet.NumPartitions <- useCluster.NumNodes
            serviceDSet.NumReplications <- 1
            serviceDSet.Mapping <- Array.init useCluster.NumNodes ( fun i -> Array.create 1 i )
            serviceDSet.Dependency <- Source
    // Trigger serviceDSet to be a passthrough DSet
    //        serviceDSet.StorageType <- StorageType.Passthrough
    //        serviceDSet.Function <- Function()
            x.Param <- serviceDSet

            if x.ParameterList.Count<>1 then 
                let msg = sprintf "DSetStartServiceAction should take a single DSet parameter, while %d parameters are given" x.ParameterList.Count
                Logger.Log( LogLevel.Error, msg )
                failwith msg
            x.FurtherDSetCallback <- x.StopServiceCallback
        
            x.BeginActionWithLaunchMode(TaskLaunchMode.DonotLaunch)
            let useDSet = x.ParameterList.[0]
            if x.Job.ReadyStatus && useDSet.NumPartitions>0 then 
                x.GetJobInstance(useDSet).RemappingCommandCallback <- x.RemappingCommandToStopService
                // Send out the fold command. 
                x.RemappingDSet() |> ignore       
                while not (x.Timeout()) && not (x.GetJobInstance(useDSet).AllDSetsRead()) do
                    x.RemappingDSet() |> ignore
                    // Wait for result to come out. 
                    Thread.Sleep( 3 )
                if x.Timeout() then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Timeout for DSetFoldAction ............." ))
// Attempt to end job (as TaskLaunchMode.DonotLaunch)
                x.OrderlyEndAction()
            else
                let status, msg = x.Job.JobStatus()
                if status then
                    Logger.Log( LogLevel.Info, msg )
                else
                    Logger.Log( LogLevel.Warning, msg )
                    failwith msg                            
    member x.RemappingCommandToStopService( queue, peeri, peeriPartitionArray:int[], curDSet:DSet ) = 
        use msPayload = new MemStream( 1024 )
        msPayload.WriteGuid( x.Job.JobID )
        msPayload.WriteString( curDSet.Name )
        msPayload.WriteInt64( curDSet.Version.Ticks )
        msPayload.WriteString( serviceName )
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Stop, Service issued to peer %d partition %A" peeri peeriPartitionArray ))
        queue.ToSend( ControllerCommand( ControllerVerb.Stop, ControllerNoun.Service), msPayload )
    member x.StopServiceCallback( cmd, peeri, msRcvd, jobID, name, verNumber, cl ) = 
        try
            let curDSet = x.ResolveDSetByName( name, verNumber )
            if Utils.IsNotNull curDSet then 
                let q = cl.Queue( peeri )
                match ( cmd.Verb, cmd.Noun ) with 
                | ( ControllerVerb.ConfirmStop, ControllerNoun.Service ) ->
                    let bSuccess = msRcvd.ReadBoolean()
                    x.GetJobInstance(curDSet).PeerCmdComplete( peeri ) 
                    if not bSuccess then 
                        x.GetJobInstance(curDSet).bPeerFailed.[peeri] <- true
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Receive ConfirmStop, Service for service %s from peer %d with %A" curDSet.Name peeri bSuccess ))

                | _ ->
                    ()
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSetFoldAction.DSetFoldCallback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
        true    

/// <summary>
/// Launching Prajna Service on a group of remote nodes.
/// serviceName: name of the serivce
/// param: parameter to be called by OnStart
/// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
/// Service is very much like a Azure Woker Role. The core differences are:
/// 1. You can run Service on Azure VM or your own cluster in the data center. 
/// 2. You can host multiple worker role in a same remote program. 
/// 3. Service can interact (call each other) with each call being a function call. 
/// 4. You can perform Big data analytical tasks (map/filter/choose/collect, fold, map-reduce, sort, hash-join, cross-join, etc.) on data generated by Service
/// 5. You can start Service with a parameter. 
/// 6. You can track debug output (log, stderr, stdout) of the remote service. 
/// </summary>
type internal Service() =
        /// <summary>
        /// Launching Prajna Service on a group of remote nodes.
        /// serviceName: name of the serivce
        /// param: parameter to be called by OnStart
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// </summary>
        static member startServiceOnClusterWithParam (cl:Cluster) (serviceName:string) (serviceClass:WorkerRoleEntryPoint) (param:'StartParam) =
            use curJob = new DSetStartServiceAction<'StartParam>( cl, serviceName, serviceClass, param )
            curJob.StartService()

        /// <summary>
        /// Launching Prajna Service on a group of remote nodes.
        /// serviceName: name of the serivce
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// </summary>
        static member startServiceWithParam serviceName serviceClass (param:'StartParam) =
            Service.startServiceOnClusterWithParam null serviceName serviceClass param

        /// <summary>
        /// Launching Prajna Service on a group of remote nodes.
        /// serviceName: name of the serivce
        /// param: parameter to be called by OnStart
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// </summary>
        static member startServiceOnCluster (cl:Cluster) (serviceName:string) (serviceClass:WorkerRoleEntryPoint) =
            use curJob = new DSetStartServiceAction<unit>( cl, serviceName, serviceClass, () )
            curJob.StartService()

        /// <summary>
        /// Launching Prajna Service on a group of remote nodes.
        /// serviceName: name of the serivce
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// </summary>
        static member startService serviceName serviceClass =
            Service.startServiceOnCluster null serviceName serviceClass 


        /// <summary>
        /// Stopping Prajna Service on a group of remote nodes.
        /// serviceName: name of the service
        /// serviceClass: OnStart(), OnStop(), Run() call for the initiating of the service. 
        /// registeredDSets: each registered DSet has information on dsetName, dsetVersion, 
        /// </summary>
        static member stopServiceOnCluster (cl:Cluster) (serviceName:string) =
            use curJob = new DSetStopServiceAction( cl, serviceName )
            curJob.StopService()

        /// <summary>
        /// Stopping Prajna Service on a group of remote nodes.
        /// serviceName: name of the service
        /// serviceClass: OnStart(), OnStop(), Run() call for the initiating of the service. 
        /// registeredDSets: each registered DSet has information on dsetName, dsetVersion, 
        /// </summary>
        static member stopService serviceName =
            Service.stopServiceOnCluster null serviceName

        /// <summary>
        /// Launching Prajna Service locally, with a callback function that can be used to stop the current service. 
        /// This is mainly used for local debug of service before deployment. 
        /// serviceName: name of the serivce
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// param: parameter to be called by OnStart
        /// Return: toStop(unit->unit), a function that can be called to stop the current service. 
        /// </summary>
        static member startServiceLocallyWithParam serviceName (serviceClass:WorkerRoleEntryPoint) (param:'StartParam) =
            let bSuccess = serviceClass.OnStartByObject( param ) 
            if bSuccess then 
                let thread = ThreadTracking.StartThreadForFunctionWithCancelation (serviceClass.OnStop) (fun _ -> sprintf "Thread for service %s" serviceName ) (serviceClass.Run) 
                serviceClass.OnStop, serviceClass.IsRunning
            else
                let voidStop () = 
                    ()
                let voidIsRunning() = 
                    false
                voidStop, voidIsRunning

        /// <summary>
        /// Launching Prajna Service locally, with a callback function that can be used to stop the current service. 
        /// This is mainly used for local debug of service before deployment. 
        /// serviceName: name of the serivce
        /// serviceClass: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
        /// Return: toStop(unit->unit), a function that can be called to stop the current service. 
        /// </summary>
        static member startServiceLocally serviceName (serviceClass) =
            Service.startServiceLocallyWithParam serviceName serviceClass ()


/// <summary>
/// Launching a PrajnaInstance on a group of remote nodes.
/// serviceName: name of the serivce
/// param: parameter to be called by OnStart
/// serviceInstance: OnStart(param), OnStop(), Run(), IsRunning() call for the initiating of the service. 
///         You must derive serviceInstance from WorkerRoleInstance
/// PrajnaInstance share similar characteristics with Prajnaservice, with characteristics below.
/// 1. You can run Service on Azure VM or your own cluster in the data center. 
/// 2. You can host multiple worker role in a same remote program. 
/// 3. Service can interact (call each other) with each call being a function call. 
/// 4. You can perform Big data analytical tasks (map/filter/choose/collect, fold, map-reduce, sort, hash-join, cross-join, etc.) on data generated by Service
/// 5. You can start Service with a parameter. 
/// 6. You can track debug output (log, stderr, stdout) of the remote service. 
/// But in addition, PrajnaInstance is 
/// 1. Initialied remotely (with a function that you have specified). As such, PrajnaInstance doesn't need to be serializable, and can contain heavy data element. 
/// 2. You may specify the number of thread to work on the PrajnaInstance (parameter specified in Start Parameter. 
/// </summary>
type internal RemoteInstance() = 
    static let localInstances=ConcurrentDictionary<_,(_*_)>(StringComparer.Ordinal)
    static member val internal MaxWaitForLocalInstanceToStopInMs = 1000 with get, set
    /// <summary>
    /// Launching Prajna Service on a group of remote nodes.
    /// serviceName: name of the serivce
    /// param: parameter to be called by OnStart, must derive from WorkerRoleInstanceStartParam
    /// func: the function to initialized the WorkerRoleInstance. This function will be executed as a closure remotely. The returned class must derive from WorkerRoleInstance.
    /// </summary>
    static member internal InternalStartInstanceOnClusterWithParam<'StartParamType, 'RType 
                    when 'StartParamType :> WorkerRoleInstanceStartParam 
                    and 'RType :> WorkerRoleInstance > cl serviceName (param:'StartParamType) ( func: unit -> 'RType )  =
        let wrappedFunc () = 
            func() :> WorkerRoleInstance
        param.NewInstanceFunc <- wrappedFunc
        Service.startServiceOnClusterWithParam cl serviceName (WorkerRoleInstanceService<'StartParamType>()) param
    /// <summary>
    /// Launching Prajna Service on a group of remote nodes.
    /// serviceName: name of the serivce
    /// param: parameter to be called by OnStart, must derive from WorkerRoleInstanceStartParam
    /// func: the function to initialized the WorkerRoleInstance. This function will be executed as a closure remotely. The returned class must derive from WorkerRoleInstance.
    /// </summary>
    static member internal InternalStartInstanceWithParam serviceName (param) ( func )  =
        RemoteInstance.InternalStartInstanceOnClusterWithParam null serviceName param func
    /// <summary>
    /// Launching Prajna Service on a group of remote nodes.
    /// serviceName: name of the serivce
    /// param: parameter to be called by OnStart, must derive from WorkerRoleInstanceStartParam
    /// func: the function to initialized the WorkerRoleInstance. This function will be executed as a closure remotely. The returned class must derive from WorkerRoleInstance.
    /// </summary>
    static member internal InternalStartInstance serviceName ( func )  =
        let param = WorkerRoleInstanceStartParam()
        RemoteInstance.InternalStartInstanceOnClusterWithParam null serviceName param func

    /// <summary>
    /// Launching Instance locally.
    /// serviceName: name of the serivce
    /// param: parameter to be called by OnStart, must derive from WorkerRoleInstanceStartParam
    /// func: the function to initialized the WorkerRoleInstance. This function will be executed as a closure remotely. The returned class must derive from WorkerRoleInstance.
    /// Return: toStop (a Action can be called to terminate the current RoleInstance). 
    ///         isRunning ( a Func that can be called to evaluate if the current RoleInstance is still running)
    /// </summary>
    static member internal InternalStartLocalInstanceWithParam<'StartParamType, 'RType 
                    when 'StartParamType :> WorkerRoleInstanceStartParam 
                    and 'RType :> WorkerRoleInstance > serviceName (param:'StartParamType) ( func: unit -> 'RType )  =
        let wrappedFunc () = 
            func() :> WorkerRoleInstance
        param.NewInstanceFunc <- wrappedFunc
        let tuple = Service.startServiceLocallyWithParam serviceName (WorkerRoleInstanceService<'StartParamType>()) param
        localInstances.Item( serviceName) <- tuple 
    /// Is a local instance still running?
    static member internal InternalIsRunningLocalInstance serviceName = 
        let bExist, tuple = localInstances.TryGetValue( serviceName )
        if bExist then 
            let onStop, isRunning = tuple
            isRunning()
         else
            false
    /// Stop an Instance locally
    static member internal InternalStopLocalInstance serviceName = 
        let bExist, tuple = localInstances.TryGetValue( serviceName )
        if bExist then 
            let onStop, isRunning = tuple
            onStop()
            let ticksStart = DateTime.UtcNow.Ticks
            while isRunning() && 
                ( DateTime.UtcNow.Ticks - ticksStart )/TimeSpan.TicksPerMillisecond < int64 RemoteInstance.MaxWaitForLocalInstanceToStopInMs do 
                Threading.Thread.Sleep( 10 )
            localInstances.TryRemove( serviceName ) |> ignore 
    /// <summary>
    /// Launching Prajna Service on a group of remote nodes.
    /// serviceName: name of the serivce
    /// serverInfo: information of the server 
    /// param: parameter to be called by OnStart, must derive from WorkerRoleInstanceStartParam
    /// func: the function to initialized the WorkerRoleInstance. This function will be executed as a closure remotely. The returned class must derive from WorkerRoleInstance.
    /// </summary>
    static member internal InternalStartInstanceOnServersWithParam<'StartParamType, 'RType 
                    when 'StartParamType :> WorkerRoleInstanceStartParam 
                    and 'RType :> WorkerRoleInstance > (serverInfo:ContractServersInfo) serviceName (param:'StartParamType) (func:unit -> 'RType )  =
        let wrappedFunc () = 
            func() :> WorkerRoleInstance
        param.NewInstanceFunc <- wrappedFunc
        if Utils.IsNotNull serverInfo then 
            let parsedServerInfo = ContractServerInfoLocal.Parse( serverInfo )
            let clusters = parsedServerInfo.GetClusterCollection()
            for cl in clusters do 
                Service.startServiceOnClusterWithParam cl serviceName (WorkerRoleInstanceService<'StartParamType>()) param        
        else
            RemoteInstance.InternalStartLocalInstanceWithParam serviceName param func
            ()
    /// <summary>
    /// Stopping Service on a group of remote nodes.
    /// </summary>
    static member internal InternalStopInstanceOnCluster (cl:Cluster) (serviceName:string) =
        Service.stopServiceOnCluster cl serviceName

    /// <summary>
    /// Stopping Service on a group of remote nodes.
    /// </summary>
    static member internal InternalStopInstance serviceName =
        Service.stopServiceOnCluster null serviceName

    /// Stopping services on a set of servers
    static member internal InternalStopInstanceOnServers (serverInfo:ContractServersInfo) serviceName =
        if Utils.IsNotNull serverInfo then 
            let parsedServerInfo = ContractServerInfoLocal.Parse( serverInfo )
            let clusters = parsedServerInfo.GetClusterCollection()
            for cl in clusters do 
                Service.stopServiceOnCluster cl serviceName        
        else
            RemoteInstance.InternalStopLocalInstance serviceName


/// <summary>
/// Cache Service doesn't do anything itself. It only register a service with an AppDomain/Exe, so that when the client stops, the AppDomain/Exe continues to 
/// run and hold the recognition service. 
/// bool OnStart(): Run once when the service started
/// bool OnStop(): Run once when the service stopped
/// void Run(): main entry point when the service is running. 
/// bool IsRunning(): return true if the service is still running, false if the service terminates.
/// </summary>
[<Serializable>]
type internal CacheServiceClass() = 
    inherit WorkerRoleEntryPointUnit()
    [<field:NonSerialized>]
    member val internal bTerminate = false with get, set
    [<field:NonSerialized>]
    member val internal EvTerminate = null with get, set
    override x.OnStart param = 
        x.bTerminate <- false
        x.EvTerminate <- new ManualResetEvent( false )
        Logger.Log( LogLevel.MildVerbose, ("CacheService OnStart() is called ..... "))
        true
    override x.Run() = 
        Logger.Log( LogLevel.MildVerbose, ("CacheService has been started ..... "))
        while not x.bTerminate do 
            x.EvTerminate.WaitOne( 1000 ) |> ignore
    override x.OnStop() = 
        // Cancel all pending jobs. 
        x.bTerminate <- true
        x.EvTerminate.Set() |> ignore
        Logger.Log( LogLevel.MildVerbose, ("CacheService has been Stopped ..... "))
    override x.IsRunning() = 
        not x.bTerminate

/// <summary>
/// Cache Service doesn't do anything itself. It only register a service with an AppDomain/Exe, so that when the client stops, the AppDomain/Exe continues to 
/// run and hold cached data. 
/// </summary>
type CacheService() = 
    /// Default name of the cache service. 
    static member val CacheServiceName = "CacheService" with get, set
    /// Start cache service on current cluster
    static member Start() = 
        Service.startService CacheService.CacheServiceName (CacheServiceClass())
    /// Stop cache service on current cluster
    static member Stop()=
        Service.stopService CacheService.CacheServiceName 
    /// Start cache service on cluster
    static member Start(cl:Cluster) = 
        Service.startServiceOnCluster cl CacheService.CacheServiceName (CacheServiceClass())
    /// Stop cache service on cluster
    static member Stop(cl:Cluster)=
        Service.stopServiceOnCluster cl CacheService.CacheServiceName 

