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
        servicebase.fs
  
    Description: 
        Service

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Dec. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service

open System
open System.Collections.Concurrent
open System.Threading

open Prajna.Tools
open Prajna.Tools.FSharp

open Prajna.Core

 /// <summary>
/// WorkerRoleEntryPoint can be extended to run a Prajna worker service. The worker service will extend the class WorkerRoleEntryPoint with the following functions:
/// Please note that the extended class will be serialized to the remote end. It is crucial to consider the data footprint in serialization for efficient execution. 
/// If you have an heavy object, consider to use WorkerRoleInstance
/// </summary>
[<AbstractClass; Serializable>]
type internal WorkerRoleEntryPoint() = 
    /// bool OnStartByObject(): Run once when the service started
    abstract OnStartByObject: Object -> bool
    /// bool OnStop(): Run once when the service stopped
    abstract OnStop: unit -> unit
    /// void Run(): main entry point when the service is running. 
    abstract Run: unit -> unit
    /// bool IsRunning(): return true if the service is running (should be set at OnStart), false if the service terminates.
    abstract IsRunning: unit -> bool

/// <summary>
/// WorkerRoleInstance can be extended to run a Prajna worker service. Unlike WorkerRoleEntryPoint, WorkerRoleInstance is to be instantiated at the remote end 
/// so it is class member is not serialized. 
/// </summary>
[<AbstractClass; AllowNullLiteral>]
type WorkerRoleInstance() = 
    /// When an instance terminates it should set EvTerminated flag so that parent instance may terminate 
    member val EvTerminated = new ManualResetEvent( false ) with get
    static member val internal CheckRoleInstanceAliveInMs = 1000 with get, set
    /// bool OnStart(): Run once when the service instance started for all thread, it will be runned in each thread. 
    abstract OnStartByObject: Object -> bool
    /// bool OnStop(): Run once to stop all thread, 
    abstract OnStop: unit -> unit
    /// void Run(): main entry point when the service is running,
    abstract Run: unit -> unit
    /// bool IsRunning(): return true if the service is running (should be set at OnStart), false if the service terminates.
    abstract IsRunning: unit -> bool

type internal RunningService( serviceName: string, service: WorkerRoleEntryPoint, param: Object ) = 
    member val internal InitializeCalled = ref 0 with get
    member val internal DoneInitialized = new ManualResetEvent(false) with get
    member val internal IsInitialized = false with get, set
    member val internal RemoteRunningThread = null with get, set
    /// <summary>
    /// Start the service, when service completes, a continuation function is called. 
    /// contFunc: bool-> unit, called after service is initialized. true: if service succeeds to initialize
    ///                                                             false: if service fails to initialize
    /// </summary>
    member internal x.StartService( contFunc ) = 
        try
            if Interlocked.CompareExchange( x.InitializeCalled, 1, 0 ) = 0 then 
                // The current process is in charge of initializing 
                x.RemoteRunningThread <- ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "Main thread for service %s" serviceName ) 
                        ( fun _ -> Logger.LogF( DeploymentSettings.TraceLevelServiceTracking, ( fun _ -> sprintf "service %s is initialized by calling OnStart()" serviceName ))
                                   let bInitializedSucceed = service.OnStartByObject param
                                   x.IsInitialized <- bInitializedSucceed
                                   x.DoneInitialized.Set() |> ignore
                                   if bInitializedSucceed then 
                                       Logger.LogF( DeploymentSettings.TraceLevelServiceTracking, ( fun _ -> sprintf "service %s initialization is successful, running main loop Run()" serviceName ))
                                       service.Run()
                                   else
                                       Logger.LogF( DeploymentSettings.TraceLevelServiceTracking, ( fun _ -> sprintf "service %s initialization fails, main loop does not run" serviceName ) )
                        )
                ThreadPoolWait.WaitForHandle ( fun _ -> sprintf "Wait for service %s to complete initialization (i.e., OnStart())" serviceName )
                    ( x.DoneInitialized ) ( fun _ -> contFunc(x.IsInitialized) ) null 
            else
                ThreadPoolWait.WaitForHandle ( fun _ -> sprintf "Wait for service %s to complete initialization (i.e., OnStart())" serviceName )
                    ( x.DoneInitialized ) ( fun _ -> contFunc(x.IsInitialized) ) null 
        with 
        | e -> 
            // Any service initialization exception
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "at StartService, service %s encounter exception %A"
                                                       serviceName e ))
            x.DoneInitialized.Set() |> ignore
            x.IsInitialized <- false
            contFunc(x.IsInitialized)
    /// <summary>
    /// Attempt to stop service. After service is stopped, a continuation function is called. 
    /// contFunc: bool -> unit, called after service is stopped. true: service has been successfully stopped. 
    ///                                                          false: timeout
    /// </summary>
    member internal x.StopService( ) = 
        try
            if !(x.InitializeCalled) <> 0 then 
                let t1 = (PerfDateTime.UtcNow())
                x.DoneInitialized.WaitOne() |> ignore
                // Wait for initialization to complete (no point in interrupt)
                if x.IsInitialized then 
                    service.OnStop() 
                let serviceStop = ref (service.IsRunning())
                while (!serviceStop) do
                    let t2 = (PerfDateTime.UtcNow())
                    let elapseMs = t2.Subtract(t1).TotalMilliseconds
                    if int elapseMs >= DeploymentSettings.TimeOutServiceStopInMs then 
                        serviceStop := true
                    else
                        let sleepInterval = Math.Min( DeploymentSettings.TimeOutServiceStopInMs - int elapseMs, DeploymentSettings.SleepIntervalServiceStopInMs )
                        Thread.Sleep( sleepInterval )
                        serviceStop := service.IsRunning()
                service.IsRunning()
            else
                false            
        with 
        | e -> 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "at StopService, service %s encounter exception %A"
                                                       serviceName e ))
            // Any exception, 
            false

type internal ServiceCollection() = 
    static member val Current = ServiceCollection() with get
    member val ServiceCollection = ConcurrentDictionary<_,RunningService>(StringComparer.Ordinal) with get
    member x.BeginStartService( serviceName, serviceClass, param, contFunc) = 
        let actualService = x.ServiceCollection.GetOrAdd( serviceName, fun _ -> RunningService( serviceName, serviceClass, param ) )
        actualService.StartService ( x.EndStartSerivce contFunc serviceName )    
    member x.EndStartSerivce contFunc (serviceName) bLaunchSuccessFul =
        if not bLaunchSuccessFul then 
            x.ServiceCollection.TryRemove( serviceName ) |> ignore
        contFunc( bLaunchSuccessFul )
    member x.StopService( serviceName: string ) = 
        let bRemove, runningService = x.ServiceCollection.TryRemove( serviceName )
        if bRemove then 
            let bIsRunning = runningService.StopService()    
            // Not running is successfully removed
            not bIsRunning
        else
            true
    member x.AllServiceNames() = 
        x.ServiceCollection.Keys :> seq<_>    
    member x.RemoveStoppedService() = 
        for pair in x.ServiceCollection do 
            let runningService = pair.Value
            let bDoneInitialization = runningService.DoneInitialized.WaitOne(0)
            if bDoneInitialization then 
                // Thread is executing 
                if runningService.RemoteRunningThread.ThreadState &&& ThreadState.Stopped = ThreadState.Stopped then 
                    // runningService has stopped. 
                    x.ServiceCollection.TryRemove( pair.Key ) |> ignore
                    ()
    member x.IsEmpty with get() = x.RemoveStoppedService()
                                  x.ServiceCollection.IsEmpty

