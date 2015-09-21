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
        client.fs
  
    Description: 
        Prajna Client. A client that waits command to execute. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Threading
open System.Diagnostics
open System.IO
open System.Net.Sockets
open System.Reflection

open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.Process

type internal ClientLauncher() = 
    inherit System.MarshalByRefObject() 
    static let Usage = "PrajnaClient \n\
        Command line arguments:\n\
        -logdir  the dir for the log files\n\
        -pwd     the passwd used for job request authentication\n\
        -mem     maximum memory size in MB allowed for the client\n\
        -port    the port that is used to listen the request\n\
        -jobports the range of ports for jobs\n\
        "

    // The name of the semaphore that is informing the test host that the client is ready to serve
    static member private ReadySemaphoreName = "Prajna-Client-Local-Test-Ready-cd42c4c6-61d8-45fe-8bf9-5ab3345f4f29"
    // The name of the event that test host uses to inform the client to shutdown
    static member private ShutdownEventName = "Prajna-Client-Local-Test-Shutdown-cd42c4c6-61d8-45fe-8bf9-5ab3345f4f29"
    // The name of the event that client uses to notify the host that it has completed shutdown 
    static member private ShutdownCompletionSemaphoreName = "Prajna-Client-Local-Test-Shutdown-Completion-cd42c4c6-61d8-45fe-8bf9-5ab3345f4f29"

    static member CreateReadySemaphore numClients =
        new Semaphore(0, numClients, ClientLauncher.ReadySemaphoreName)

    static member private NotifyClientReady () =
        let sem = Semaphore.OpenExisting(ClientLauncher.ReadySemaphoreName)
        sem.Release() |> ignore

    static member CreateShutdownEvent () = 
        new EventWaitHandle(false, EventResetMode.ManualReset, ClientLauncher.ShutdownEventName)
    
    static member private CheckShutdownEvent () =
        let e = EventWaitHandle.OpenExisting(ClientLauncher.ShutdownEventName)
        e.WaitOne(0)

    static member CreateShutdownCompletionSemaphore numClients = 
        new Semaphore(0, numClients, ClientLauncher.ShutdownCompletionSemaphoreName)

    static member private NotifyClientShutdownCompletion () = 
        let sem = Semaphore.OpenExisting(ClientLauncher.ShutdownCompletionSemaphoreName)
        sem.Release() |> ignore


    static member Main orgargs = 
        let args = Array.copy orgargs
        let firstParse = ArgumentParser(args, false)
        
        let logdir = 
            // Keep "-dirlog" to not break existing scripts
            let d1 = firstParse.ParseString( "-dirlog", String.Empty )
            let d2 = firstParse.ParseString( "-logdir", String.Empty )
            if d1 = String.Empty && d2 = String.Empty then
                DeploymentSettings.LogFolder
            else if d2 <> String.Empty then
                d2
            else
                d1

        DeploymentSettings.LogFolder <- logdir
        let logdirInfo = FileTools.DirectoryInfoCreateIfNotExists( logdir ) 
        let logFileName = Path.Combine( logdir, VersionToString( (PerfDateTime.UtcNow()) ) + ".log" )

        let args2 = Array.concat (seq { yield (Array.copy args); yield [|"-log"; logFileName|] })
        let parse = ArgumentParser(args2)  

        let keyfile = parse.ParseString( "-keyfile", "" )
        let keyfilepwd = parse.ParseString( "-keypwd", "" )
        let pwd = parse.ParseString( "-pwd", "" )
        let bAllowRelay = parse.ParseBoolean( "-allowrelay", DeploymentSettings.bDaemonRelayForJobAllowed )
        let bAllowAD = parse.ParseBoolean( "-allowad", false )
        let bLocal = parse.ParseBoolean( "-local", false )
        let nTotalJob = parse.ParseInt( "-totaljob", -1 )
        let bUseAllDrive = parse.ParseBoolean( "-usealldrives", false )
        if bUseAllDrive then 
            DeploymentSettings.UseAllDrivesForData()
  
        try
            let ramCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
            // Find usable RAM space, leave 512MB, and use all. 
            let usableRAM =( (int64 (ramCounter.NextValue())) - 512L ) <<< 20
            if usableRAM > 0L then
                let maxWorkingSet = 
                    if IntPtr.Size = 4 then 
                        nativeint (Math.Min( usableRAM, 1300L<<<20 ))
                    else
                        nativeint usableRAM
                if maxWorkingSet > System.Diagnostics.Process.GetCurrentProcess().MinWorkingSet then
                    System.Diagnostics.Process.GetCurrentProcess().MaxWorkingSet <- maxWorkingSet
        with
        | e -> // Exception here should not cause the program to fail, it's safe to continue 
               Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "ClientLauncher.Main: exception on reading perf counter and set MaxWorkingSet: %A" e))

        if nTotalJob > 0 then 
            DeploymentSettings.TotalJobLimit <- nTotalJob
            DeploymentSettings.ExeJobLimit <- nTotalJob
            DeploymentSettings.AppDomainJobLimit <- nTotalJob
            Logger.Log( LogLevel.Info, ( sprintf "Set # of Jobs allowed to execute in parallel to %d ..........." (DeploymentSettings.TotalJobLimit) ))

        let memLimit = parse.ParseInt64( "-mem", (DeploymentSettings.MaxMemoryLimitInMB) )
        if memLimit <> (DeploymentSettings.MaxMemoryLimitInMB) then 
            DeploymentSettings.MaxMemoryLimitInMB <- memLimit
            Logger.Log( LogLevel.Info, ( sprintf "Set Memory Limit to %d MB..........." (DeploymentSettings.MaxMemoryLimitInMB) ))
        let ip = parse.ParseString("-ip", "")
        let port = parse.ParseInt( "-port", (DeploymentSettings.ClientPort) )
        DeploymentSettings.ClientIP <- ip
        DeploymentSettings.ClientPort <- port
        Prajna.Core.Cluster.Connects.IpAddr <- ip
        let jobDirectory = Path.Combine(DeploymentSettings.LocalFolder, DeploymentSettings.JobFolder + DeploymentSettings.ClientPort.ToString() )
        JobDependency.CleanJobDir(jobDirectory)

        let jobip = parse.ParseString("-jobip", "")
        DeploymentSettings.JobIP <- jobip
        
        let jobport = 
            // Keep "-jobport" to not break existing scripts
            let j = parse.ParseString( "-jobports", String.Empty )
            if j <> String.Empty then
                j
            else
                parse.ParseString( "-jobport", String.Empty )

        if Utils.IsNotNull jobport && jobport.Length > 0 then 
            let jobport2 = jobport.Split(("-,".ToCharArray()), StringSplitOptions.RemoveEmptyEntries )
            if jobport2.Length >= 2 then 
                DeploymentSettings.JobPortMin <- Int32.Parse( jobport2.[0] )
                DeploymentSettings.JobPortMax <- Int32.Parse( jobport2.[1] )
            else if jobport2.Length >= 1 then 
                let numPorts = DeploymentSettings.JobPortMax - DeploymentSettings.JobPortMin
                DeploymentSettings.JobPortMin <- Int32.Parse( jobport2.[0] )
                DeploymentSettings.JobPortMax <- DeploymentSettings.JobPortMin + numPorts
            ()

        Logger.Log( LogLevel.Info, ( sprintf "Start PrajnaClient at port %d (%d-%d)...................... Mode %s, %d MB" DeploymentSettings.ClientPort DeploymentSettings.JobPortMin DeploymentSettings.JobPortMax DeploymentSettings.PlatformFlag (DeploymentSettings.MaximumWorkingSet>>>20)))

        let argsToLog = Array.copy orgargs
        for i in 0..argsToLog.Length-1 do
            if String.Compare(argsToLog.[i], "-pwd", StringComparison.InvariantCultureIgnoreCase) = 0 ||
               String.Compare(argsToLog.[i], "-keypwd", StringComparison.InvariantCultureIgnoreCase) = 0 then
                argsToLog.[i + 1] <- "****"

        Logger.Log( LogLevel.Info, ( sprintf "Start Parameters %A" argsToLog ))

        if bAllowAD then 
            Logger.Log( LogLevel.Info, ( sprintf "Allow start remote container in AppDomain" ))
            DeploymentSettings.AllowAppDomain <- true
        if bAllowRelay then 
            Logger.Log( LogLevel.Info, ( sprintf "Allow daemon to realy command for jobs. " ))
            DeploymentSettings.bDaemonRelayForJobAllowed <- true

    //    MakeFileAccessible( logfname ) : logfname may not be the name used for log anymore

        // Delete All log except the latest 5 log
        let allLogFiles = logdirInfo.GetFiles()
        let sortedLogFiles = allLogFiles |> Array.sortBy( fun fileinfo -> fileinfo.CreationTimeUtc.Ticks )
        for i=0 to sortedLogFiles.Length-DeploymentSettings.LogFileRetained do
            sortedLogFiles.[i].Delete()    

        let PrajnaMasterFile = parse.ParseString( "-master", "" )
        let masterInfo = 
            if Utils.IsNotNull PrajnaMasterFile && PrajnaMasterFile.Length>0 then 
                Logger.Log( LogLevel.Info, (sprintf "Master information file ==== %s" PrajnaMasterFile ))
                ClientMasterConfig.Parse( PrajnaMasterFile )          
            else
                ClientMasterConfig.ToParse( parse )          
        let bAllParsed = parse.AllParsed Usage
        if not bAllParsed then
            failwith "Incorrect arguments"
        Logger.Log( LogLevel.Info, (sprintf "All command parsed ==== %A" bAllParsed ) )

    //    let dataDrive = StringTools.GetDrive( dir )
        let client0 =
            if masterInfo.ReportingServers.Count > 0 then 
                let client = new HomeInClient( masterInfo.VersionInfo, DeploymentSettings.DataDrive, masterInfo.ReportingServers,  masterInfo.MasterName, masterInfo.MasterPort )

                //Async.StartAsTask(ClientController.ConnectToController(client, masterInfo.ControlPort)) |> ignore

                for pair in masterInfo.Executables do
                    let exe, param = pair.Key, pair.Value
                    Logger.Log( LogLevel.Info, (sprintf "To execute %s with %s" exe param ))

                // let homeInTimer = new Timer( HomeInClient.HomeInClient, client, 0, 10000)  
                // ListenerThread
//                let HomeInThreadStart = new Threading.ParameterizedThreadStart( HomeInClient.HomeInClient )
//                let HomeInThread = new Threading.Thread( HomeInThreadStart )
//                HomeInThread.IsBackground <- true
//                HomeInThread.Start( client )
                let HomeInThread = ThreadTracking.StartThreadForFunction( fun _ -> "Home in thread") ( fun _ -> HomeInClient.HomeInClient client)

                client
            else
                null
        let listener = Listener.StartListener()
        let monTimer = ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Task Queue Monitor Timer" ) listener.TaskQueue.MonitorTasks DeploymentSettings.MonitorIntervalTaskQueueInMs DeploymentSettings.MonitorIntervalTaskQueueInMs
        
        // set authentication parameters for networking
        Logger.Log( LogLevel.Info, (sprintf "Authentication parameters: pwd=%s keyfile=%s keyfilepwd=%s" 
                                                (if String.IsNullOrEmpty(pwd) then "empty" else "specified") 
                                                keyfile 
                                                (if String.IsNullOrEmpty(keyfilepwd) then "empty" else "specified") ) )
        if not (keyfilepwd.Equals("", StringComparison.Ordinal)) then
            if (not (pwd.Equals("", StringComparison.Ordinal))) then
                listener.Connects.InitializeAuthentication(pwd, keyfile, keyfilepwd)
            else
                let keyinfo = NetworkConnections.ObtainKeyInfoFromFiles(keyfile)
                listener.Connects.InitializeAuthentication(keyinfo, keyfilepwd)
        elif (not (pwd.Equals("", StringComparison.Ordinal))) then
            listener.Connects.InitializeAuthentication(pwd)

//        let ListenerThreadStart = new Threading.ParameterizedThreadStart( Listener.StartServer )
//        let ListenerThread = new Threading.Thread( ListenerThreadStart )
//        ListenerThread.IsBackground <- true
//        ListenerThread.Start( listener )
        let ListenerThread = ThreadTracking.StartThreadForFunction( fun _ -> sprintf "Main Listener thread for daemon, on port %d" DeploymentSettings.ClientPort ) (fun _ -> Listener.StartServer listener)


        if bLocal then
            // for local, the host should have created the semaphore, the client
            // tries to notify the host that it is ready to serve 
            ClientLauncher.NotifyClientReady()
            
        while (not (bLocal && ClientLauncher.CheckShutdownEvent())) do
            Thread.Sleep(100)

        if not (Utils.IsNull client0) then 
            client0.Shutdown <- true

        listener.InListeningState <- false // Shutdown main loop
    //     homeInTimer.Dispose()
    //    ListenerThread.Abort()
    (*
        // With startclient.ps1, we do not use auto update at this moment. 
    *)
        // Should give a chance for attached Remote container to shutdown. 

        (*
        let tStart = PerfDateTime.UtcNowTicks()
        let mutable bDoneWaiting = false
        while not bDoneWaiting do 
            if listener.TaskQueue.IsEmptyExecutionTable() then 
                bDoneWaiting <- true 
            else
                let tCur = PerfDateTime.UtcNowTicks()
                let span = (tCur - tStart) / TimeSpan.TicksPerMillisecond
                if span < 5000L then 
                    Threading.Thread.Sleep(50)
                else
                    bDoneWaiting <- true
        *)
        listener.TaskQueue.EvEmptyExecutionTable.WaitOne( 5000 ) |> ignore 

        CleanUp.Current.CleanUpAll()



//      Code below is included in Cluster.Connects.Close()
//        ThreadPoolWait.TerminateAll()
//        ThreadTracking.CloseAllActiveThreads DeploymentSettings.ThreadJoinTimeOut
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "daemon on port %d ended" DeploymentSettings.ClientPort ))

        if bLocal then
            // Notify the host that the shutdown has completed
            ClientLauncher.NotifyClientShutdownCompletion()

        /// Give a chance for all the launched task to be closed. 

        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ClientLauncher.Main (for daemon on port %d) returns" DeploymentSettings.ClientPort))
        0 // return an integer exit code

    // Instance member that wraps over Main
    member x.Start orgargs =
        ClientLauncher.Main orgargs

    // start the client inside an AppDomain
    static member StartAppDomain name port (orgargs : string[]) =
        let thread = ThreadTracking.StartThreadForFunction (fun _ -> sprintf "Daemon on port %d" port ) ( fun () ->
            try
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Start AppDomain for client with name '%s' and args '%s'" name (String.Join(" ", orgargs))))
    
                let adName = name + "_" + VersionToString(PerfADateTime.UtcNow()) + Guid.NewGuid().ToString("D")
                let ads = new AppDomainSetup()
                ads.ApplicationBase <- Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) 

                let ad = AppDomain.CreateDomain(adName, new Security.Policy.Evidence(), ads)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "AppDomain %s created (%i)" adName port))
                let fullTypeName = Operators.typeof< ClientLauncher >.FullName
                let curAsm = Assembly.GetExecutingAssembly()
                try
                    let proxy = ad.CreateInstanceFromAndUnwrap( curAsm.Location, fullTypeName)
                    let remoteObj = proxy :?> ClientLauncher
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "RemoteObject for %s is created (%i)" fullTypeName port))
                    remoteObj.Start orgargs |> ignore
                    Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ClientLauncher.Start has returned, current domain is '%s'" AppDomain.CurrentDomain.FriendlyName)
                finally
                    try 
                        if DeploymentSettings.SleepMSBeforeUnloadAppDomain > 0 then 
                            Threading.Thread.Sleep( DeploymentSettings.SleepMSBeforeUnloadAppDomain )
                        // Currently, during unit test clean up phase, the container hosted by appdomain takes very long time to unload for unknow reasons. It causes
                        // the appdomain for deamon (which starts the container appdomain) also cannot unload quickly.
                        // As a temporary workaround before the root cause was figured out, the code below limits the time given to unload the appdomain. 
                        // Notes:
                        //  1. When we reach this point, the execution for the daemon has already completed. Unload the now un-used appdomain is the only thing left.
                        //  2. If the unload itself hit any exception, the Wait should rethrow it here and it would be caught by the "with" below.
                        if Async.StartAsTask(async { AppDomain.Unload( ad ) }).Wait(DeploymentSettings.AppDomainUnloadWaitTime) then
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Successfully unload AppDomain for client with name '%s' and args '%s'" name (String.Join(" ", orgargs))))
                        else
                            Logger.LogF( LogLevel.MildVerbose, 
                                ( fun _ -> sprintf "Time budget (%f ms) for unload AppDomain for client with name '%s' is used up, continue without waiting " 
                                             DeploymentSettings.AppDomainUnloadWaitTime.TotalMilliseconds name )  )                            
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "fail to unload AppDomain for client with name '%s' and args '%s'" name (String.Join(" ", orgargs))))
            with            
            | e -> 
                Logger.Log( LogLevel.Error, ( sprintf "ClientLauncher.StartAppDomain exception: %A" e ))
            Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "ClientLauncher.StartAppDomain (for client with name '%s' and args '%s') returns" name (String.Join(" ", orgargs))))
        )
        ()
