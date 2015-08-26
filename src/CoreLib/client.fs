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
        -allowad Allow AppDoamin execution \n\
        -master  MASTER_INFORMATION_FILE : master information file\n\
        -dir     WORKING_DIRECTORY :  working directory\n\
        -mem     MAX_MEMORY_LIMIT : Maximum Memory Limit in MB\n\
        -totaljob   TOTAL_JOB_LIMIT : set number of jobs that can be executed by Prajna in parallel\n\
        -local   start on local machine\n\
        -usealldrives use all drives for data (including system drive)\n\
        "

    static let TryRemoteAccess() = 
        let curAcct = System.Security.Principal.WindowsIdentity.GetCurrent()
        Logger.Log( LogLevel.Info, ( sprintf "Current user: %s (IsAuthenticated: %A)" (curAcct.Name) (curAcct.IsAuthenticated) ))
        //    for claim in curAcct.Claims do 
        //        Logger.Log(LogLevel.Info, ( sprintf "Type %A Subject:%A Issuer:%A value:%A " claim.Type claim.Subject claim.Issuer claim.Value ))
        try
            let fname = """\\OneNet20\c$\Prajna\Log\JinL\test_remotewrite.txt""" 
            let fStream = new FileStream( fname, FileMode.Create, Security.AccessControl.FileSystemRights.FullControl, 
                                        FileShare.ReadWrite, (1<<<20), FileOptions.Asynchronous )
            let enc = System.Text.UTF8Encoding().GetBytes( "To test if remote write can succeed, cow jump over the moon" )
            fStream.Write( enc, 0, enc.Length )
            fStream.Flush()
            fStream.Close()
            Logger.Log( LogLevel.Info, ( sprintf "Remote writing succeeds" ))
        with
        | e -> 
            Logger.Log( LogLevel.Info, ( sprintf "Remote writing fails with exception %A" e))

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
        //System.Threading.ThreadPool.SetMinThreads(500,100) |> ignore
        let args = Array.copy orgargs
        let firstParse = ArgumentParser(args, false)  
        let logdir = firstParse.ParseString( "-dirlog", (DeploymentSettings.LogFolder) )
        DeploymentSettings.LogFolder <- logdir
        let logdirInfo = FileTools.DirectoryInfoCreateIfNotExists( logdir ) 
        let logFileName = Path.Combine( logdir, VersionToString( (PerfDateTime.UtcNow()) ) + ".log" )

        let args2 = Array.concat (seq { yield (Array.copy args); yield [|"-log"; logFileName|] })
        let parse = ArgumentParser(args2)  

        let keyfile = parse.ParseString( "-keyfile", (DeploymentSettings.KeyFile) )
        let keyfilepwd = parse.ParseString( "-keypwd", "" )
        let pwd = parse.ParseString( "-pwd", "" )
        let bAllowRelay = parse.ParseBoolean( "-allowrelay", DeploymentSettings.bDaemonRelayForJobAllowed )
        let bAllowAD = parse.ParseBoolean( "-allowad", false )
        let bLocal = parse.ParseBoolean( "-local", false )
        let nTotalJob = parse.ParseInt( "-totaljob", -1 )
        let bUseAllDrive = parse.ParseBoolean( "-usealldrives", false )
        if bUseAllDrive then 
            DeploymentSettings.UseAllDrivesForData()
  
        // Trigger Tracefile creation, otherwise, MakeFileAccessible( logfname ) will fail. 
        let ramCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
        // Find usable RAM space, leave 512MB, and use all. 
        let usableRAM =( (int64 (ramCounter.NextValue())) - 512L ) <<< 20
        if IntPtr.Size = 4 then 
            System.Diagnostics.Process.GetCurrentProcess().MaxWorkingSet <- nativeint (Math.Min( usableRAM, 1300L<<<20 ))
        else
            System.Diagnostics.Process.GetCurrentProcess().MaxWorkingSet <- nativeint usableRAM
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
        OneNet.Core.Cluster.Connects.IpAddr <- ip
        let jobDirectory = Path.Combine(DeploymentSettings.LocalFolder, DeploymentSettings.JobFolder + DeploymentSettings.ClientPort.ToString() )
        JobDependency.CleanJobDir(jobDirectory)

        let jobip = parse.ParseString("-jobip", "")
        DeploymentSettings.JobIP <- jobip
        let jobport = parse.ParseString( "-jobport", "" )
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
        Logger.Log( LogLevel.Info, ( sprintf "Start Parameters %A" orgargs ))

        if bAllowAD then 
            Logger.Log( LogLevel.Info, ( sprintf "Allow start remote container in AppDomain" ))
            DeploymentSettings.AllowAppDomain <- true
        if bAllowRelay then 
            Logger.Log( LogLevel.Info, ( sprintf "Allow daemon to realy command for jobs. " ))
            DeploymentSettings.bDaemonRelayForJobAllowed <- true

    //    MakeFileAccessible( logfname ) : logfname may not be the name used for log anymore

    //    TryRemoteAccess()
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
        if not (keyfilepwd.Equals("", StringComparison.Ordinal)) then
            if (not (pwd.Equals("", StringComparison.Ordinal))) then
                listener.Connects.InitializeAuthentication(pwd, keyfile, keyfilepwd)
            else
                let keyinfo = NetworkConnections.ObtainKeyInfoFromFiles(keyfile)
                listener.Connects.InitializeAuthentication(keyinfo, keyfilepwd)

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
                finally
                    try 
                        if DeploymentSettings.SleepMSBeforeUnloadAppDomain > 0 then 
                            Threading.Thread.Sleep( DeploymentSettings.SleepMSBeforeUnloadAppDomain )
                        AppDomain.Unload(ad)   
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Successfully unload AppDomain for client with name '%s' and args '%s'" name (String.Join(" ", orgargs))))
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "fail to unload AppDomain for client with name '%s' and args '%s'" name (String.Join(" ", orgargs))))
            with            
            | e -> 
                Logger.Log( LogLevel.Error, ( sprintf "ClientLauncher.StartAppDomain exception: %A" e ))
        )
        ()
