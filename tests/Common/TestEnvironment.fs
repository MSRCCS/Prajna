namespace Prajna.Test.Common

open System
open System.Diagnostics
open System.IO
open System.Threading

open Prajna.Core
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Service.FSharp

// The test environment
type TestEnvironment private () = 

    static let useAppDomainForDaemonsAndContainers = true
    static let clusterSize = 2
    // To use a remote cluster, set "useRemoteCluster" to true, and provide a remote cluster list file
    static let useRemoteCluster = false
    static let RemoteClusterListFile = @"path-to-a-cluster-list-file"
    static let envStartTime = DateTime.UtcNow
    static let travisLogLevel = 4
    static let generalLogLevel = 4

    // Is the testing running in a Travis CI env (https://travis-ci.org)
    static let isRunningOnTravisEnv =
        // According to https://docs.travis-ci.com/user/environment-variables/#Default-Environment-Variables
        // * TRAVIS=true
        let travisEnvVal = Environment.GetEnvironmentVariable("TRAVIS")
        let isTravis = Utils.IsNotNull travisEnvVal && travisEnvVal = "true"
        isTravis

    static let env = lazy(let e = new TestEnvironment()
                          AppDomain.CurrentDomain.DomainUnload.Add(fun _ -> (e :> IDisposable).Dispose())
                          e)

    do
        Environment.Init()
        let logdir = Path.Combine ([| DeploymentSettings.LocalFolder; "Log"; "UnitTest" |])
        let fileLog = Path.Combine( logdir, "UnitTestApp_" + StringTools.UtcNowToString() + ".log" )
        // Note: Currently there're bugs that causes the travis build to sometimes fail when use "5". With "6", the chance is better
        //       This "workaround" is not a fix but just for unblocking the setup of travis build. The underlying issue must be investigated
        let logLevel = if isRunningOnTravisEnv then travisLogLevel.ToString() else generalLogLevel.ToString()
        let args = [| "-verbose"; logLevel; 
                       "-log"; fileLog |]
        let dirInfo= FileTools.DirectoryInfoCreateIfNotExists logdir
        let dirs = dirInfo.GetDirectories()
         // Remove related versions. 
        for dir in dirs do
             try 
                 Directory.Delete(dir.FullName, true)
             with 
             | e -> 
                 Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "(May be OK) Failted to delete directory %s, with exception %A" dir.FullName e ))
        for file in dirInfo.GetFiles() do 
             try 
                 File.Delete(file.FullName)
             with 
             | e -> 
                 Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "(May be OK) Failted to delete file %s, with exception %A" file.FullName e ))

        let parse = ArgumentParser(args)      
        Logger.Log( LogLevel.Info, "##### Setup test environment starts .... #####")
        Logger.Log( LogLevel.Info, sprintf "Current AppDomain: %s" (AppDomain.CurrentDomain.FriendlyName))
    
    let reportProcessStatistics msg = 
        GC.Collect()
        GC.WaitForPendingFinalizers()
        let proc = Process.GetCurrentProcess()
        let maxThreads, maxIOThreads = ThreadPool.GetMaxThreads()
        let availThreads, availIOThreads = ThreadPool.GetAvailableThreads()
        Logger.Log(LogLevel.Info, 
                   sprintf "%s -- # of TH: %i, (%i, %i) ThreadPool THs, GC Heap: %f MB, Private Memory: %f MB" 
                       msg (proc.Threads.Count) (maxThreads - availThreads) (maxIOThreads - availIOThreads) ((float (GC.GetTotalMemory(false))) / 1e6) (float proc.PrivateMemorySize64 / 1e6))

    let testCluster =
        Logger.Log( LogLevel.Info, "##### Setup LocalCluster for tests starts.... #####")
        reportProcessStatistics("Before local cluster is created")
        let sw = Stopwatch()
        sw.Start()
        // Note: Currently there're bugs that causes the travis build to sometimes fail when use "LogLevel.MediumVerbose". With "LogLevel.WildVerbose", the chance is better
        //       This "workaround" is not a fix but just for unblocking the setup of travis build. The underlying issue must be investigated
        let logLevel = if isRunningOnTravisEnv then LogLevel.WildVerbose else LogLevel.MediumVerbose
        DeploymentSettings.LocalClusterTraceLevel <- logLevel
        // Sometimes the AppVeyor build VM is really slow on IO and need more time to establish the container
        DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit <- 240L

        let useRealCluster = true

        let cl =
            if useRemoteCluster then
                Cluster(RemoteClusterListFile)
            else
            if useAppDomainForDaemonsAndContainers then
                Cluster(sprintf "local[%i]" clusterSize)
            else
                let localClusterCfg = { Name = sprintf "LocalPP-%i" clusterSize
                                        Version = (DateTime.UtcNow)
                                        NumClients = clusterSize
                                        ContainerInAppDomain = false
                                        ClientPath = "PrajnaClient.exe" |> Some // Note: put the path of PrajnaClient here
                                        NumJobPortsPerClient = 5 |> Some
                                        PortsRange = (20000, 20011) |> Some
                                      }
                Cluster(localClusterCfg)

        reportProcessStatistics("After local cluster is created")
        CacheService.Start(cl)
        sw.Stop()
        reportProcessStatistics("After containers are created")
        Logger.Log( LogLevel.Info, (sprintf "##### Setup LocalCluster for tests .... completed (%i ms) #####" (sw.ElapsedMilliseconds)))
        cl

    let completed = 
        Logger.Log( LogLevel.Info, "##### Setup test environment .... completed #####")
        true

    let mutable disposed = false
    let dispose () =
        if not disposed then
            let sw = Stopwatch()
            Logger.Log( LogLevel.Info, "##### Dispose test environment starts ..... #####") 
            reportProcessStatistics("Before closing containers")
            sw.Start()
            CacheService.Stop(testCluster)
            reportProcessStatistics("After closing containers")
            Prajna.Core.Environment.Cleanup()
            sw.Stop()
            reportProcessStatistics("After environment cleanup")
            Logger.Log( LogLevel.Info, (sprintf "##### Dispose test environment ..... completed ##### (%i ms)" (sw.ElapsedMilliseconds)))
            Thread.Sleep(TimeSpan.FromMinutes(10.0))
            disposed <- true

    interface IDisposable with
        member this.Dispose() =
            dispose()
            GC.SuppressFinalize(this)

    // tear down the test environment 
    override x.Finalize() = 
        dispose()

    /// The cluster for test
    member x.Cluster with get() = testCluster

    /// Is cluster a remote cluster?
    member x.IsRemoteCluster with get() = useRemoteCluster

    /// Start time of the testing environment
    member x.StartTime with get() = envStartTime

    /// The test environment
    static member Environment with get() = env
