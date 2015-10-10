namespace Prajna.Test.Common

open System
open System.IO
open System.Diagnostics

open Prajna.Core
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Service.FSharp

// The test environment
type TestEnvironment private () = 

    static let useAppDomainForDaemonsAndContainers = true
    static let clusterSize = 2

    static let env = lazy(let e = new TestEnvironment()
                          AppDomain.CurrentDomain.DomainUnload.Add(fun _ -> (e :> IDisposable).Dispose())
                          e)


    do
        Environment.Init()
        let logdir = Path.Combine ([| DeploymentSettings.LocalFolder; "Log"; "UnitTest" |])
        let fileLog = Path.Combine( logdir, "UnitTestApp_" + StringTools.UtcNowToString() + ".log" )
        let args = [| "-verbose"; "5"; 
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
        // GC.Collect()
        // GC.WaitForPendingFinalizers()
        let proc = Process.GetCurrentProcess()
        Logger.Log(LogLevel.Info, 
                   sprintf "%s -- # of TH: %i, GC Heap: %f MB, Private Memory: %f MB" 
                       msg (proc.Threads.Count) ((float (GC.GetTotalMemory(false))) / 1e6) (float proc.PrivateMemorySize64 / 1e6))

    let localCluster =
        Logger.Log( LogLevel.Info, "##### Setup LocalCluster for tests starts.... #####")
        reportProcessStatistics("Before local cluster is created")
        let sw = Stopwatch()
        sw.Start()
        DeploymentSettings.LocalClusterTraceLevel <- LogLevel.MediumVerbose
        // Sometimes the AppVeyor build VM is really slow on IO and need more time to establish the container
        DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit <- 120L
        let cl =
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
            CacheService.Stop(localCluster)
            reportProcessStatistics("After closing containers")
            Prajna.Core.Environment.Cleanup()
            sw.Stop()
            reportProcessStatistics("After environment cleanup")
            Logger.Log( LogLevel.Info, (sprintf "##### Dispose test environment ..... completed ##### (%i ms)" (sw.ElapsedMilliseconds)))             
            disposed <- true

    interface IDisposable with
        member this.Dispose() =
            dispose()
            GC.SuppressFinalize(this)

    // tear down the test environment 
    override x.Finalize() = 
        dispose()

    /// The local cluster for test
    member x.LocalCluster with get() = localCluster

    /// The test environment
    static member Environment with get() = env
