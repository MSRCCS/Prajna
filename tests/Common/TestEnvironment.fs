namespace Prajna.Test.Common

open System
open System.IO

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
        let dirLog = Path.Combine ( [| DeploymentSettings.LocalFolder; "Log"; "UnitTest" |])
        let fileLog = Path.Combine( dirLog, "UnitTestApp_" + StringTools.UtcNowToString() + ".log" )
        let args = [| "-verbose"; "4"; 
                       "-log"; fileLog |]
        let dirInfo= FileTools.DirectoryInfoCreateIfNotExists dirLog
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

    let localCluster =
        Logger.Log( LogLevel.Info, "##### Setup LocalCluster for tests starts.... #####")
        //DeploymentSettings.LocalClusterTraceLevel <- LogLevel.WildVerbose
        let cl =
            if useAppDomainForDaemonsAndContainers then
                Cluster(sprintf "local[%i]" clusterSize)
            else
                let localClusterCfg = { Name = sprintf "LocalPP-%i" clusterSize
                                        Version = (DateTime.UtcNow)
                                        NumClients = clusterSize
                                        ContainerInAppDomain = false
                                        ClientPath = "PrajnaClient.exe" |> Some // Note: put the path of PrajnaClient here
                                        NumJobPortsPerClient = 5
                                        PortsRange = (20000, 20011)
                                      }
                Cluster(localClusterCfg)

        CacheService.Start(cl)
        Logger.Log( LogLevel.Info, "##### Setup LocalCluster for tests .... completed #####")        
        cl

    let completed = 
        Logger.Log( LogLevel.Info, "##### Setup test environment .... completed #####")
        true

    let mutable disposed = false
    let dispose () =
        if not disposed then
            Logger.Log( LogLevel.Info, "##### Dispose test environment starts ..... #####") 
            CacheService.Stop(localCluster)
            Prajna.Core.Environment.Cleanup()
            Logger.Log( LogLevel.Info, "##### Dispose test environment ..... completed #####")             
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
