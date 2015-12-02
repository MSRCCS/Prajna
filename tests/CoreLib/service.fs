namespace Prajna.Core.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading

open NUnit.Framework

open Prajna.Core
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Service.FSharp

type PrajnaTestInstance() = 
    inherit WorkerRoleInstance()
    let mutable bIsRunning = true
    override x.OnStartByObject(o) = 
        true
    /// bool OnStop(): Run once to stop all thread, 
    override x.OnStop() = 
        ()
    /// void Run(): main entry point when the service is running,
    override x.Run() = 
        bIsRunning <- false
    /// bool IsRunning(): return true if the service is running (should be set at OnStart), false if the service terminates.
    override x.IsRunning() = 
        bIsRunning

module Helper =
    let RemoteFunc guid = 
        let remoteObj = new PrajnaTestInstance()
        let fName = Path.Combine(Path.GetTempPath(), guid + "-" + Guid.NewGuid().ToString("D"))
        let fs = File.Create(fName)
        fs.Close()
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Hello, Remote function with %s has been executed and created file %s #####" guid fName))
        remoteObj :> WorkerRoleInstance

[<TestFixture(Description = "Tests for service")>]
type CoreServiceTests () =
    inherit Prajna.Test.Common.Tester()

    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize   

    let sw = Diagnostics.Stopwatch()

    [<Test(Description = "Start a service remotely")>]
    member x.PrajnaInstanceStart() =
        if Prajna.Test.Common.TestEnvironment.Environment.Value.IsRemoteCluster then
            // This test current is not designed to be ran on remote cluster
            ()
        else
            let guid = Guid.NewGuid().ToString("D")
            let param = WorkerRoleInstanceStartParam()
            RemoteInstance.Start<_,_>( cluster, ("service " + guid), param, (fun _ -> Helper.RemoteFunc guid) )

            let start = DateTime.UtcNow
            let mutable cont = true
            while cont do 
                let stop = DateTime.UtcNow
                let span = stop - start
                if span > TimeSpan.FromMinutes(1.0) then
                    Assert.Fail("Cannot Find the expected files within 1 minute")
                else
                    let files = Directory.GetFiles(Path.GetTempPath(), guid + "-*" )
                    if not (Array.isEmpty files) && files.Length = clusterSize then
                        files |> Array.iter (fun f -> try File.Delete(f) with |_ -> ())
                        cont <- false
                    else
                        Thread.Sleep(TimeSpan.FromSeconds(1.0))
