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
        LocalCluster.fs
  
    Description: 
        A Prajna cluster that can be started at local machine

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Diagnostics
open System.IO
open System.Net.NetworkInformation
open System.Threading

open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

/// Represents a local cluster that all clients are started on the same machine
type internal LocalCluster (name, version, numClients, containerMode, clientPath, numJobPortsPerClient, portsRange) =
    let [<Literal>] machineIdStart = 10000
    do 
        let numPortsRequired = numClients + numJobPortsPerClient * numClients
        // portsRange is a tuple that specify the range of ports that can be usd by the local cluster
        let s,e = portsRange
        if e - s + 1 < numPortsRequired then failwith(sprintf "The specified range of ports [%i, %i] is not enough for starting %i clients" s e numClients)
    let clients =
        [| 
          for i in 0..(numClients-1) do
              let status = new ClientStatus()
              status.Name <- sprintf "localhost"
              status.ClientVersionInfo <- "v1"
              status.ProcessorCount <- 1
              status.MachineID <- (uint64)(machineIdStart + i)
              let startPort = (portsRange |> fst) + i * (numJobPortsPerClient + 1)
              status.InternalPort <- startPort
              status.JobPortMin <- startPort + 1
              status.JobPortMax <- startPort + numJobPortsPerClient
              yield new ClientInfo(status) 
        |]

    let clusterInfo =
        let c = new ClusterInfo()
        c.Version <- version
        c.Name <- "LocalCluster-" + name
        c.ListOfClients <- clients
        c

    let generateClientArgs (client : ClientInfo) =
        let dirLog = Path.Combine( [| DeploymentSettings.LogFolder; "LocalCluster"; name; "Client-" + ((string)client.MachinePort) |] )
        let args = [ "-mem"; "48000"; 
                     "-verbose"; ((int ) DeploymentSettings.LocalClusterTraceLevel).ToString();
                     "-dirlog"; dirLog;
                     "-port";  (string)client.MachinePort;
                     "-jobport"; sprintf "%i-%i" client.JobPortMin client.JobPortMax;
                     "-local"  ]
        let allArgs = if containerMode = LocalClusterContainerMode.AppDomain then "-allowad"::args else args
        Array.ofList allArgs    

    // Start client
    // If client path is not given, start the client as appdomain, otherwise, start the client as a single process
    let startClient clientPath (client : ClientInfo) = 
        match clientPath with
        | Some(p) ->  // Start client as process
                      if containerMode = LocalClusterContainerMode.AppDomain then
                          // container will be started as appdomain as part of the daemon
                          // need to merge the configuration of the app to the client's configuration
                          let appAsmBinding = ConfigurationUtils.GetAssemblyBindingsForCurrentExe()
                          appAsmBinding |> ConfigurationUtils.ReplaceAssemblyBindingsForExeIfNeeded p
                      use proc = new Process()
                      proc.StartInfo.RedirectStandardOutput <- true
                      proc.StartInfo.UseShellExecute <- false
                      proc.StartInfo.CreateNoWindow <- true
                      proc.StartInfo.FileName <- p
                      proc.StartInfo.Arguments <- String.Join(" ", (generateClientArgs client))
                      proc.Start() |> ignore
        | None -> ClientLauncher.StartAppDomain client.MachineName (client.MachinePort) (generateClientArgs client)       
        if DeploymentSettings.RunningOnMono then
            // Mono Note: When we try to start two clients for a local culster as appdomains on Mono, 
            // We observe that sometimes one CreateDomain succeeds but the other fails due to Serialization exception. 
            // Haven't figured out the reason. For now, add a 5 second delay after one client is created. So the two clients
            // won't be created at the same time. This seems to reduce the chance of the aforementioned issue.
            Thread.Sleep(TimeSpan.FromSeconds(5.0))

    let shutdownEvent =  ClientLauncher.CreateShutdownEvent()

    // factory method
    static member Create (name, version, numClients, containerMode, clientPath, numJobPortsPerClient, portsRange) =
        let cl = new LocalCluster(name, version, numClients, containerMode, clientPath, numJobPortsPerClient, portsRange) 
        cl.Start()
        cl :> ILocalCluster

    member val ContainerMode = containerMode with get
    member val Clients = clients with get

    member val private nShutDownCalled = ref 0 with get
    member val private EvCloseExecuted = new ManualResetEvent(false) with get

    /// Start the cluster that each client is launched via a process using specified client
    member x.Start () =
        let sem = ClientLauncher.CreateReadySemaphore(x.Clients.Length)
        x.Clients |> Array.iter (startClient clientPath)
        for i in 1..(x.Clients.Length) do
            sem.WaitOne() |> ignore
        sem.Close()
        let stopObject = CleanUp.Current.Register( 2500, x, x.InternalStop, fun _ -> sprintf "Local Cluster #%d" numClients ) 
        ()

    /// Actual Shutdown the cluster by terminating all clients
    /// Externally, if need to shutdown the cluser, you should call stopObject.CleanUpThisOnly, or dispose the x object. 
    member private x.InternalStop () = 
        if Interlocked.CompareExchange( x.nShutDownCalled, 1, 0 ) = 0 then 
            let sem = ClientLauncher.CreateShutdownCompletionSemaphore(x.Clients.Length)
    
            // signal the client
            if not (shutdownEvent.Set()) then failwith ("cannot set the shutdown event")
    
            // wait for the client to complete
            for i in 1..(x.Clients.Length) do
                sem.WaitOne() |> ignore
            sem.Close()        
            shutdownEvent.Close()
            x.EvCloseExecuted.Set() |> ignore 
        x.EvCloseExecuted.WaitOne() |> ignore 

    interface ILocalCluster with
        /// Get the cluster information
        member val ClusterInfo = clusterInfo with get
    override x.Finalize() =
        // Allow the cluster to be individually closed. 
        CleanUp.Current.CleanUpOneObject( x )
    interface IDisposable with
        // Allow the cluster to be individually closed. 
        member x.Dispose() = 
            CleanUp.Current.CleanUpOneObject( x )
            GC.SuppressFinalize(x)
