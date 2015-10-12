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
        detailed config.fs
  
    Description: 
        Portion of the configuration that takes long time to run, separate it out so that 
    it won't get initialized. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Net
open System.Collections.Generic
open System.IO
open System.Diagnostics
open System.Security.Cryptography
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools



/// <summary> 
/// Detailed Machine configuration. The detailed config takes more time to execute (e.g., using system memory object, and initialization takes around 500ms, 
/// The code should not be executed at client for job
/// !!! Wish !!! For F#, please only instantiated the static object when the object is used, rather than initiated all objects in the file. 
/// </summary>
type internal DetailedConfig() = 
    inherit Config()
    static do Logger.Log( LogLevel.MildVerbose, "start to get detailed config information" )
    static let storageSpace = 
        let totalFreeSpace = ref 0L
        let list = [ for drive in DriveInfo.GetDrives() do
                        if drive.IsReady then 
                            let name = drive.Name
                            let freeSpace = drive.AvailableFreeSpace
                            totalFreeSpace := !totalFreeSpace + freeSpace
                            yield (name, freeSpace)
                   ]
        (  totalFreeSpace, list )

    static let memSpace = 
        if not DeploymentSettings.RunningOnMono then
            let winQuery = System.Management.ObjectQuery("select * from Win32_PhysicalMemory")
            use searcher = new System.Management.ManagementObjectSearcher(winQuery)
            let mutable totalCapacity = 0UL
            try
                for item in searcher.Get() do
                    let cp = item.GetPropertyValue("Capacity")
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "RAM Capacity ==== %AB" cp ))
                    totalCapacity <- totalCapacity + Convert.ToUInt64( cp )
                    item.Dispose()
            with
            | _ -> ()
            totalCapacity
        else
            use pc = new PerformanceCounter ("Mono Memory", "Total Physical Memory");
            uint64 (pc.RawValue)

    static let storageCapacity = 
        let totalCapacity = ref 0L
        let list = [ for drive in DriveInfo.GetDrives() do
                        if drive.IsReady then 
                            let name = drive.Name
                            let capacity = drive.TotalSize
                            totalCapacity := !totalCapacity + capacity
                            yield (name, capacity)
                   ]
        (  totalCapacity, list )

    static let sMAC = 
        let list = [ for nic in Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces() do
                        if nic.OperationalStatus = Net.NetworkInformation.OperationalStatus.Up then
                            yield nic.GetPhysicalAddress()
                   ]
        list

    static let generatedMachineId = lazy (
        let mutable machineId = 0L
        use sha = new SHA256Managed()
        let nics = NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
        //    Array.Sort( nics, { new IComparer<NetworkInformation.NetworkInterface> with member this.Compare(x, y) = x.GetPhysicalAddress.ToString().CompareTo( y.GetPhysicalAddress.ToString()) } )
        for nic in nics do
            let addr = nic.GetPhysicalAddress()
            let bt = addr.GetAddressBytes()
            sha.TransformBlock( bt, 0, bt.Length, bt, 0 ) |> ignore
        let bt1 = Array.zeroCreate<byte> 0
        sha.TransformFinalBlock( bt1, 0, 0 ) |> ignore
        let result = sha.Hash
        machineId <- BitConverter.ToInt64( result, 0 )
        if not DeploymentSettings.RunningOnMono then
            if machineId = 0L then 
                use mc = new System.Management.ManagementClass("win32_processor")
                let moc = mc.GetInstances()
                for mo in moc do
                    if machineId = 0L then 
                        let v = mo.GetPropertyValue("ProcessorID")
                        match v with 
                        | :? string as s -> machineId <- Int64.Parse( s, System.Globalization.NumberStyles.HexNumber)
                        | _ -> ()
        else
            // Mono Note: figure out what to do
            ()
        machineId)

    static let machineID = 
            let mutable curMachineID = 0L
            try
                curMachineID <- ArgumentParser.GetRegistryKeyInt64 "PrajnaMachineID" 0L
                if curMachineID = 0L then 
                    curMachineID <- generatedMachineId.Value
                    ArgumentParser.SetRegistryKeyInt64 "PrajnaMachineID" curMachineID
                    curMachineID <- ArgumentParser.GetRegistryKeyInt64 "PrajnaMachineID" 0L
                if curMachineID = 0L then 
                    Logger.Fail(sprintf "Fail to set register PrajnaMachineID" )
            with
                | _ -> ()          

            // Fail to get the machineId via registry, let's try file now
            if curMachineID = 0L then     
                let path = Path.Combine(DeploymentSettings.LocalFolder, DeploymentSettings.MachineIdFile)
                Logger.Log( LogLevel.Info, ( sprintf "Attempt to get PrajnaMachineId from %s" path ))
                curMachineID <- if File.Exists path 
                                then Int64.Parse(File.ReadAllText path)
                                else 
                                    Logger.Log( LogLevel.Info, ( sprintf "%s does not exist, create one" path ))
                                    let id = generatedMachineId.Value                    
                                    File.WriteAllText(path, string id)
                                    id
            Logger.Log( LogLevel.Info, ( sprintf "PrajnaMachineId is %x" curMachineID ))
            (uint64) curMachineID

    static let endString = 
        Logger.Log( LogLevel.MildVerbose, ("end to get detailed config information"))
    
    static member GetMachineID with get() = machineID
    static member GetMemorySpace with get() = memSpace

    member val ProcessorCount = Environment.ProcessorCount with get

    member val MemorySpace = memSpace with get

    member val StorageSpace = storageSpace with get

    member val StorageCapacity = storageCapacity with get

    member val MAC = sMAC with get

    member val MachineID = machineID with get

    member x.DriveSpace( drive:string  ) = 
        let tFreeSpace, driveList = x.StorageSpace
        let mutable dName = ""
        let mutable freeSpace = 0L
        for (dr, nSpace) in driveList do
            // if ( drive.Length>0 && String.Compare( dr.Substring(0, drive.Length), drive, true )=0 ) || ( drive.Length=0 && dr.[0]<>'c' && dr.[0]<>'C' ) then 
            if ( drive.Length>0 && dr.IndexOf( drive, StringComparison.OrdinalIgnoreCase)>=0 ) || ( drive.Length=0 && DeploymentSettings.IsDataDrive(dr) ) then 
                dName <- drive
                freeSpace <- freeSpace + nSpace
        freeSpace

    member x.DriveCapacity( drive:string  ) = 
        let tCapacity, driveList = x.StorageCapacity
        let mutable dName = ""
        let mutable capacity = 0L
        for (dr, nCapacity ) in driveList do
            // if ( drive.Length>0 && String.Compare( dr.Substring(0, drive.Length), drive, true )=0 ) || ( drive.Length=0 && dr.[0]<>'c' && dr.[0]<>'C' ) then 
            if ( drive.Length>0 && dr.IndexOf( drive, StringComparison.OrdinalIgnoreCase)>=0 ) || ( drive.Length=0 && DeploymentSettings.IsDataDrive(dr) ) then 
                dName <- drive
                capacity <- capacity + nCapacity
        capacity

