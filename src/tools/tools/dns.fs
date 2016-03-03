(*---------------------------------------------------------------------------
	Copyright 2014 Microsoft

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
		dns.fs
  
	Description: 
		Help

	Author:																	
 		Jin Li, Partner Research Manager
        Sanjeev Mehrotra, Princial Architect
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Sept. 2014
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools.Network

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

// I find BeginGetHostEntry() and EndGetHostEntry() has a latency of 70s!!! in AppDomain, which leads to unacceptable performance degradation. 
// In light of the discovery, a local DNS Cache is created to reduce the latency of the resolve. 
// We want to show host information in trace, but using DNS is too slow. LocalDNS class caches the DNS information, and allow quick look up
/// <summary> 
/// DNS helper functions. Cannot instantiate class, only contains static members.
/// </summary>
[<AbstractClass>]
type LocalDNS() = 
    static member val private HostNameToIP = ConcurrentDictionary<_,_>(StringComparer.OrdinalIgnoreCase) with get
    static member val private IPToHostName = ConcurrentDictionary<_,_>() with get

    /// Add a DNS entry of hostname <-> ipAddresses
    /// <param name="hostname">The hostname to add to the cache</param>
    /// <param name="ipAddresses">The IP addresses corresponding to the hostname being added</param>
    static member AddEntry( hostname:string, ipAddresses:byte[][] ) = 
        let uhostname = hostname.ToUpper() |> String.Copy
        let copyIPAddress = Array.init ipAddresses.Length ( fun i -> Array.copy ipAddresses.[i] )
        LocalDNS.HostNameToIP.Item( uhostname ) <- copyIPAddress
        for ip in copyIPAddress do 
            let ipNum = BitConverter.ToUInt32( ip, 0 )
            LocalDNS.IPToHostName.Item( ipNum ) <- uhostname
    /// Monitor DNS host resolution
    static member val TraceLevelMonitorDNSResolution = LogLevel.ExtremeVerbose with get, set
    /// Similar to Dns.GetHostAddresses, but only use IPv4 addresses, and caches its result.\
    /// <param name="hostname">The hostname to resolve</param>
    /// <param name="bTryResolve">
    /// Optional argument (default is true).
    /// Also, if bTryResolve is false, the function only attemps to retrieve the DNS entry in cache. 
    /// If bTryResolve is false and the DNS entry doesn't exist, null is returned for addresses.
    /// </param>
    /// <returns>An array of host addresses, where each address is array of bytes</returns>
    static member GetHostAddresses( hostname:string, ?bTryResolve:bool ) = 
        let tryResolve = defaultArg bTryResolve true
        let uhostname = hostname.ToUpper()
        let bExst, addrArray = LocalDNS.HostNameToIP.TryGetValue( uhostname ) 
        if bExst then 
            if Utils.IsNotNull ( addrArray ) then 
                addrArray |> Array.map( fun addr -> IPAddress(addr) ) 
            else
                null   
        elif tryResolve then 
            let t1 = (PerfDateTime.UtcNow())
            let addr = Dns.GetHostAddresses( uhostname )
            let addrBytes = addr |> Array.filter ( fun addr -> addr.AddressFamily = Net.Sockets.AddressFamily.InterNetwork ) 
                                 |> Array.map ( fun addr -> addr.GetAddressBytes() )
            Logger.LogF( LocalDNS.TraceLevelMonitorDNSResolution, 
                                               ( fun _ -> let elapse = (PerfDateTime.UtcNow()).Subtract( t1 ).TotalMilliseconds
                                                          sprintf "Calling Dns.GetHostAddresses, resolve hostname %s in %f ms to %A" hostname elapse addrBytes))
            LocalDNS.AddEntry( hostname, addrBytes )
            addrBytes |> Array.map( fun addr -> IPAddress(addr) )
        else
            null
    
    /// Similar to Dns.GetHostEntry, but cache its result. 
    /// <param name="addr">The ipaddress as array of bytes</param>
    /// <param name="bTryResolve">
    /// Optional argument (default is true).
    /// Also, if bTryResolve is false, the function only attemps to retrieve the DNS entry in cache. 
    /// If bTryResolve is false and the DNS entry doesn't exist, null is returned for hostname. 
    /// </param>
    /// <returns>A string representing the host address</returns>
    static member GetHostByAddress( addr, ?bTryResolve ) = 
        let tryResolve = defaultArg bTryResolve true
        let addrNum = BitConverter.ToUInt32( addr, 0 )
        if LocalDNS.IPToHostName.ContainsKey( addrNum ) then 
            LocalDNS.IPToHostName.Item( addrNum ) 
        elif tryResolve then 
            let t1 = (PerfDateTime.UtcNow())
            let entry = Dns.GetHostEntry( IPAddress( addr) )
            let hostname = entry.HostName.ToUpper()
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> let elapse = (PerfDateTime.UtcNow()).Subtract( t1 ).TotalMilliseconds
                                                          sprintf "Calling Dns.GetHostEntry, resolve address %A in %f ms" (IPAddress(addr))  elapse ))
            LocalDNS.AddEntry( hostname, Array.create 1 addr )
            hostname
        else 
            null

    /// Return a random IPv4 address of hostname. 
    /// <param name="hostname">The hostname</param>
    /// <param name="bTryResolve">
    /// Optional argument - default is true
    /// If bTryResolve is false, the function only attemps to retrieve the DNS entry in cache. 
    /// If bTryResolve is false and the DNS entry doesn't exist, null is returned for addresses. 
    /// </param>
    /// <returns>A random IPv4 address of the hostname</returns>
    static member GetAnyIPAddress( hostname:string, ?bTryResolve:bool ) =
        let tryResolve = defaultArg bTryResolve true
        let addr = LocalDNS.GetHostAddresses( hostname, tryResolve )
        if Utils.IsNotNull addr && addr.Length >0 then 
            let rnd = RandomWithSalt( hostname.GetHashCode() ) 
            addr.[ rnd.Next( addr.Length ) ]
        else
            null
    /// Show host information of an endpoint ep. If the hostname is not in DNS cache, IP address and port information is shown. 
    /// <param name="ep">The endpoint</param>
    /// <returns>A string for display purposes</returns>
    static member GetShowInfo( ep:EndPoint ) = 
        //ep.ToString()
        if Utils.IsNull ep then 
            "null"
        else
            let eip = ep :?> IPEndPoint
            let name = LocalDNS.GetHostByAddress( eip.Address.GetAddressBytes(), false )
            let showinfo = if Utils.IsNull name then eip.ToString() else name + ":" + eip.Port.ToString()
            showinfo
    /// Attempt to retrieve the hostname for IPAddress addr. If the DNS entry does not exist, the function returns IP addresses information string as x.x.x.x 
    static member GetNameForIPAddress( addr: IPAddress ) = 
        let name = LocalDNS.GetHostByAddress( addr.GetAddressBytes(), false )
        if Utils.IsNull name then addr.ToString() else name 
    /// Map an IPv4 address to a 48bit signature of type int64, with port being the lowest 16 bit, and IPv4 address takes 17 to 48 bit. 
    static member IPv4AddrToInt64( addr: IPAddress, port: int ) = 
        ( int64 port) + ((int64 (BitConverter.ToUInt32( addr.GetAddressBytes(), 0 )) )<<<16) 
    /// Map an IPv4 endpoint to a 48bit signature of type int64, with port being the lowest 16 bit, and IPv4 address takes 17 to 48 bit. 
    static member IPEndPointToInt64( ep: IPEndPoint ) = 
        ( int64 ep.Port) + ((int64 (BitConverter.ToUInt32( ep.Address.GetAddressBytes(), 0 )) )<<<16) 
    /// Convert a 48bit signature of type int64 back to an IPv4 endpoint, with port being the lowest 16 bit, and IPv4 address takes 17 to 48 bit. 
    static member Int64ToIPEndPoint( addrSignature:int64 ) = 
        let port = int ( addrSignature &&& 0xffffL )
        let addr = ( addrSignature &&& 0xffffffff0000L ) >>> 16
        IPEndPoint( addr, port )
    /// Attempt to retrieve the hostname of a 48bit signature of type int64, with port being the lowest 16 bit, and IPv4 address takes 17 to 48 bit. 
    /// If the DNS entry doesn't exist in cache, the hostname information is shown as x.x.x.x
    static member GetHostInfoInt64(  addrSignature:int64 ) = 
        let addr64 = ( addrSignature &&& 0xffffffff0000L ) >>> 16
        let addr = IPAddress( addr64 )
        let name = LocalDNS.GetHostByAddress( addr.GetAddressBytes(), false )
        if Utils.IsNull name then addr.ToString() else name 

    /// Begin To resolve DNS
    static member internal BeginDNSResolve(machineName, callback : (IPAddress*bool*obj)->unit, state : obj) : bool =
        try
            let addr = LocalDNS.GetAnyIPAddress( machineName, false )
            if Utils.IsNull addr then
                Dns.BeginGetHostEntry( machineName, AsyncCallback(LocalDNS.EndDNSResolve), (machineName, callback, state) ) |> ignore
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "BeginGetHostEntry to host %s" machineName ))
            else
                callback(addr, true, state)
            true
        with 
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Begin DNS Resolve of host %s failed" machineName ))
            false
    /// End resolve DNS
    static member internal EndDNSResolve( ar ) =
        let (machineName, callback, state) = ar.AsyncState :?> string*((IPAddress*bool*obj)->unit)*obj
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "EndDNSResolve to host %s" machineName  ))
        let bCallbackCalled = ref false
        try
            let ipv4Addr = List<_>(2)
            let hostEntry = Dns.EndGetHostEntry( ar )
            for addr in hostEntry.AddressList do
                if addr.AddressFamily = AddressFamily.InterNetwork then 
                    ipv4Addr.Add( addr )
            if ipv4Addr.Count = 0 then
                failwith (sprintf "No IPv4 interface for host %s" machineName )
            else
                let useIPv4Addr = 
                    if ipv4Addr.Count=1 then 
                        ipv4Addr.[0] 
                    else
                        let rnd = RandomWithSalt( machineName.GetHashCode() )
                        ipv4Addr.[ rnd.Next( ipv4Addr.Count ) ]   
                LocalDNS.AddEntry( machineName, ipv4Addr.ToArray() |> Array.map( fun addr -> addr.GetAddressBytes() ) )
                bCallbackCalled := true
                callback(useIPv4Addr, true, state)
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "End DNS Resolve of host %s failed" machineName ))
            if (not !bCallbackCalled) then
                callback(null, false, state)
