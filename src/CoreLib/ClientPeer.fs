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
        ClientPeer.fs
  
    Description: 
        Code for client peers, mainly on class DSetPeer

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Generic
open System.Net
open System.Threading
open System.Diagnostics
open System.IO
open System.Net.Sockets
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.Network
open System.Security.Cryptography

/// A Prajna CommandQueue that is resulted from accept() 
[<AllowNullLiteral>]
type NetworkCommandQueuePeer internal ( socket, onet ) = 
    inherit NetworkCommandQueue( socket, onet )
    member val internal BlockOnClusterInfo = 0x01 with get
    member val internal BlockOn = 0 with get, set
    member val internal SetDSetMSG: StreamBase<byte> = null with get, set
    member val internal CallOnClose = List<OnPeerClose>() with get
    member val internal CloseCommandQueuePeerCalled = false with get, set
    /// Write DSet Metadata to disk
    member internal x.SetDSet( jobID, ?inputStream: StreamBase<byte> ) = 
        let ms = 
            match inputStream with 
            | Some ( stream ) -> if Utils.IsNotNull stream then stream else x.SetDSetMSG
            | None -> x.SetDSetMSG
        if Utils.IsNull ms then 
            /// Set Cluster is called, no DSet is pending to be resolved. 
            ( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.ClusterInfo ), null )
        else
            // Replicate the whole stream so that it can be read again, don't forget to copy the pointer. 
            //let readStream = new MemStream( ms.GetBuffer(), 0, int ms.Length, false, true )
            let readStream = new MemStream()
            readStream.AppendNoCopy(ms, 0L, ms.Length)
            readStream.Seek( ms.Position, SeekOrigin.Begin ) |> ignore
            let dsetOption, errMsg, msSend = DSetPeer.Unpack( readStream, true, x, jobID )
            match errMsg with 
            | ClientBlockingOn.Cluster ->
                // Cluster Information can't be parsed, Wait for cluster information. 
                x.SetDSetMSG <- ms 
                x.BlockOn <- x.BlockOnClusterInfo
                ( ControllerCommand( ControllerVerb.Get, ControllerNoun.ClusterInfo ), msSend )
            | ClientBlockingOn.None ->
                // Unblocking
                x.BlockOn <- 0
                let curDSet = Option.get( dsetOption )
                let newDSet = DSetPeerFactory.CacheDSetPeer( curDSet )
                newDSet.Setup()
            | _ ->
                // Error, fail to set DSet
                let retCmd = ControllerCommand( ControllerVerb.Error, ControllerNoun.Message )
                ( retCmd, msSend )
    /// Update DSet meta data, when the peer finished writing. 
    member internal x.UpdateDSet( jobID: Guid, ms: StreamBase<byte> ) = 
        // Replicate the whole stream so that it can be read again, don't forget to copy the pointer. 
        //let readStream = new MemStream( ms.GetBuffer(), 0, int ms.Length, false, true )
        let readStream = new MemStream()
        readStream.AppendNoCopy(ms, 0L, ms.Length)
        readStream.Seek( ms.Position, SeekOrigin.Begin ) |> ignore
        let dsetOption, errMsg, msSend = DSetPeer.Unpack( readStream, true, x, jobID )
        match errMsg with 
        | ClientBlockingOn.Cluster ->
            // Cluster Information can't be parsed, Wait for cluster information. 
            x.SetDSetMSG <- ms 
            x.BlockOn <- x.BlockOnClusterInfo
            ( ControllerCommand( ControllerVerb.Get, ControllerNoun.ClusterInfo ), msSend )
        | ClientBlockingOn.None ->
            // Unblocking
            x.BlockOn <- 0
            let curDSet = Option.get( dsetOption )
            if curDSet.ReadyStoreStreamArray() then 
                curDSet.WriteDSetMetadata()
                curDSet.CloseStorageProvider()
                DSetPeerFactory.CacheDSetPeer( curDSet ) |> ignore
            curDSet.Setup()
        | _ ->
            // Error, fail to set DSet
            let retCmd = ControllerCommand( ControllerVerb.Error, ControllerNoun.Message )
            ( retCmd, msSend )

    override x.Close() = 
        // Light weight lock, ensure ClosePrajnaCommandQueuePeer is only entered once. 
        let bCloseCalled = 
            lock ( x ) ( fun _ ->
                if not x.CloseCommandQueuePeerCalled then 
                    x.CloseCommandQueuePeerCalled <- true 
                    false
                else
                    true   
                )
        if not bCloseCalled then 
            for obj in x.CallOnClose do 
                (obj.CallbackOnClose)()
            x.CallOnClose.Clear()
            base.Close()

    override x.Finalize() =
        x.Close()

    interface IDisposable with
        member x.Dispose() = 
            x.Close()
            GC.SuppressFinalize(x)

type internal ClientConnections() = 
    let common = Cluster.Connects
    member x.AddPeerConnect( socket ) = 
        let newChannel = new NetworkCommandQueuePeer( socket, common )
        common.AddToCollection(newChannel)
        Logger.LogF( LogLevel.WildVerbose, (fun _ -> let ep = socket.RemoteEndPoint 
                                                     let eip = ep :?> IPEndPoint
                                                     sprintf "Add NetworkCommandQueuePeer accepted socket from %A with name %A" ep (LocalDNS.GetHostByAddress( eip.Address.GetAddressBytes(),false)  ) ))
        newChannel
    member x.Connects with get() = common
    member x.Initialize() = common.Initialize()
