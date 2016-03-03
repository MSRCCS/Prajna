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
        contracts.fs
  
    Description: 
        Contracts are functions being exported to/imported from a Prajna service. 
    Prajna service exports contracts as either Action<_>, Func<_,_> or FuncTask<_,_>. 
    The contracts can be imported by other service, a regular Prajna program, or 
    a data anlytical jobs. There is overhead in setting up task, but once setup, the 
    contract can be consumed efficiently, as a function call. 

    Message Used:
        Register, Contract:     Register a contract at daemon (no feedback on whether registration succeeds)
        Ready, Contract:        Registration status. 
        Error, Contract:        Certain service fails. 
        Get, Contract:          Lookfor a contract with a specific name 
        Set, Contract:          Returned a list of contracts with name, contract type, input and output type
        Close, Contract:        There is no valid contract for the name
        Request, Contract:      Initiate a call to a contract
        FailedRequest, Contract:    Error in servicing the request   
        Reply, Contract         Return a call to a contract
        FailedReply, Contract:      Error in deliverying the reply
    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Feb. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service
open System
open System.Threading
open System.IO
open System.Net
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Prajna.Tools
open Prajna.Tools.Network
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core

/// <summary>
/// Type of Contract being exported. 
/// </summary>
type internal ContractKind = 
    | UnknownContract = 0
    | Action = 1
    | Function = 2
    | FunctionTask = 2 // No difference between FunctionTask and Function for remoting, there are all implemented via callback anyway 
    | SeqFunction = 3

[<AllowNullLiteral>]
type internal ContractInfo() = 
    let mutable objTypeIn = null
    let mutable objTypeOut = null
    /// <summary> 
    /// Default timeout value for contract. If nothing happens during the interval, the contract will be removed from daemon
    /// </summary>
    static member val DefaultRequestTimeoutAtDaemonInMS = 60000 with get, set
    member val ContractType = ContractKind.UnknownContract with get, set
    member x.ObjTypeIn with get() = objTypeIn
                        and set( typeName ) = objTypeIn <- if StringTools.IsNullOrEmpty( typeName ) then null else typeName
    member x.ObjTypeOut with get() = objTypeOut
                         and set( typeName ) = objTypeOut <- if StringTools.IsNullOrEmpty( typeName ) then null else typeName 
    member val RequestTimeoutAtDaemonInMS = ContractInfo.DefaultRequestTimeoutAtDaemonInMS with get, set
    override x.ToString() = 
        sprintf "type %A (%s, %s)" x.ContractType x.ObjTypeIn x.ObjTypeOut
    member x.AllowMultiMatch() = 
        x.ContractType=ContractKind.SeqFunction || x.ContractType = ContractKind.Action
    member x.IsAggregable with get() = x.ContractType=ContractKind.SeqFunction
    member x.Pack( msInfo: MemStream ) = 
        msInfo.WriteVInt32( int x.ContractType ) 
        msInfo.WriteStringV( x.ObjTypeIn ) 
        msInfo.WriteStringV( x.ObjTypeOut ) 
        msInfo.WriteInt32( x.RequestTimeoutAtDaemonInMS )
    member x.Equals( y: ContractInfo ) = 
        x.ContractType = y.ContractType && 
            String.Compare( x.ObjTypeIn, y.ObjTypeIn, StringComparison.Ordinal ) = 0 && 
            String.Compare( x.ObjTypeOut, y.ObjTypeOut, StringComparison.Ordinal ) = 0 
    member x.MatchContract( queueSignature, arr: ContractInfo[], registerFunc ) = 
        let mutable nMatch = 0 
        let mutable nMissMatch = 0
        /// Always use null to represent empty string, so that type comparison can be meansingful. 
        for target in arr do 
                if x.Equals( target ) then 
                    nMatch <- nMatch + 1
                else
                    nMissMatch <- nMissMatch + 1
        if nMissMatch > 0 then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Import contract from %s fails as the contracts %A is of unmatch type with the contract of %A that is looking for" 
                                                       (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                       arr x ))
        elif nMatch > 1 && not (x.AllowMultiMatch()) then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Import contract from %s fails there are %d contracts, but contract of %A does not allow multimatch." 
                                                       (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                       arr.Length x ))
        elif nMatch >= 1 && not (Utils.IsNull registerFunc)  then 
            registerFunc( queueSignature )
    static member Unpack( ms: StreamBase<byte> ) = 
        let ty = enum<ContractKind>( ms.ReadVInt32() )
        let objTypeIn = ms.ReadStringV() 
        let objTypeOut = ms.ReadStringV()
        let timeoutAtDaemonInMS = ms.ReadInt32() 
        ContractInfo( ContractType=ty, ObjTypeIn=objTypeIn, ObjTypeOut=objTypeOut, RequestTimeoutAtDaemonInMS=timeoutAtDaemonInMS )
    static member PackWithName( name: string, x:ContractInfo, bReload ) = 
        let msInfo = new MemStream( 1024 )
        msInfo.WriteStringV( name ) 
        x.Pack( msInfo ) 
        msInfo.WriteBoolean( bReload ) 
        msInfo
    static member UnpackWithName( ms: StreamBase<byte> ) = 
        let name = ms.ReadStringV() 
        let x = ContractInfo.Unpack( ms ) 
        let bReload = ms.ReadBoolean()
        name, x, bReload


                                                            

type internal ContractStoreCommon() = 
    /// <summary>
    /// Attach Common Header for remote invokation. 
    /// </summary> 
    static member inline internal RemoteInvoke (name ) = 
        let msRemote = new MemStream()
        msRemote.WriteStringV( name ) 
        let opid = System.Guid.NewGuid()
        // Uniquely identify the Function, etc.. 
        msRemote.WriteBytes( opid.ToByteArray() )
        msRemote, opid
    /// <summary>
    /// Action parsing to invoke an action remotely. 
    /// </summary> 
    static member internal RemoteActionWrapper<'T> (name) (sendFunc) = 
        Action<'T>( 
            fun param ->    
                // No need to 
                let msAction, _ = ContractStoreCommon.RemoteInvoke( name ) 
                msAction.SerializeFromWithTypeName( param )
                sendFunc( msAction )
       )
    /// <summary>
    /// Action parsing when an action is invoked remotely. 
    /// </summary> 
    static member internal ParseRemoteAction<'T> (act:Action<'T>) (queueSignature:int64, ms:StreamBase<byte> ) = 
        let reqID = ms.ReadGuid() 
        let errorMsg = ref null 
        let obj = ms.DeserializeObjectWithTypeName()
        match obj with 
        | :? 'T as param -> 
            act.Invoke( param ) 
        | _ -> 
            errorMsg := sprintf "Fail to execute remote Action as parameter %A is not of type %s" obj (typeof<'T>.FullName )
        let queue = Cluster.Connects.LookforConnectBySignature( queueSignature )
        let bSuccess =  Utils.IsNull !errorMsg
        if not ( Utils.IsNull queue ) && queue.CanSend then 
            if bSuccess then 
                use msFeedback = new MemStream( 64 )
                msFeedback.WriteGuid( reqID ) 
                queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msFeedback )
            else
                use msError = new MemStream( (!errorMsg).Length * 2 + 16)
                msError.WriteGuid( reqID ) 
                msError.WriteString( !errorMsg ) 
                queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
        if not bSuccess then 
            Logger.Log( LogLevel.MildVerbose, !errorMsg )

    /// <summary>
    /// Parsing when a remote program call a function that returns IEnumerable Result
    /// </summary> 
    static member internal ParseRemoteSeqFunction<'T, 'TResult> (serializationLimit:int) (func:Func<'T,IEnumerable<'TResult>>) (queueSignature:int64, ms:StreamBase<byte> ) = 
        let reqID = ms.ReadGuid()
        let errorMsg = ref null 
        if true then 
            let obj = ms.DeserializeObjectWithTypeName()
            let en = 
                if Utils.IsNull obj then 
                    func.Invoke( Unchecked.defaultof<_> ).GetEnumerator()
                else
                    match obj with 
                    | :? 'T as param -> 
                        func.Invoke( param ).GetEnumerator()
                    | _ -> 
                        errorMsg := sprintf "Fail to execute remote SeqFunction as parameter %A is not of type %s" obj (typeof<'T>.FullName )
                        null
            if Utils.IsNotNull en then 
                let sLimit, lst = 
                    if serializationLimit>0 && serializationLimit< Int32.MaxValue then 
                        serializationLimit, List<_>(serializationLimit)  
                    else 
                        Int32.MaxValue, List<_>()
                let mutable bStop = false
                while not bStop && en.MoveNext() do
                    lst.Add( en.Current )
                    if lst.Count >= sLimit then 
                        /// Send partial result back to server. 
                        let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
                        if not ( Utils.IsNull queue ) && queue.CanSend then 
                            let arr = lst.ToArray()
                            use msReply = new MemStream() 
                            msReply.WriteGuid( reqID )
                            msReply.SerializeObjectWithTypeName( arr )
                            Logger.LogF( DeploymentSettings.TraceLevelSeqFunction, ( fun _ -> sprintf "ParseRemoteSeqFunction: Reply, Contract to send %d count of %s as reply for req %A (%dB)"
                                                                                                arr.Length
                                                                                                typeof<'TResult>.Name
                                                                                                reqID msReply.Length ))
                            queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msReply )
                            lst.Clear()
                        else
                            bStop <- true
                let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
                if not ( Utils.IsNull queue ) && queue.CanSend then 
                    if lst.Count > 0 then 
                        let arr = lst.ToArray()
                        use msReply = new MemStream() 
                        msReply.WriteGuid( reqID )
                        msReply.SerializeObjectWithTypeName( arr )
                        Logger.LogF( DeploymentSettings.TraceLevelSeqFunction, ( fun _ -> sprintf "ParseRemoteSeqFunction: Reply, Contract to send FINAL %d count of %s (+null) as reply for req %A (%dB)"
                                                                                            arr.Length
                                                                                            typeof<'TResult>.Name
                                                                                            reqID msReply.Length ))
                        queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msReply )
                        lst.Clear()
                    else
                        Logger.LogF( DeploymentSettings.TraceLevelSeqFunction, ( fun _ -> sprintf "ParseRemoteSeqFunction: Reply, Contract to send final (null) of %s as reply for req %A "
                                                                                            typeof<'TResult>.Name
                                                                                            reqID ))
                        
                    /// Signal the end of the reply 
                    use msEnd = new MemStream() 
                    msEnd.WriteGuid( reqID )
                    msEnd.SerializeObjectWithTypeName( null )
                    queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msEnd )
        if not (Utils.IsNull !errorMsg) then 
            Logger.Log( LogLevel.MildVerbose, !errorMsg )
            let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
            if not ( Utils.IsNull queue ) && queue.CanSend then 
                    use msError = new MemStream( (!errorMsg).Length * 2 + 20 )
                    msError.WriteGuid( reqID )
                    msError.WriteString( !errorMsg ) 
                    queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
    /// <summary>
    /// Parsing when a remote program call a function that returns IEnumerable Result
    /// </summary> 
    static member internal ParseRemoteFunction<'T, 'TResult> (func:Func<'T,'TResult>) (queueSignature:int64, ms:StreamBase<byte> ) = 
        let reqID = ms.ReadGuid()
        let errorMsg = ref null 
        let resultRef = ref Unchecked.defaultof<_>
        if true then 
            let obj = ms.DeserializeObjectWithTypeName()
            if Utils.IsNull obj then 
                resultRef := func.Invoke( Unchecked.defaultof<_> )
            else
                match obj with 
                | :? 'T as param -> 
                    resultRef := func.Invoke( param )
                | _ -> 
                    errorMsg := sprintf "Fail to execute remote SeqFunction as parameter %A is not of type %s" obj (typeof<'T>.FullName )
        let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
        let bSuccess = Utils.IsNull !errorMsg
        if not ( Utils.IsNull queue ) && queue.CanSend then 
            if bSuccess then 
                use msReply = new MemStream( 64 )
                msReply.WriteGuid( reqID ) 
                msReply.SerializeObjectWithTypeName( !resultRef )
                queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msReply )
            else
                use msError = new MemStream( (!errorMsg).Length * 2 + 20 )
                msError.WriteGuid( reqID ) 
                msError.WriteString( !errorMsg ) 
                queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
        if not bSuccess then 
            Logger.Log( LogLevel.MildVerbose, !errorMsg )
    /// <summary>
    /// Parsing when a remote program call a function that returns IEnumerable Result
    /// </summary> 
    static member internal ParseRemoteFunctionTask<'T, 'TResult> (func:Func<'T,Task<'TResult>>) (queueSignature:int64, ms:StreamBase<byte> ) = 
        let reqID = ms.ReadGuid()
        let errorMsg = ref null 
        if true then 
            let obj = ms.DeserializeObjectWithTypeName()
            let ta = 
                if Utils.IsNull obj then 
                    func.Invoke( Unchecked.defaultof<_> )
                else
                    match obj with 
                    | :? 'T as param -> 
                        func.Invoke( param )
                    | _ -> 
                        errorMsg := sprintf "Fail to execute remote Task as parameter %A is not of type %s" obj (typeof<'T>.FullName )
                        null
            if Utils.IsNotNull ta then 
                let sendResult (reply:Task<'TResult>) = 
                    /// Send partial result back to server. 
                    let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
                    if not ( Utils.IsNull queue ) && queue.CanSend then 
                        use msReply = new MemStream() 
                        msReply.WriteGuid( reqID )
                        msReply.SerializeObjectWithTypeName( reply.Result )
                        queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract), msReply )
                let ta2 = ta.ContinueWith( sendResult )
                ta.Start() 
        let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
        let bSuccess = Utils.IsNull !errorMsg
        if not bSuccess then 
            Logger.Log( LogLevel.MildVerbose, !errorMsg )
        if not ( Utils.IsNull queue ) && queue.CanSend then 
            if not bSuccess then 
                use msError = new MemStream( (!errorMsg).Length * 2 + 20)
                msError.WriteGuid( reqID )
                msError.WriteString( !errorMsg ) 
                queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
        
            
    /// <summary>
    /// Fucntion parsing to invoke a function remotely. 
    /// </summary> 
    static member inline internal  RemoteFunctionTaskWrapper<'T, 'TResult> (name) (registerFunc: Guid->Task<'TResult>) (sendFunc) = 
        Func<'T, Task<'TResult>>( 
            fun param ->    
                let msFunc, reqID = ContractStoreCommon.RemoteInvoke( name ) 
                msFunc.SerializeFromWithTypeName( param )
                sendFunc( reqID, msFunc )
                let ta = registerFunc( reqID) 
                // Wait for result to become available. 
                ta
       )
    /// <summary>
    /// Fucntion parsing to invoke a function remotely. 
    /// </summary> 
    static member inline internal  RemoteFunctionWrapper<'T, 'TResult> (name) (registerFunc: Guid->Task<'TResult>) (sendFunc) = 
        Func<'T, 'TResult>( 
            fun param ->    
                let ta = ContractStoreCommon.RemoteFunctionTaskWrapper<'T, 'TResult> name registerFunc sendFunc
                ta.Invoke( param).Result
       )



/// <summary>
/// ContractStoreAtDaemon registers contracts that are exported by PrajnaProgram at daemon. Client and other PrajnaProgram may search the contract store to find 
/// appropriate contract to import. The class is to be used only by Prajna core programmers. 
/// </summary>
type internal ContractStoreAtDaemon() = 
    /// <summary>
    /// Access the common ContractStoreAtDaemon for the address space. 
    /// </summary>
    static member val Current = ContractStoreAtDaemon() with get
    /// <summary>
    /// Exported contract at daemon
    /// </summary>
    member val internal ExportCollections = ConcurrentDictionary<_,ConcurrentDictionary<_,_>>(StringComparer.Ordinal) with get
    /// <summary>
    /// Imported collection at daemon
    /// </summary>
    member val internal ImportCollections = ConcurrentDictionary<_,ConcurrentDictionary<_,_>>( (StringTComparer<int64>(StringComparer.Ordinal)) ) with get
    /// <summary>
    /// Requests that are pending
    /// </summary>
    member val internal PendingRequests = ConcurrentDictionary<_,(_*_*_)>() with get
    /// <summary> 
    /// Register a contract at daemon
    /// </summary> 
    member internal x.RegisterContract( name, info: ContractInfo, epSignature: int64, bReload ) =
        let nameDic = x.ExportCollections.GetOrAdd( name, fun _ -> ConcurrentDictionary<_,_>() ) 
        let oldInfo = nameDic.GetOrAdd( epSignature, info )
        let errorMsgRef = ref null
        let mutable bSuccess = Object.ReferenceEquals(oldInfo, info ) 
        if not bSuccess then 
            if not bReload then 
                errorMsgRef := sprintf "!!! Warning !!! Contract %s of %A from %s is reloaded by a contract of same name but of %A from the same server"
                                            name
                                            oldInfo (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                                            info
                bSuccess <- false
            else
                nameDic.Item( epSignature ) <- info
                bSuccess <- true
        if nameDic.Count > 1 then 
        /// Examine type in the contract store, is there any conflict?
            if not (info.AllowMultiMatch()) then 
                    errorMsgRef := sprintf "!!! Warning !!! Contract %s of %A is reloaded from %s, however, the contract type is not a type that allows multiple match"
                                                                            name
                                                                            info (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                    bSuccess <- false
            else                
                for pair in nameDic do
                    let info1 = pair.Value
                    if info1.ContractType<>info.ContractType
                        || String.Compare( info1.ObjTypeIn, info.ObjTypeIn, StringComparison.Ordinal )<>0  
                        || String.Compare( info1.ObjTypeOut, info.ObjTypeOut, StringComparison.Ordinal )<>0 then 
                        let epSignature1 = pair.Key
                        errorMsgRef := sprintf "!!! Warning !!! Contract %s of %A from %s is reloaded by a contract of same name but of %A from %s"
                                                    name
                                                    info1 (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature1)))
                                                    info (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                        bSuccess <- false
        if bSuccess then 
            let msSuccess = new MemStream( name.Length * 2 + 4) 
            msSuccess.WriteString( name ) 
            ControllerCommand( ControllerVerb.Ready, ControllerNoun.Contract ), msSuccess
        else
            let msError = new MemStream( ( name.Length + (!errorMsgRef).Length) * 2 + 8) 
            msError.WriteString( name ) 
            msError.WriteString( !errorMsgRef ) 
            ControllerCommand( ControllerVerb.Error, ControllerNoun.Contract ), msError
            
    /// <summary> 
    /// Look for a contract at daemon with a particular name
    /// </summary> 
    member internal x.LookforContract( name, epSignature: int64 ) =
        let bExist, nameDic = x.ExportCollections.TryGetValue( name ) 
        let arr = 
            if bExist then 
                nameDic |> Seq.toArray 
            else
                Array.empty
        let msContract = new MemStream( ) 
        if arr.Length > 0 then 
            let importDic = x.ImportCollections.GetOrAdd( (name, epSignature), fun _ -> ConcurrentDictionary<_,_>())
            msContract.WriteStringV( name )
            msContract.WriteVInt32( arr.Length ) 
            for i = 0 to arr.Length - 1 do 
                arr.[i].Value.Pack( msContract )
                importDic.GetOrAdd( arr.[i].Key, true ) |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Look for contract %s by %s, returned %d entries"
                                                           name 
                                                           (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                                                           arr.Length ))
            ControllerCommand( ControllerVerb.Set, ControllerNoun.Contract ), msContract
        else
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Look for contract %s by %s but doesn't find the contract"
                                                           name 
                                                           (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                                                           ))
            msContract.WriteStringV( name )
            ControllerCommand( ControllerVerb.Close, ControllerNoun.Contract ), msContract
            
    /// <summary> 
    /// Look for a contract at daemon with a particular name
    /// </summary> 
    member internal x.ProcessContractRequest( ms:StreamBase<byte>, epSignature: int64 ) =
        // Peek out name and request ID
        let pos = ms.Position
        let name = ms.ReadStringV( )
        let reqID = ms.ReadGuid( ) 
        ms.Seek( pos, SeekOrigin.Begin ) |> ignore 
        let bExist, nameDic = x.ExportCollections.TryGetValue( name ) 
        let reqDic = ConcurrentDictionary<_,_>()
        if bExist then 
            for pair in nameDic do 
                let queueSignature = pair.Key
                let queue = Cluster.Connects.LookforConnectBySignature( queueSignature )
                if Utils.IsNotNull queue && queue.CanSend then 
                    let _, contractInfo = nameDic.TryGetValue( queue.RemoteEndPointSignature )
                    reqDic.GetOrAdd( queue.RemoteEndPointSignature, contractInfo ) |> ignore
                    queue.ToSend( ControllerCommand( ControllerVerb.Request, ControllerNoun.Contract ), ms )
        if not reqDic.IsEmpty then
            let bAddSuccess = ref false
            let addFunc _ = 
                bAddSuccess := true
                (epSignature, ref (PerfDateTime.UtcNowTicks()), reqDic )
            x.PendingRequests.GetOrAdd( reqID, addFunc ) |> ignore
            if !bAddSuccess then 
                // ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.Contract ), msOrg
                ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Unknown), null
            else
                let errorMsg = sprintf "req ID %A already exist" reqID
                let msError = new MemStream( ( name.Length + errorMsg.Length) * 2 + 8) 
                msError.WriteString( name ) 
                msError.WriteString( errorMsg ) 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s" errorMsg ))
                ControllerCommand( ControllerVerb.Error, ControllerNoun.Contract ), msError
        else
            let errorMsg = sprintf "can't find active end points for service %s" 
                                    name
            let msError = new MemStream( ( name.Length + errorMsg.Length) * 2 + 8) 
            msError.WriteString( name ) 
            msError.WriteString( errorMsg ) 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s" errorMsg ))
            ControllerCommand( ControllerVerb.Error, ControllerNoun.Contract ), msError
    /// <summary> 
    /// Look for a contract at daemon with a particular name
    /// </summary> 
    member internal x.ProcessContractReply( ms:StreamBase<byte>, epSignature: int64 ) =
        // Parse out name and request ID
        let pos = ms.Position
        let reqID = ms.ReadGuid( )
        let bNull = CustomizedSerialization.PeekIfNull(ms) 
        ms.Seek( pos, SeekOrigin.Begin ) |> ignore
        let bExist, tuple = x.PendingRequests.TryGetValue( reqID ) 
        let errorMsg = ref null
        if bExist then 
            let replyToSignature, ticksReqRef, reqDic = tuple
            let bExist, contractInfo = reqDic.TryGetValue( epSignature )
            if bExist then 
            /// Find the request
                let bToRemove = bNull || not contractInfo.IsAggregable
                if bToRemove then 
                    reqDic.TryRemove( epSignature ) |> ignore
                // not relay when the reply is null && contract is aggregable && there is still request pending 
                let mutable bLastReply = reqDic.IsEmpty
                let bRelayReply = not bNull || not contractInfo.IsAggregable || bLastReply
                if bRelayReply then 
                    let queue = Cluster.Connects.LookforConnectBySignature( replyToSignature )
                    if Utils.IsNotNull queue && queue.CanSend then 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Forward Reply, Contract from %s to %s of %dB" 
                                                                               (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                                                                               (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                               ms.Length ))
                        queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract ), ms )
                    else
                        errorMsg := sprintf "ProcessContractReply: End points %s for req %A has already been closed" 
                                                (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(replyToSignature))) reqID
                        bLastReply <- true
                if bLastReply then 
                    x.PendingRequests.TryRemove( reqID ) |> ignore
                else
                    ticksReqRef := (PerfDateTime.UtcNowTicks())

            else
                errorMsg := sprintf "ProcessContractReply: End points %s sends in a reply for req %A, but there is no record that the request has been sent to this endpoint" 
                                        (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature))) reqID
        else
            errorMsg := sprintf "ProcessContractReply: End points %s sends in a reply for req %A, which doesn't exist in pending request store." 
                                    (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature))) reqID
        if Utils.IsNull !errorMsg then
            // ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.Contract ), msOrg
            ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Unknown), null
        else
            let msError = new MemStream( ( !errorMsg).Length * 2 + 20) 
            msError.WriteGuid( reqID ) 
            msError.WriteString( !errorMsg ) 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s" !errorMsg ))
            ControllerCommand( ControllerVerb.FailedReply, ControllerNoun.Contract ), msError
    /// <summary> 
    /// Look for a contract at daemon with a particular name
    /// </summary> 
    member internal x.ProcessContractFailedRequest( ms:StreamBase<byte>, epSignature: int64 ) =
        // Parse out name and request ID
        let pos = ms.Position
        let reqID = ms.ReadGuid( )
        ms.Seek( pos, SeekOrigin.Begin ) |> ignore
        let bExist, tuple = x.PendingRequests.TryGetValue( reqID ) 
        let errorMsg = ref null
        if bExist then 
            let replyToSignature, ticksReqRef, reqDic = tuple
            let bExist, contractInfo = reqDic.TryRemove( epSignature )
            if bExist then 
                let mutable bLastReply = reqDic.IsEmpty
                let queue = Cluster.Connects.LookforConnectBySignature( replyToSignature )
                if Utils.IsNotNull queue && queue.CanSend then 
                    if contractInfo.IsAggregable then 
                        // Insert a null reply before error 
                        use msNull = new MemStream( 1024 )
                        msNull.WriteGuid( reqID )
                        CustomizedSerialization.WriteNull(msNull) 
                        queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.Contract ), msNull )
                    queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract ), ms )
                else
                    errorMsg := sprintf "ProcessContractFailedReply: End points %s for req %A has already been closed" 
                                            (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(replyToSignature))) reqID
                    bLastReply <- true
                if bLastReply then 
                    x.PendingRequests.TryRemove( reqID ) |> ignore
                else
                    ticksReqRef := (PerfDateTime.UtcNowTicks())
            else
                errorMsg := sprintf "ProcessContractFailedReply: End points %s sends in a reply for req %A, but there is no record that the request has been sent to this endpoint" 
                                        (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature))) reqID
        else
            errorMsg := sprintf "ProcessContractFailedReply: End points %s sends in a reply for req %A, which doesn't exist in pending request store." 
                                    (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature))) reqID
        if Utils.IsNull !errorMsg then
            // ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.Contract ), msOrg
            ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Unknown), null
        else
            let msError = new MemStream( ( !errorMsg).Length * 2 + 20) 
            msError.WriteGuid( reqID ) 
            msError.WriteString( !errorMsg ) 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s" !errorMsg ))
            ControllerCommand( ControllerVerb.FailedReply, ControllerNoun.Contract ), msError
    /// Call when a queue disconnects. 
    member internal x.CloseConnection( epSignature) = 
        // Examine exported contract
        for pair0 in x.ExportCollections do 
            let bExist, info = pair0.Value.TryRemove( epSignature ) 
            if bExist then 
                if pair0.Value.IsEmpty then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "Contract %s has been removed .... "
                                                                           pair0.Key ))
                    x.ExportCollections.TryRemove( pair0.Key ) |> ignore 
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let name = pair0.Key
                                                                   sprintf "Contract %s from %s of %A is removed .... "
                                                                           name (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)))
                                                                           info ))
        // Examine imported contract
        for pair0 in x.ImportCollections do 
            let name, epImport = pair0.Key
            if epImport = epSignature then 
                x.ImportCollections.TryRemove( pair0.Key ) |> ignore 
            else
                let dic = pair0.Value
                let bRemove, _ = dic.TryRemove( epSignature ) 
                if bRemove && dic.IsEmpty then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "Contract %s from %s is removed as there are no service provider .... "
                                                                           name (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epImport)))
                                                                           ))
                    let queue = Cluster.Connects.LookforConnectBySignature( epSignature )
                    if not (Utils.IsNull queue) && queue.CanSend then 
                        use msContract = new MemStream( 1024 )
                        msContract.WriteStringV( name )
                        queue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Contract ), msContract )
                    x.ImportCollections.TryRemove( pair0.Key ) |> ignore 
        // Examine requests. 
        for pair1 in x.PendingRequests do 
            let replyToSignature, ticksReq, reqDic = pair1.Value
            if replyToSignature = epSignature then 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ ->  sprintf "Request %s has been removed due to endpoint closure.... "
                                                                             (pair1.Key.ToString()) ))
                x.PendingRequests.TryRemove( pair1.Key ) |> ignore 
            else
                let bRemove, _ = reqDic.TryRemove( epSignature ) 
                if bRemove && reqDic.IsEmpty then 
                    let reqID = pair1.Key
                    let errMsg = sprintf "Request %s has been closed due to all endpoints that provide the services are closed .... "
                                                                                  (reqID.ToString())
                    Logger.Log( LogLevel.ExtremeVerbose, errMsg )
                    let queue = Cluster.Connects.LookforConnectBySignature( replyToSignature )
                    if Utils.IsNotNull queue && queue.CanSend then 
                        use msError = new MemStream( errMsg.Length * 2 + 20) 
                        msError.WriteGuid( reqID ) 
                        msError.WriteString( errMsg ) 
                        queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract ), msError )
                    x.PendingRequests.TryRemove( reqID ) |> ignore                     
                
    static member val internal MonitorIntervalInMS = 60000L with get, set
    member val internal LastMonitorTicks = ref (PerfDateTime.UtcNowTicks()) with get
    member val internal LastTimeOutCheckTicks = ref (PerfDateTime.UtcNowTicks()) with get
    static member val internal TimeOutCheckIntervalInMS = 1000L with get, set
    /// <summary> 
    /// Monitor contracts at daemon
    /// </summary> 
    member internal x.MonitorContracts() = 
        Logger.Do( LogLevel.MildVerbose, ( fun _ -> 
           let oldValue = !x.LastMonitorTicks
           let newValue = (PerfDateTime.UtcNowTicks())
           if newValue - oldValue > TimeSpan.TicksPerMillisecond * ContractStoreAtDaemon.MonitorIntervalInMS then 
               if Interlocked.CompareExchange( x.LastMonitorTicks, newValue, oldValue ) = oldValue then 
                   /// monitor
                   for pair0 in x.ExportCollections do 
                       let name = pair0.Key
                       for pair1 in pair0.Value do 
                           let epSignature1 = pair1.Key
                           let info = pair1.Value
                           Logger.LogF( LogLevel.MildVerbose, ( fun _ -> 
                              sprintf "Exported Contract %s from %s of %A " 
                                          name (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature1)))
                                          info 
                              ))
                   for pair0 in x.ImportCollections do 
                           Logger.LogF( LogLevel.MildVerbose, ( fun _ -> 
                              let name, epImport = pair0.Key
                              sprintf "Imported Contract %s by %s serviced by %s " 
                                          name (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epImport)))
                                          ( pair0.Value.Keys |> Seq.map( fun ep -> (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epImport))) ) |> String.concat "," )
                              ))
       ))
        let oldValue = !x.LastTimeOutCheckTicks
        let newValue = (PerfDateTime.UtcNowTicks())
        if newValue - oldValue > TimeSpan.TicksPerMillisecond * ContractStoreAtDaemon.TimeOutCheckIntervalInMS then 
            if Interlocked.CompareExchange( x.LastMonitorTicks, newValue, oldValue ) = oldValue then 
                /// check for time out request
                for pair1 in x.PendingRequests do 
                    let mutable bRemove = false
                    let replyToSignature, ticksReq, reqDic = pair1.Value
                    for pair0 in reqDic do 
                        let contractInfo = pair0.Value
                        let ticksCur = (PerfDateTime.UtcNowTicks())
                        if !ticksReq < ticksCur - TimeSpan.TicksPerMillisecond * (int64 contractInfo.RequestTimeoutAtDaemonInMS ) then 
                            // Request timeout 
                            reqDic.TryRemove( pair0.Key ) |> ignore 
                            bRemove <- true
                    if bRemove && reqDic.IsEmpty then 
                        /// No end point is available, requested timeout. 
                        let reqID = pair1.Key
                        let errMsg = sprintf "Request %s has been timed out after %d ms of inactivity .... "
                                                (reqID.ToString()) ( ((PerfDateTime.UtcNowTicks())-(!ticksReq))/TimeSpan.TicksPerMillisecond) 
                        Logger.Log( LogLevel.MildVerbose, errMsg )
                        let queue = Cluster.Connects.LookforConnectBySignature( replyToSignature )
                        if Utils.IsNotNull queue && queue.CanSend then 
                            use msError = new MemStream( errMsg.Length * 2 + 20) 
                            msError.WriteGuid( reqID ) 
                            msError.WriteString( errMsg ) 
                            queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract ), msError )
                        x.PendingRequests.TryRemove( reqID ) |> ignore                     
                ()

[<AllowNullLiteral>]
type internal ContractRequest() = 
    member val ParseFunc = Unchecked.defaultof<_> with get, set
    member val EndPointCollection = ConcurrentDictionary<int64,bool>() with get

[<AllowNullLiteral; AbstractClass>]
/// Class that manage the life cycle of one request
type internal ContractRequestManager ( reqID: Guid ) = 
    /// <summary> default timeout value (in milliseconds) to timeout the request </summary>
    static member val DefaultRequestTimeoutInMs = 30000 with get, set
    /// ID of the request 
    member val ReqID = reqID with get
    /// Ticks at which time the request timesout
    member val internal RequestTicksTimeOut = DateTime.MaxValue.Ticks with get, set
    /// ManualResetEvent to wait for the request to be fullfilled. 
    member val EvWait = new ManualResetEvent(false) with get
    /// A collection of endpoint that the request has been sent to 
    member val EndPointCollection = ConcurrentDictionary<int64,bool>() with get
    /// A function that is called when a reply comes back from a contract server 
    abstract ParseFunc: StreamBase<byte> * int64 -> bool
    /// A function that is called when a contract server informs that it fails to service the contract 
    abstract FailedRequestFunc: string * int64 -> bool


[<AllowNullLiteral>]
/// <summary>
/// Used to resolve pending contracts. 
/// </summary>
type internal ContractResolver( name ) = 
    /// <summary>
    /// A store for connection end point of a particular contract
    /// </summary>
    static member val internal OutgoingCollections = ConcurrentDictionary<_,ContractRequestManager>() with get
    /// Timeout for importing a contract 
    static member val ResolveTimeOutInMilliseconds = 60000 with get, set
    member val internal ContractName = name with get
    member val internal ContractInfo: ContractInfo = null with get, set
    member val internal ResolveTicks = (PerfDateTime.UtcNowTicks()) with get, set
    /// Timeout for importing the current contract 
    member val ResolveTimeOutInMs = ContractResolver.ResolveTimeOutInMilliseconds with get, set
    /// Action being invoked when a new contract server becomes available. 
    member val RegisterFunc: Action<int64> = null with get, set
    /// Action being invoked when a contract server leaves
    member val UnregisterFunc: Action<string*int64> = null with get, set
    /// Constract a request 
    member val ConstructRequestFunc: Func<Guid,ContractRequestManager> = null with get, set
    member val internal EvWaitTobeResolved = new ManualResetEvent( false) with get
    member val internal  ResolvedSuccess = false with get, set
    member val internal  EndPointCollection = ConcurrentDictionary<int64,bool>() with get
    /// Return a set of queues to be used to send out the current request. 
    abstract GetSendQueues: Guid -> List<NetworkCommandQueue>
    default x.GetSendQueues( reqID) = 
        let epList = List<_>()
        for pair in x.EndPointCollection do
            if pair.Value then
                let queue = Cluster.Connects.LookforConnectBySignature( pair.Key )
                if not (Utils.IsNull queue) && queue.CanSend then 
                    epList.Add( queue ) 
        epList
    /// <summary>
    /// Send out a request 
    /// </summary> 
    member x.SendRequestCustomizable<'T> timeoutInMs (inp: 'T) =
        let name = x.ContractName
        let reqID = System.Guid.NewGuid()
        let epList = x.GetSendQueues( reqID)
        if epList.Count > 0 then 
            let timeoutTicks = 
                if timeoutInMs > 0 then 
                    (PerfADateTime.UtcNowTicks()) + int64 timeoutInMs * TimeSpan.TicksPerMillisecond 
                else
                    DateTime.MaxValue.Ticks
            let reqHolder = ContractResolver.OutgoingCollections.GetOrAdd( reqID, x.ConstructRequestFunc )
            if Utils.IsNull reqHolder then 
                // This happens for Action, in which we do not track incoming reply
                ContractResolver.OutgoingCollections.TryRemove( reqID ) |> ignore     
            use msRequest = new MemStream( )
            msRequest.WriteStringV( name )
            msRequest.WriteGuid( reqID ) 
            msRequest.SerializeObjectWithTypeName( inp )
            for queue in epList do 
                queue.ToSend( ControllerCommand( ControllerVerb.Request, ControllerNoun.Contract), msRequest )
                if not (Utils.IsNull reqHolder) then 
                    reqHolder.EndPointCollection.GetOrAdd( queue.RemoteEndPointSignature, false ) |> ignore 
            if not (Utils.IsNull reqHolder) then 
                reqHolder.RequestTicksTimeOut <- timeoutTicks
            reqHolder
        else
            // Request failed 
            failwith ( sprintf "There is no valid service endpoints for contract %s" name )
    /// <summary>
    /// Send out a request, with name, input paramter
    /// </summary> 
    member x.SendRequestDefaultTimeout<'T> inp =
        x.SendRequestCustomizable<'T> ContractRequestManager.DefaultRequestTimeoutInMs inp


[<AllowNullLiteral>]
type internal ContractRequestManagerForFunc<'TResult>( reqID ) = 
    inherit ContractRequestManager( reqID )
    member val ContFunc : 'TResult -> unit = fun _ -> () with get, set
    member val ExceptionFunc: Exception  -> unit = fun _ -> () with get, set
    member val Result = Unchecked.defaultof<_> with get, set
    override x.ParseFunc( ms, queueSignature ) = 
        let bRemove, _ = x.EndPointCollection.TryRemove( queueSignature ) 
        let obj = ms.DeserializeObjectWithTypeName() 
        match obj with 
        | :? 'TResult as res -> 
            x.Result <- res 
            x.ContFunc( res )
            x.EvWait.Set() |> ignore 
            if bRemove && x.EndPointCollection.IsEmpty then 
                true
            else 
                false
        | _ -> 
            x.FailedRequestFunc( sprintf "Rcvd %A that is not of type %s" obj typeof<'TResult>.FullName, queueSignature )
    /// Failed request, if queueSignature is 0, all pending requests will be cancelled. 
    override x.FailedRequestFunc ( errMsg, queueSignature ) =
        let bRemove, _ = 
            if queueSignature<>0L then 
                x.EndPointCollection.TryRemove( queueSignature ) 
            else
                x.EndPointCollection.Clear()
                true, true
        Logger.Log( LogLevel.Error, ( errMsg ))
        x.ExceptionFunc( Exception( errMsg ) )
        x.EvWait.Set() |> ignore 
        if bRemove && x.EndPointCollection.IsEmpty then 
            true
        else 
            false
    static member Constructor (reqID: Guid ) = 
        ContractRequestManagerForFunc<'TResult>( reqID ) :> ContractRequestManager
    static member ConstructorForAction (guid: Guid ) = 
        ( null : ContractRequestManagerForFunc<'TResult> ) :> ContractRequestManager


[<AllowNullLiteral>]
type internal ContractRequestManagerForSeqFunc<'TResult>( reqID ) = 
    inherit ContractRequestManager( reqID )
    let mutable allRcvd = false
    let mutable allDone = false 
    let nRefTotal = ref 0
    let nRefCurrentCount = ref -1
    member val CurrentArrayRef = ref null with get
    member val RcvdResult = ConcurrentQueue<'TResult[]>() with get
    override x.ParseFunc( ms, queueSignature ) = 
        let obj = ms.DeserializeObjectWithTypeName() 
        if Utils.IsNull obj then 
            // End of a certain collection 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ContractRequestManagerForFunc receives null type %s[] for req %A from %s"
                                                                   (typeof<'TResult>.Name)
                                                                   reqID 
                                                                   (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint( queueSignature ) ))
                                                                   ))
            let bRemove, _ = x.EndPointCollection.TryRemove( queueSignature ) 
            if bRemove && x.EndPointCollection.IsEmpty then 
                allRcvd <- true
                x.EvWait.Set() |> ignore 
                true
            else
                false
        else
            match obj with 
            | :? ('TResult[]) as arr -> 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ContractRequestManagerForFunc receives %d count of %s for req %A from %s"
                                                                       arr.Length 
                                                                       (typeof<'TResult>.Name)
                                                                       reqID 
                                                                       (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint( queueSignature ) ))
                                                                       ))
                x.RcvdResult.Enqueue( arr ) 
                x.EvWait.Set() |> ignore 
                false
            | _ -> 
                x.FailedRequestFunc( sprintf "Rcvd %A that is not of type %s" obj typeof<'TResult>.FullName, queueSignature )
    /// Failed request, if queueSignature is 0, all pending requests will be cancelled. 
    override x.FailedRequestFunc (errMsg, queueSignature ) =
        let bRemove, _ = 
            if queueSignature<>0L then 
                x.EndPointCollection.TryRemove( queueSignature ) 
            else
                x.EndPointCollection.Clear()
                true, true
        Logger.Log( LogLevel.Error, ( errMsg ))
        x.EvWait.Set() |> ignore 
        if bRemove && x.EndPointCollection.IsEmpty then 
            allRcvd <- true
            x.EvWait.Set() |> ignore 
            true
        else 
            false
    member x.ClearAll() = 
        x.EndPointCollection.Clear() 
        while x.RcvdResult.TryDequeue( x.CurrentArrayRef ) do
            ()
        nRefCurrentCount := -1 
        allRcvd <- true
        allDone <- true
        x.EvWait.Set() |> ignore
    static member Constructor (reqID: Guid ) = 
        new ContractRequestManagerForFunc<'TResult>( reqID ) :> ContractRequestManager
    interface IEnumerator<'TResult> with 
        member x.Current = 
            (!x.CurrentArrayRef).[ !nRefCurrentCount ]
    interface System.Collections.IEnumerator with 
        member x.Current = box ((!x.CurrentArrayRef).[ !nRefCurrentCount ]) 
        member x.MoveNext() = 
            if not allDone then 
                let mutable bRet = false
                while not bRet && not allDone do 
                    while Utils.IsNull !(x.CurrentArrayRef) && ( not x.RcvdResult.IsEmpty || not allRcvd ) do 
                        // Try to pull additional object if
                        // 1. there is no object 'TResult[] pening to be enumerated
                        // and 2. there is some receiving result 
                        // or  3. not every peers have sent in their result. 
                        x.EvWait.Reset() |> ignore 
                        let bExist = x.RcvdResult.TryDequeue( x.CurrentArrayRef )
                        if not bExist then 
                            x.EvWait.WaitOne() |> ignore   
                    if Utils.IsNull !(x.CurrentArrayRef) then 
                        // has to be: no receiving result ( x.RcvdResult.IsEmpty ) && allRcvd
                        allDone <- true
                    else
                        nRefCurrentCount := !nRefCurrentCount + 1 // Enumerator is single thread, no need to use Interlocked.Increment
                        if (!nRefCurrentCount) >= (!x.CurrentArrayRef).Length then 
                            x.CurrentArrayRef := null // Try to pull a different object. 
                            nRefCurrentCount := -1 
                        else
                            nRefTotal := !nRefTotal + 1
                            bRet <- true
                if allDone then 
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ContractRequestManagerForFunc, IEnumerator<%s> generated %d count" 
                                                                           (typeof<'TResult>.Name)
                                                                           (!nRefTotal) ))
                else
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ContractRequestManagerForFunc, IEnumerator<%s> get object %d" 
                                                                           (typeof<'TResult>.Name)
                                                                           (!nRefTotal) ))
                        
            // Only case to return false is:
            // allDone is true, i.e., no pending result, no receiving result ( x.RcvdResult.IsEmpty ) && allRcvd
            not allDone 
        member x.Reset() = 
            raise (new System.NotSupportedException("ContractRequestManagerForFunc doesn't support reset") )
    interface System.IDisposable with 
        member x.Dispose() = x.ClearAll()


/// Method to specify a contract server
type internal ContractServerType =
    | TrafficManager of string*int
    | SingleServer of string*int
    | ServerCluster of string*int64*byte[]
    | Daemon

/// Specify servers to be used to launch services, import and export contract
[<AllowNullLiteral; Serializable>]
type ContractServersInfo() = 
    /// A Guid that uniquely identifies ContractServersInfo class. 
    /// We expect that the ContractServersInfo to be parsed to remote node (e.g., to import contract). The Guid can be used to uniquely 
    /// identify the ContractServerInfo class, and avoids information to be reinitialized. 
    member val internal ID = Guid.NewGuid() with get, set
    /// Assign a new ID, usually means that there are some change/addition of the server 
    member private x.NewID() = 
        x.ID <- Guid.NewGuid()
    /// A list of contract server to be used. 
    member val internal ServerCollection = ConcurrentQueue<ContractServerType>() with get
    /// Add one traffic manager, with server name and port, 
    /// the traffic manager may be resolved to different servers during the time span
    member x.AddServerBehindTrafficManager( serverName, port ) = 
        let oneServer = ContractServerType.TrafficManager( serverName, port )
        x.ServerCollection.Enqueue( oneServer )
        x.NewID()
    /// Add one single server, with server name and port, 
    /// We assume that the single server always resolved to a certain servername and port, thus doesn't need to be resolved repeatedly
    member x.AddSingleServer( serverName:string, port ) = 
        if serverName.IndexOf( "trafficmanager.net", StringComparison.OrdinalIgnoreCase ) >= 0 then 
            x.AddServerBehindTrafficManager( serverName, port)
            x.NewID()
        else
            let oneServer = ContractServerType.SingleServer( serverName, port )
            x.ServerCollection.Enqueue( oneServer )
            x.NewID()
    /// Add a cluster 
    member x.AddCluster( cl: Cluster ) = 
        if Utils.IsNotNull cl then 
            use ms = new MemStream()
            cl.Pack( ms )
            let oneCluster = ContractServerType.ServerCluster( cl.Name, cl.Version.Ticks, ms.GetBuffer() )
            x.ServerCollection.Enqueue( oneCluster )
            x.NewID()
    /// Add Daemon
    member x.AddDaemon() = 
        x.ServerCollection.Enqueue( ContractServerType.Daemon )
        x.NewID()
//    /// Get all clusters in the serversinfo
//    member x.GetClusters() = 
//        x.ServerCollection |> Seq.choose ( fun serverType -> match serverType with 
//                                                             | ServerCluster( name, ticks, cluster ) -> 
//                                                                Some cluster
//                                                             | _ -> 
//                                                                None
//                                            )
    /// Add a cluster
    member x.Cluster with set( cl: Cluster ) = x.AddCluster( cl ) 
//                      and get() = x.GetClusters() |> Seq.find ( fun _ -> true )

                                
    /// Internal to Resolve All Traffic manager (in millisecond) 
    member val InternalToResolveAllInMillisecond = 30000 with get, set
    /// Show server information
    override x.ToString() = 
        x.ServerCollection  |> Seq.choose ( fun oneServer -> 
                                            match oneServer with 
                                            | TrafficManager ( serverName, port ) -> 
                                                Some ( sprintf "Traffic Manager %s:%d" serverName port )
                                            | SingleServer( serverName, port ) -> 
                                                Some ( sprintf "Server %s:%d" serverName port )
                                            | ServerCluster( clusterName, clusterVersion, clusterInfo ) -> 
                                                Some (sprintf "Cluster %s" clusterName )
                                            | Daemon ->
                                                Some "Daemon"
                                        )
                            |> String.concat ","
    interface IEquatable<ContractServersInfo> with
        override x.Equals y = 
            let xnull = Utils.IsNull x
            let ynull = Utils.IsNull y
            if xnull then 
                ynull
            elif ynull then 
                false
            else
                ( x.ID = y.ID )
    override x.Equals other = 
            let ynull = Utils.IsNull other
            if ynull then 
                false
            else
                match other with 
                | :? ContractServersInfo as y -> 
                    x.ID = y.ID 
                | _ -> 
                    false
    override x.GetHashCode() = 
        x.ID.GetHashCode()
    interface System.IComparable with 
        member x.CompareTo yobj =
            if Utils.IsNull yobj then 
                x.ID.CompareTo( Guid.Empty )
            else
                match yobj with 
                | :? ContractServersInfo as y -> 
                    x.ID.CompareTo( y.ID )
                | _ -> 
                    x.ID.CompareTo( Guid.Empty )

        

/// Specify contract servers to be monitored, this class can not be serialized
type [<AllowNullLiteral>]
    ContractServerInfoLocal internal () = 
    inherit ContractServersInfo()
    /// A list of to be resolved server ContractServerType is equal
    static member internal IsEqualContractServerType( a: ContractServerType, b:ContractServerType ) = 
        match a with 
        | TrafficManager ( serverNameA, portA) ->
            match b with 
            | TrafficManager ( serverNameB, portB) -> 
                String.Compare( serverNameA, serverNameB, StringComparison.OrdinalIgnoreCase )=0 && portA=portB
            | _ -> 
                false
        | SingleServer ( serverNameA, portA ) ->
            match b with 
            | SingleServer ( serverNameB, portB) -> 
                String.Compare( serverNameA, serverNameB, StringComparison.OrdinalIgnoreCase )=0 && portA=portB
            | _ -> 
                false
        | ServerCluster ( clusterNameA, clusterVerA, clusterInfoA ) -> 
            match b with 
            | ServerCluster ( clusterNameB, clusterVerB, clusterInfoB) -> 
                String.Compare( clusterNameA, clusterNameB, StringComparison.OrdinalIgnoreCase )=0 && clusterVerA=clusterVerB
            | _ -> 
                false
            
        | Daemon -> 
            match b with 
            | Daemon -> 
                true
            | _ -> 
                false
    /// Register Functions ( a set of functions to be called once per queue connected ) 
    member val internal OnConnectOperation = ExecuteEveryTrigger<int64>(LogLevel.MildVerbose) with get
    /// The action will be called once a queue has been connected
    member x.OnConnectAction( act:Action<int64>, infoFunc:Func<string> ) = 
        x.OnConnectOperation.Add( act, infoFunc.Invoke )
    /// The action will be called once a queue has been connected
    member x.OnConnect( act:int64 -> unit, infoFunc:unit -> string ) = 
        x.OnConnectOperation.Add( Action<_>(act), infoFunc )
    member val internal ToBeResolvedServerCollection = ConcurrentQueue<_>() with get
    /// Function that compare if two 
    /// Instantiate a new set of Contract server information from local server. 
    private new ( info: ContractServersInfo ) as x = 
        ContractServerInfoLocal() 
        then 
            x.Add( info )
    /// Add an additional set of Contract server
    member x.Add( info: ContractServersInfo) =
        for oneServer in info.ServerCollection do 
            let bExist = x.ServerCollection |> Seq.exists ( fun v -> ContractServerInfoLocal.IsEqualContractServerType( v, oneServer)) 
            if not bExist then 
                x.ServerCollection.Enqueue( oneServer )
                x.ToBeResolvedServerCollection.Enqueue( oneServer )
    static member val private ContractServerInfoLocalCollection = ConcurrentDictionary<Guid, ContractServerInfoLocal>() with get
    /// Public interface to derive a ContractServerInfoLocal from ContractServersInfo
    /// Cache is used so that if the same ContractServersInfo is passed in, a already constructed ContractServerInfoLocal is returned
    static member Parse( serversInfo: ContractServersInfo ) = 
        if Utils.IsNull serversInfo then 
            null 
        else 
            ContractServerInfoLocal.ContractServerInfoLocalCollection.GetOrAdd( serversInfo.ID, fun _ -> ContractServerInfoLocal(serversInfo) )
    member val internal ResolvedNameCollection = ConcurrentDictionary<_, IPHostEntry >( StringComparer.OrdinalIgnoreCase) with get
    /// A list of resolved server, ip address to server name 
    member val internal ResolvedServerCollection = ConcurrentDictionary<int64, string >( ) with get
    /// A list of resolved server, name to ip addresses
    /// server name is case insensitive
    member val internal ResolvedServerNameToIP = ConcurrentDictionary<string, int64 >( StringComparer.OrdinalIgnoreCase ) with get
    /// A list of connected server 
    member val internal ConnectedServerCollection = ConcurrentDictionary<int64, bool >( ) with get
    member val internal CTS = SafeCTSWrapper() with get 
    /// Servers whose information is as part of the cluster
    member val internal ServerInCluster = ConcurrentDictionary<_, bool >( StringComparer.Ordinal) with get
    /// Return each server as a single entry in the cluster. 
    member x.GetClusterCollection() = 
        let lstCluster = List<_>() 
        let mutable bTrafficManager = false
        for entry in x.ServerCollection do 
            match entry with 
            | TrafficManager ( serverName, port ) -> 
                // ToDo: Resolve Traffic Managers and add the servers as clusters. 
                bTrafficManager <- true
            | SingleServer ( serverName, port ) ->
                // ToDo: Convert single server into a cluster
                ()
            | ServerCluster ( clusterName, clusterVersion, clusterInfo ) -> 
                let addFunc() = 
                    use ms = new MemStream( clusterInfo )
                    let cl = Cluster.Unpack( ms )
                    cl 
                let cluster = ClusterFactory.GetOrAddCluster( clusterName, clusterVersion, addFunc )
                lstCluster.Add( cluster )
            | Daemon -> 
                ()
        lstCluster.ToArray()
    /// Return all resolved servers as a ConcurrentDictionary<int64, string >, where 
    /// the key is int64 which can parse to the IP Address & port of the server, 
    /// and the value is the host name of the server. 
    member x.GetServerCollection() = 
        x.ResolvedServerCollection
    /// Disconnect a certain channel 
    member x.DisconnectChannel( sig64: int64 ) = 
        let bExist, _ = x.ConnectedServerCollection.TryRemove( sig64 )
        if bExist then 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ContractServerInfoLocal:Disconnect of channel %A" (LocalDNS.GetHostInfoInt64(sig64)) )
    /// Add a connected channel. 
    member x.AddConnectedChannel( ch: NetworkCommandQueue, funcInfo ) = 
        let sig64 = ch.RemoteEndPointSignature
        /// We always use signature, to avoid holding reference to NetworkCommandQueue object
        x.ConnectedServerCollection.GetOrAdd( sig64, true ) |> ignore 
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ContractServerInfoLocal, add channel to %s" (LocalDNS.GetHostInfoInt64(sig64)) )
        x.OnConnectOperation.Trigger( sig64, funcInfo )
        ch.OnDisconnect.Add( fun _ -> x.DisconnectChannel( sig64 ) )
    /// Contains 
    member x.ContainsSignature( sig64: int64 ) = 
        x.ConnectedServerCollection.ContainsKey( sig64 )
    /// Cancel all pending DNS resolve operation
    member x.Cancel() = 
        x.CTS.Cancel()
    /// <summary> 
    /// Resolve: all Single server, 
    /// Resolve: traffic manager once. 
    /// This process is a long running process and may contain blocking operation (DNS resolve), and is recommend to run on its separate thread. 
    /// </summary> 
    /// <param name="bAutoConnect"> true, automatically connect to the server that is resolved. 
    ///                             false, no action is done. </param>
    member x.DNSResolveOnce( bAutoConnect ) = 
      use token = x.CTS.Token
      if Utils.IsNotNull token then
        let mutable bResovleAgain = false
        let mutable nTrafficManager = 0 
        let lstServers = List<_>()
        let refValue = ref Unchecked.defaultof<_>
        let mutable bEncounterOneTrafficManager = false
        let mutable bEncounterSingleServer = false
        let mutable cnt = x.ToBeResolvedServerCollection.Count
        while cnt > 0 && x.ToBeResolvedServerCollection.TryDequeue( refValue ) && not token.IsCancellationRequested do 
            cnt <- cnt - 1
            match !refValue with 
            | TrafficManager ( serverName, port ) -> 
                nTrafficManager <- nTrafficManager + 1
                if not bEncounterOneTrafficManager then 
                    // Only resolve at most one traffic manager. 
                    lstServers.Add( serverName, port )
                    x.ToBeResolvedServerCollection.Enqueue( !refValue )
                    bEncounterOneTrafficManager <- true
            | SingleServer ( serverName, port ) -> 
                lstServers.Add( serverName, port )
                bEncounterSingleServer <- true 
            | ServerCluster ( clusterName, clusterVersion, clusterInfo ) -> 
                let addFunc() = 
                    use ms = new MemStream( clusterInfo )
                    let cl = Cluster.Unpack( ms )
                    cl 
                let cluster = ClusterFactory.GetOrAddCluster( clusterName, clusterVersion, addFunc )
                for peeri = 0 to cluster.NumNodes - 1 do 
                    let node = cluster.Nodes.[peeri]
                    lstServers.Add( node.MachineName, node.MachinePort)
                    x.ServerInCluster.GetOrAdd( node.MachineName, true ) |> ignore 
                bEncounterSingleServer <- true
            | Daemon -> 
                if RemoteExecutionEnvironment.IsContainer() then 
                    let channels = NetworkConnections.Current.GetLoopbackChannels()
                    if bAutoConnect then 
                        for ch in channels do 
                            x.AddConnectedChannel( ch, fun _ -> sprintf "ContractServerInfoLocal:Loopback to %A" (LocalDNS.GetShowInfo(ch.RemoteEndPoint)) )
        for tuple in lstServers do 
          if not token.IsCancellationRequested then 
            let servername, port = tuple
            try 
                let entry = 
                    if bEncounterSingleServer then 
                        x.ResolvedNameCollection.GetOrAdd( servername, fun name -> Dns.GetHostEntry(servername) )
                    else
                        x.ResolvedNameCollection.AddOrUpdate( servername, (fun name -> Dns.GetHostEntry(servername)), (fun name v -> Dns.GetHostEntry(servername)) )
                let mutable nEntry = if Object.ReferenceEquals( entry.AddressList, null ) then 0 else entry.AddressList.Length
                let mutable bEncounterTrafficManager = false
                if x.ResolvedNameCollection.ContainsKey( entry.HostName ) then 
                    ()
                elif entry.HostName.IndexOf( "trafficmanager.net", StringComparison.OrdinalIgnoreCase ) >= 0 then
                    x.AddServerBehindTrafficManager( entry.HostName, port )
                    bResovleAgain <- not bEncounterOneTrafficManager
                    bEncounterTrafficManager <- true
                    nTrafficManager <- nTrafficManager + 1
                    nEntry <- 0 // We will resolve this host entry recursively again, as it may map to other address. 
                else
                    x.ResolvedNameCollection.GetOrAdd( entry.HostName, entry ) |> ignore 
                if nEntry > 0 then 
                    // The server has been resolved successfully.
                    let addrList = entry.AddressList |> Array.choose ( fun ipAddress -> if ipAddress.AddressFamily = Sockets.AddressFamily.InterNetwork then 
                                                                                            Some ipAddress
                                                                                        else 
                                                                                            None ) 
                    let ipv4Addr = addrList |> Array.map ( fun ipAddress -> ipAddress.GetAddressBytes())
                    nEntry <- ipv4Addr.Length
                    if nEntry > 0 then 
                        LocalDNS.AddEntry( entry.HostName, ipv4Addr )
                        for addr in ipv4Addr do
                            let sig64 = LocalDNS.IPv4AddrToInt64( IPAddress( addr), port ) 
                            if x.ResolvedServerCollection.ContainsKey( sig64 ) then 
                                // Update host name
                                x.ResolvedServerCollection.Item( sig64) <- entry.HostName 
                            else
                                x.ResolvedServerCollection.GetOrAdd( sig64, entry.HostName) |> ignore 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Dns name %s(%s) resovled to ipv4 addresses %A ... " servername entry.HostName (IPAddress(addr)) ))
                    if nEntry > 0 && bAutoConnect then 
                        let serverEntry = entry.HostName + ":" + port.ToString()
                        let bExist, oldEntry = x.ResolvedServerNameToIP.TryGetValue( serverEntry )
                        let bReconnect = 
                            if not bExist then 
                                true
                            else
                                let sigArray = addrList |> Array.map ( fun ipAddress -> LocalDNS.IPv4AddrToInt64( ipAddress, port) )
                                not ( sigArray |> Array.exists (fun u -> u=oldEntry ) )
                        if bExist && bReconnect then 
                            // Old server entry is mapped to a new address, which doesn't correspond to the old address, the connection should be disconnected 
                            let ch = NetworkConnections.Current.LookforConnectBySignature( oldEntry )
                            if Utils.IsNotNull ch then 
                                // Close the outdated queue
                                ch.Close()
                            x.ResolvedServerNameToIP.TryRemove( serverEntry ) |> ignore 
                        if bReconnect then 
                            let sig64 = LocalDNS.IPv4AddrToInt64( addrList.[0], port)
                            x.ResolvedServerNameToIP.Item( serverEntry ) <- sig64
                            let ch = NetworkConnections.Current.AddConnect( addrList.[0], port )
                            x.AddConnectedChannel( ch, fun _ -> sprintf "ContractServerInfoLocal to %s" entry.HostName )
                if nEntry = 0 && not bEncounterTrafficManager then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Dns name %s (%s) resovled to zerosyn ipv4 addresses ... " servername entry.HostName ))
            with
            | e -> 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "!!! Exception !!! when try to resolve dns name %s ... %A " servername e ))
        if bResovleAgain then 
            x.DNSResolveOnce(bAutoConnect)
        else
            if nTrafficManager >= 1 && not (token.IsCancellationRequested) then 
                let bStatus = token.WaitHandle.WaitOne( x.InternalToResolveAllInMillisecond / nTrafficManager ) 
                if not bStatus then 
                    x.DNSResolveOnce(bAutoConnect)
    static member ConstructFromCluster( cl: Cluster ) = 
        let serverInfo = ContractServersInfo()
        serverInfo.AddCluster( cl )
        let serverInfoLocal = ContractServerInfoLocal( serverInfo )
        serverInfoLocal.DNSResolveOnce( false )
        serverInfoLocal

/// ContractServerInfoLocalRepeatable is used to host remote servers that requires repeatable DNS resolution. 
/// Such servers may be behind a traffic manager, in which new servers can be added for a single address, e.g., *.trafficmanager.net.
type [<AllowNullLiteral>]
    ContractServerInfoLocalRepeatable internal () = 
    inherit ContractServerInfoLocal() 
    member val private RepeatedDNSResolveInProccess = ref 0 with get
    /// Start a continuous DNS resolve process, because DNS resolve has blocking operation, the process is placed on its own thread, rather than schedule on a timer or task 
    member x.RepeatedDNSResolve(bAutoConnect) = 
        if Interlocked.CompareExchange( x.RepeatedDNSResolveInProccess, 1, 0) = 0 then 
            use token = x.CTS.Token
            if Utils.IsNotNull token then 
                // Remove all objects in the Concurrent Queue
                let refObj = ref Unchecked.defaultof<_>
                while ( x.ToBeResolvedServerCollection.TryDequeue( refObj ) ) do
                    ()
                for entry in x.ServerCollection do 
                    x.ToBeResolvedServerCollection.Enqueue( entry )
                let thread = ThreadTracking.StartThreadForFunctionWithCancelation( fun _ -> x.CTS.Cancel() ) ( x.ToString ) ( fun _ -> x.DNSResolveOnce(bAutoConnect) )
                ()
        else
            failwith "Try to start another DNS resolution process. This is usually caused by trying to add additional servers after the DNS resolution process has been started"

/// Specify additional contract servers to be used. 
type [<AllowNullLiteral>]
    internal ContractServerQueues private () = 
    /// Default contract servers used.
    static member val Default = ContractServerQueues() with get
    /// A set of pre-resolved contract servers
    static member val ResolvedContractServersCollection = ConcurrentDictionary<Guid, ContractServerQueues>() with get

    member val internal ServerCollections = ConcurrentDictionary<int64, bool>() with get
    /// Add a contract server by queue 
    member x.AddQueue( queue: NetworkCommandQueue ) = 
        x.ServerCollections.GetOrAdd( queue.RemoteEndPointSignature, true ) |> ignore
    /// Add a cluster of contract servers 
    member x.AddCluster( cluster: Cluster ) = 
        for peeri = 0 to cluster.NumNodes - 1 do 
            let queue = cluster.QueueForWrite( peeri )
            x.AddQueue( queue ) 
    static member ParseServers( serversInfo:ContractServersInfo ) = 
        if Utils.IsNull serversInfo then 
            null 
        else 
            let addFunc() = 
                let localServersInfo = ContractServerInfoLocal.Parse( serversInfo )
                let serverQueues = ContractServerQueues()
                for cluster in localServersInfo.GetClusterCollection() do 
                    serverQueues.AddCluster(cluster)    
                serverQueues
            let sQueues = ContractServerQueues.ResolvedContractServersCollection.GetOrAdd( serversInfo.ID, fun _ -> addFunc() )
            sQueues 
    /// Return a sequence of contract servers that are active
    member x.GetNetworkQueues() = 
        let seq = x.ServerCollections.Keys 
                  |> Seq.choose( fun ep -> let queue = Cluster.Connects.LookforConnectBySignature( ep ) 
                                           if Utils.IsNull queue then 
                                                x.ServerCollections.TryRemove( ep ) |> ignore 
                                                None
                                           elif queue.CanSend then 
                                                Some queue
                                           else
                                                None )
        seq 
    /// Return a sequence of contract servers that are active
    static member GetNetworkQueues( serversInfo: ContractServersInfo ) = 
        if Utils.IsNull serversInfo then 
            ContractServerQueues.Default.GetNetworkQueues() 
        else
            let sQueues = ContractServerQueues.ParseServers serversInfo
            Seq.append (ContractServerQueues.Default.GetNetworkQueues()) (sQueues.GetNetworkQueues())
    /// Construct a contract resolver for a particular contract 
    abstract ConstructContractResolver: string -> ContractResolver
    default x.ConstructContractResolver contractName = 
        // Use default.
        null




     

/// <summary>
/// ContractStoreAtProgram contains wrapped up contract and deserialization code for other program/service to access the contract. The class is to be used by Prajna 
/// core programmers.  
/// </summary>
type internal ContractStoreAtProgram() = 
    /// <summary>
    /// Access the common ContractStoreAtDaemon for the address space. 
    /// </summary>
    static member val Current = ContractStoreAtProgram() with get
    /// <summary>
    /// Access the store for deserialization &amp; return of value for service. 
    /// </summary>
    member val Collections = ConcurrentDictionary<_,_>(StringComparer.Ordinal) with get
    /// <summary>
    /// name -> contract function
    /// </summary>
    member val internal RegisteredContract = ConcurrentDictionary<_,ContractRequest>(StringComparer.Ordinal) with get
    /// <summary>
    /// A store that stores the collected service entrypoint 
    /// </summary>
    member val RequestCollections = ConcurrentDictionary<_,ContractRequest>() with get
    /// <summary>
    /// Contracts to be resolved
    /// </summary>
    member val internal ToResolve = ConcurrentDictionary<_,ContractResolver>(StringComparer.Ordinal) with get
    /// <summary>
    /// Register the action/function to daemon &amp; a group of servers
    /// </summary> 
    member x.RegisterContractToServers (serversInfo) ( name, info, bReload, parseFunc: int64*StreamBase<byte> -> unit ) = 
        let mutable bRegisterSuccessful = false
        use msInfo = ContractInfo.PackWithName( name, info, bReload )
        x.Collections.Item( name ) <- parseFunc
        let allqueues = ContractServerQueues.GetNetworkQueues( serversInfo ) 
        let store = x.RegisteredContract.GetOrAdd( name, fun _ -> ContractRequest() )
        store.ParseFunc <- parseFunc
        for queue in allqueues do 
            if not (Utils.IsNull queue) && queue.CanSend then 
                ContractStoreAtProgram.RegisterNetworkParser queue
                queue.ToSend( ControllerCommand( ControllerVerb.Register, ControllerNoun.Contract ), msInfo )
                store.EndPointCollection.GetOrAdd( queue.RemoteEndPointSignature, false ) |> ignore 
                bRegisterSuccessful <- true
    /// <summary>
    /// Register the action/function to daemon
    /// </summary> 
    member x.RegisterContractToDaemon  = 
        x.RegisterContractToServers null  
    member x.LookforContractAtServers ( registerFunc:int64 -> unit) (unregisterFunc:string->int64->unit) (constructReqFunc) (serversInfo:ContractServersInfo) ( name, info: ContractInfo) = 
        use msLookfor = new MemStream()
        msLookfor.WriteStringV( name ) 
        let addFunc name = 
            let oneResolver = 
                if Utils.IsNull serversInfo then 
                    ContractResolver(name )
                else 
                    let serverQueues = ContractServerQueues.ParseServers serversInfo
                    let oneInstance = serverQueues.ConstructContractResolver( name ) 
                    if Utils.IsNull oneInstance then 
                        ContractResolver(name )
                    else
                        oneInstance
            if Utils.IsNull oneResolver.ContractInfo then 
                oneResolver.ContractInfo <- info
            if Utils.IsNull oneResolver.RegisterFunc then 
                oneResolver.RegisterFunc <- Action<_>( registerFunc )
            if Utils.IsNull oneResolver.UnregisterFunc then 
                oneResolver.UnregisterFunc <- Action<_>( fun ( name, ep ) -> unregisterFunc name ep )
            if Utils.IsNull oneResolver.ConstructRequestFunc then 
                oneResolver.ConstructRequestFunc <- Func<_,_>( fun guid -> constructReqFunc guid )
            oneResolver
        let resolver = x.ToResolve.GetOrAdd( name, addFunc ) 
        let bMatch = Object.ReferenceEquals( resolver.ContractInfo, info ) || resolver.ContractInfo.Equals (info )
        if not bMatch then 
            failwith ( sprintf "Contract %s already has type interface %A that does not match the contract type interface %A that is attempted to be resolved" 
                                name resolver.ContractInfo info )
        else
            let allqueues = ContractServerQueues.GetNetworkQueues( serversInfo ) 
            for queue in allqueues do 
                if Utils.IsNotNull queue && queue.CanSend then 
                    ContractStoreAtProgram.RegisterNetworkParser queue
                    queue.ToSend( ControllerCommand( ControllerVerb.Get, ControllerNoun.Contract ), msLookfor )
                    resolver.EndPointCollection.GetOrAdd( queue.RemoteEndPointSignature, true ) |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Lookfor contract %s at %s" 
                                                                   name (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) ))
            // Start resolving 
            resolver.ResolveTicks <- (PerfDateTime.UtcNowTicks())
        resolver
    member x.RegisterContract name queueSignature =
        let bExist, resolver = x.ToResolve.TryGetValue( name ) 
        if bExist then 
            resolver.EndPointCollection.Item( queueSignature ) <- true
            let mutable bAllDone = true
            for pair in resolver.EndPointCollection do 
                bAllDone <- bAllDone && pair.Value
            if bAllDone then 
                resolver.ResolvedSuccess <- true
                resolver.EvWaitTobeResolved.Set() |> ignore 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Contract %s successfully registered from %s. All providers have responsed. "
                                                               name
                                                               (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Contract %s successfully registered from %s. We are still waiting for some additional providers. "
                                                               name
                                                               (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
        else
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Received resolved contract %s from %s, but there is no named contract pending resolving"
                                                           name
                                                           (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
    member x.UnregisterContract name queueSignature = 
        let bExist, resolver = x.ToResolve.TryGetValue( name ) 
        if bExist then 
            let bRemove, _ = resolver.EndPointCollection.TryRemove( queueSignature ) 
            if bRemove then 
                if resolver.EndPointCollection.IsEmpty then 
                    x.ToResolve.TryRemove( name ) |> ignore 
                    resolver.ResolvedSuccess <- false
                    resolver.EvWaitTobeResolved.Set() |> ignore 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Unregister contract %s from %s. All contract provider has unregistered, the contract is removed. "
                                                                   name
                                                                   (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Unregister contract %s from %s. The contract is still active, as there are still providers. "
                                                                   name
                                                                   (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Unregister contract %s from %s, but the server has not registered to provide the contract. "
                                                               name
                                                               (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
        else
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Unregister contract %s from %s, but we can't find the relevant contract. "
                                                           name
                                                           (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))

    member x.SyncLookForContractAtServersWithTimeout (timeoutInMilliseconds : int) (servers) constructReqFunc ( name, info: ContractInfo) = 
        let resolver = x.LookforContractAtServers (x.RegisterContract name ) (x.UnregisterContract ) constructReqFunc servers (name, info )
        if Utils.IsNull resolver then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Failed in SyncLookForContractAtServersWithTimeout to get a valid resolver" ))
            null
        else
            let bSuccess = ThreadPoolWaitHandles.safeWaitOne( resolver.EvWaitTobeResolved, timeoutInMilliseconds ) 
            if bSuccess && resolver.ResolvedSuccess then 
                resolver
            else
                null
    member x.SyncLookForContractAtServers = 
        x.SyncLookForContractAtServersWithTimeout ContractResolver.ResolveTimeOutInMilliseconds  
    member x.LookforContractAtDaemon = 
        x.SyncLookForContractAtServersWithTimeout ContractResolver.ResolveTimeOutInMilliseconds null
    member x.ParseContractCommand (queueSignature:int64) (command:ControllerCommand) (ms:StreamBase<byte>) = 
        match (command.Verb, command.Noun ) with 
        | (ControllerVerb.Ready, ControllerNoun.Contract ) -> 
            let contractName = ms.ReadString() 
            let bExist, store = x.RegisteredContract.TryGetValue( contractName ) 
            if bExist then 
                let bExistEndPoint, _ = store.EndPointCollection.TryGetValue( queueSignature )
                if bExistEndPoint then 
                    store.EndPointCollection.Item( queueSignature ) <- true  
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "contract %s has been successfully registered .... " contractName ) )
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "claim that contract %s has been registered, but can't find end point %s .... " 
                                                                               contractName (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "claim that contract %s has been registered from %s, but can't find contract .... " 
                                                                               contractName (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) ))
        | (ControllerVerb.Error, ControllerNoun.Contract ) -> 
            let contractName = ms.ReadString() 
            let msg = ms.ReadString()
            let bExist, store = x.RegisteredContract.TryGetValue( contractName ) 
            if bExist then 
                store.EndPointCollection.TryRemove( queueSignature ) |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "Contract %s host at %s encounter an error %s  .... " 
                                                                     contractName
                                                                     (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature))) msg
                                                                     ))
        | (ControllerVerb.Set, ControllerNoun.Contract ) -> 
            let serviceName = ms.ReadStringV( )
            let arrlen = ms.ReadVInt32( ) 
            let arr = Array.zeroCreate<_> arrlen 
            for i = 0 to arrlen - 1 do 
                let info = ContractInfo.Unpack( ms ) 
                arr.[i] <- info
            let bExist, resolver = x.ToResolve.TryGetValue( serviceName ) 
            if bExist then 
                let info = resolver.ContractInfo
                info.MatchContract( queueSignature, arr, resolver.RegisterFunc.Invoke ) 
            else
                Logger.LogF( LogLevel.Info, ( fun _ ->  sprintf "Set, Contract received for Contract %s from %s, but there is no contract pending resolved (timeout?)  .... " 
                                                                         serviceName
                                                                         (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                                         ))
        | (ControllerVerb.Close, ControllerNoun.Contract ) -> 
            let serviceName = ms.ReadStringV( )
            let bExist, resolver = x.ToResolve.TryGetValue( serviceName ) 
            if bExist then 
                resolver.UnregisterFunc.Invoke( serviceName, queueSignature )
            else
                Logger.LogF( LogLevel.Info, ( fun _ ->  sprintf "Close, Contract received for Contract %s from %s, but there is no contract pending resolved (timeout?)  .... " 
                                                                         serviceName
                                                                         (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                                         ))
                
        | (ControllerVerb.Reply, ControllerNoun.Contract ) -> 
            let reqID = ms.ReadGuid() 
            let bExist, pendingRequest = ContractResolver.OutgoingCollections.TryGetValue( reqID ) 
            if bExist then 
                let bRemove = pendingRequest.ParseFunc( ms, queueSignature )
                if bRemove then 
                    ContractResolver.OutgoingCollections.TryRemove( reqID ) |> ignore     
            else
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ ->  sprintf "Reply, Contract received for Request %A from %s, but there is no reqeust pending  (timeout or action?)  .... " 
                                                                         reqID
                                                                         (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                                         ))
        | (ControllerVerb.FailedRequest, ControllerNoun.Contract ) -> 
            let reqID = ms.ReadGuid() 
            let errMsg = ms.ReadString()
            let bExist, pendingRequest = ContractResolver.OutgoingCollections.TryGetValue( reqID ) 
            if bExist then 
                let bRemove = pendingRequest.FailedRequestFunc( errMsg, queueSignature )
                if bRemove then 
                    ContractResolver.OutgoingCollections.TryRemove( reqID ) |> ignore     
            else
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ ->  sprintf "FailedRequest, Contract received for Request %A from %s, but there is no reqeust pending  (already timeout or action?)  .... " 
                                                                         reqID
                                                                         (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                                         ))
        | (ControllerVerb.Request, ControllerNoun.Contract ) -> 
            let contractName = ms.ReadStringV( )
            let pos = ms.Position
            let bExist, store = x.RegisteredContract.TryGetValue( contractName ) 
            try
                if bExist then 
                    store.ParseFunc( queueSignature, ms )
                else
                    let reqID = ms.ReadGuid() 
                    let errMsg = sprintf "Non exist contract %s for request ID %s" contractName (reqID.ToString())
                    Logger.Log( LogLevel.MildVerbose, errMsg )
                    let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
                    if Utils.IsNotNull queue && queue.CanSend then 
                        use msError = new MemStream( errMsg.Length * 2 + 20 )
                        msError.WriteGuid( reqID ) 
                        msError.WriteString( errMsg ) 
                        queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
            with 
            | e -> 
                ms.Seek( pos, SeekOrigin.Begin ) |> ignore 
                let reqID = ms.ReadGuid() 
                let errMsg = sprintf "Exception when serving contract %s for request ID %s with %A" contractName (reqID.ToString()) e
                Logger.Log( LogLevel.MildVerbose, errMsg )
                let queue = Cluster.Connects.LookforConnectBySignature( queueSignature ) 
                if Utils.IsNotNull queue && queue.CanSend then 
                    use msError = new MemStream( errMsg.Length * 2 + 20 )
                    msError.WriteGuid( reqID ) 
                    msError.WriteString( errMsg ) 
                    queue.ToSend( ControllerCommand( ControllerVerb.FailedRequest, ControllerNoun.Contract), msError )
        | (ControllerVerb.FailedReply, ControllerNoun.Contract ) -> 
            let reqID = ms.ReadGuid() 
            let errMsg = ms.ReadString() 
            Logger.LogF( LogLevel.Info, ( fun _ ->  sprintf "FailedReply, Contract received for reply %s from %s  .... " 
                                                                       (reqID.ToString())
                                                                       (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(queueSignature)))
                                                                       ))
        | _ ->
            // Command not relevant to this class 
            ()
    static member RegisterNetworkParser (queue:NetworkCommandQueue) = 
            let procContractStoreTask = (
                fun (cmd : NetworkCommand) -> 
                    ContractStoreAtProgram.Current.ParseContractCommand queue.RemoteEndPointSignature cmd.cmd (cmd.ms)
                    null
            )
            queue.GetOrAddRecvProc("ContractStore", procContractStoreTask) |> ignore
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.FunctionReturn<'TResult> ( holder: ContractRequestManagerForFunc<'TResult> ) = 
        if Utils.IsNull holder then 
            Func< 'TResult > ( fun _ -> Unchecked.defaultof<_> )
        else
            Func<'TResult>( fun _ -> 
                                    holder.EvWait.WaitOne() |> ignore 
                                    holder.Result )
    /// <summary> 
    /// Import a Action, with name, input parameter
    /// </summary> 
    member x.ImportAction<'T> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.Action, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = null )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForFunc<Object>.ConstructorForAction (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Action<'T>( 
                fun inp -> 
                    let holder = resolver.SendRequestDefaultTimeout inp 
                    // holder should be none, action will not be acknowledged. 
                    ()
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportFunction<'T,'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.Function, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<'T, int, 'TResult>( 
                fun inp timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable timeoutInMs inp 
                    if Utils.IsNull holder then 
                        Unchecked.defaultof<_>
                    else
                        let reqHolder = holder :?> ContractRequestManagerForFunc<'TResult>
                        reqHolder.EvWait.WaitOne( timeoutInMs ) |> ignore 
                        reqHolder.Result
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportFunctionUnit<'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.Function, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<int, 'TResult>( 
                fun timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable<Object> timeoutInMs null 
                    if Utils.IsNull holder then 
                        Unchecked.defaultof<_>
                    else
                        let reqHolder = holder :?> ContractRequestManagerForFunc<'TResult>
                        reqHolder.EvWait.WaitOne( timeoutInMs ) |> ignore 
                        reqHolder.Result
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportFunctionTask<'T,'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.FunctionTask, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<'T, int, Task<'TResult>>( 
                fun inp timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable timeoutInMs inp 
                    if Utils.IsNull holder then 
                        new Task<'TResult>( fun _ -> Unchecked.defaultof<_> )
                    else
                        let ta = TaskCompletionSource<'TResult>()
                        let reqHolder = holder :?> ContractRequestManagerForFunc<'TResult>
                        reqHolder.ContFunc <- ( fun res -> ta.SetResult( res ) )
                        reqHolder.ExceptionFunc <- ( fun exn -> ta.SetException( exn ) )
                        ta.Task
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportFunctionTaskUnit<'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.FunctionTask, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<int, Task<'TResult>>( 
                fun timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable<Object> timeoutInMs null 
                    if Utils.IsNull holder then 
                        new Task<'TResult>( fun _ -> Unchecked.defaultof<_> )
                    else
                        let ta = TaskCompletionSource<'TResult>()
                        let reqHolder = holder :?> ContractRequestManagerForFunc<'TResult>
                        reqHolder.ContFunc <- ( fun res -> ta.SetResult( res ) )
                        reqHolder.ExceptionFunc <- ( fun exn -> ta.SetException( exn ) )
                        ta.Task
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportSeqFunction<'T,'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.SeqFunction, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForSeqFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<'T, int, IEnumerable<'TResult>>( 
                fun inp timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable timeoutInMs inp 
                    if Utils.IsNull holder then 
                        Seq.empty
                    else
                        {   new IEnumerable<'TResult> with
                                member this.GetEnumerator() =
                                    let reqHolder = holder :?> ContractRequestManagerForSeqFunc<'TResult>
                                    reqHolder :> IEnumerator<'TResult>
                            interface System.Collections.IEnumerable with 
                                member this.GetEnumerator() = 
                                    let reqHolder = holder :?> ContractRequestManagerForSeqFunc<'TResult>
                                    reqHolder :> System.Collections.IEnumerator
                        }
                    )
    /// <summary> 
    /// Import a function, with name, input parameter
    /// </summary> 
    member x.ImportUnitSeqFunction<'TResult> serverInfo name = 
        let contractInfo = ContractInfo( ContractType = ContractKind.SeqFunction, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        let resolver = ContractStoreAtProgram.Current.SyncLookForContractAtServers serverInfo ContractRequestManagerForSeqFunc<'TResult>.Constructor (name, contractInfo )
        if Utils.IsNull resolver then 
            null
        else
            Func<int, IEnumerable<'TResult>>( 
                fun timeoutInMs -> 
                    let holder = resolver.SendRequestCustomizable<Object> timeoutInMs null
                    if Utils.IsNull holder then 
                        Seq.empty
                    else
                        {   new IEnumerable<'TResult> with
                                member this.GetEnumerator() =
                                    let reqHolder = holder :?> ContractRequestManagerForSeqFunc<'TResult>
                                    reqHolder :> IEnumerator<'TResult>
                            interface System.Collections.IEnumerable with 
                                member this.GetEnumerator() = 
                                    let reqHolder = holder :?> ContractRequestManagerForSeqFunc<'TResult>
                                    reqHolder :> System.Collections.IEnumerator
                        }
                    )

/// <summary>
/// ContractStore provides a central location for export/import contract
/// </summary>
type internal ContractStore () = 
    /// <summary>
    /// Access the common ContractStore for the address space. 
    /// </summary>
    static member val Current = ContractStore() with get
    /// <summary>
    /// Access the common ContractStore for the address space. 
    /// </summary>
    member val Collections = ConcurrentDictionary<_,Object>(StringComparer.Ordinal) with get
    /// <summary>
    /// Export an action Action&lt;'T>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="act"> An action of type Action&lt;'T> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member internal x.ExportInternal( name, obj: Object, bReload) = 
        if bReload then 
            // bReload is true, always overwrite the content in store. 
            x.Collections.Item( name ) <- obj
        else
            // bReload is false, operation will fail if an item of the same name exists in ContractStore
            let existingObj = x.Collections.GetOrAdd( name, obj )
            if not(Object.ReferenceEquals( obj, existingObj )) then 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore, local register of contract %s failed, as reloading is turned off, but item of same name already exists " name ))
    /// <summary>
    /// Export an action Action&lt;'T>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="act"> An action of type Action&lt;'T> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.ExportAction<'T>( name, act:Action<'T>, bReload) = 
        x.ExportInternal( name, act, bReload )
        let info = ContractInfo( ContractType = ContractKind.Action, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = null )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload,
                                                                ContractStoreCommon.ParseRemoteAction<_> act )
                                                            
                
    /// <summary>
    /// Export as a function Func&lt;'T,'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.ExportFunction<'T,'TResult>( name, func:Func<'T,'TResult>, bReload) = 
        x.ExportInternal( name, func, bReload ) 
        let info = ContractInfo( ContractType = ContractKind.Function, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteFunction<_,_> func )
    /// <summary>
    /// Export as a function Func&lt;'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.ExportFunction<'TResult>( name, func:Func<'TResult>, bReload) = 
        x.ExportInternal( name, func, bReload ) 
        let wrappedFunc = Func<Object, 'TResult>( fun _ -> func.Invoke() )
        let info = ContractInfo( ContractType = ContractKind.Function, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteFunction<_,_> wrappedFunc )
    /// <summary>
    /// Export as a function Func&lt;seq&lt;'TResult>>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    member x.ExportSeqFunction<'TResult>( name, func:Func<IEnumerable<'TResult>>, serializationLimit:int , bReload) = 
        x.ExportInternal( name, func, bReload ) 
        let wrappedFunc = Func<Object, IEnumerable<'TResult>>( fun _ -> func.Invoke() )
        let info = ContractInfo( ContractType = ContractKind.SeqFunction, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteSeqFunction<_,_> serializationLimit wrappedFunc )
    /// <summary>
    /// Export as a function Func&lt;seq&lt;'TResult>>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    member x.ExportSeqFunction<'T,'TResult>( name, func:Func<'T, IEnumerable<'TResult>>, serializationLimit:int , bReload) = 
        x.ExportInternal( name, func, bReload ) 
        let info = ContractInfo( ContractType = ContractKind.SeqFunction, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteSeqFunction<_,_> serializationLimit func )
    /// <summary>
    /// Export as a function that return a Task Func&lt;Task&lt;'TResult>>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.ExportFunctionTask<'TResult>( name, func:Func<Task<'TResult>>, bReload) = 
        x.ExportInternal( name, func, bReload )
        let wrappedFunc = Func<Object, Task<'TResult>>( fun _ -> func.Invoke() )
        let info = ContractInfo( ContractType = ContractKind.FunctionTask, ObjTypeIn = null, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteFunctionTask<_,_> wrappedFunc )

    /// <summary>
    /// Export as a function that return a Task Func&lt;'T,Task&lt;'TResult>>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.ExportFunctionTask<'T,'TResult>( name, func:Func<'T,Task<'TResult>>, bReload) = 
        x.ExportInternal( name, func, bReload )
        let info = ContractInfo( ContractType = ContractKind.FunctionTask, ObjTypeIn = typeof<'T>.FullName, ObjTypeOut = typeof<'TResult>.FullName )
        ContractStoreAtProgram.Current.RegisterContractToDaemon( name, info, bReload, 
                                                                    ContractStoreCommon.ParseRemoteFunctionTask<_,_> func )

    /// Export an action Action<'T>
    member x.ImportInternal( name ) = 
        let bExist, obj = x.Collections.TryGetValue( name )
        if bExist then 
            obj
        else
            null // failwith ( sprintf "ContractStore: failed to find item of name %s" name )
    /// Import an action Action<'T>
    member x.ImportLocalAction<'T>( name ) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Action<'T> as act -> 
                act 
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Action<%s>" name typeof<'T>.FullName ))
                null 
    /// Import a function Func<'T,'TResult>
    member x.ImportLocalFunction<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T,'TResult> as func -> 
                func
            | :? Func<'T, int, 'TResult> as func -> 
                Func<'T, 'TResult>( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | :? Func<'T, Task<'TResult>> as func -> 
                Func<'T, 'TResult>( fun inp -> (func.Invoke( inp )).Result )
            | :? Func<'T, int, Task<'TResult>> as func -> 
                Func<'T, 'TResult>( fun inp -> (func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs )).Result )

            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<%s, %s>" name typeof<'T>.FullName typeof<'TResult>.FullName ))
                null 
    /// Import a function Func<'T,'TResult>
    member x.ImportLocalFunction<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'TResult> as func -> 
                func
            | :? Func< int, 'TResult> as func -> 
                Func<'TResult>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | :? Func<int, Task<'TResult>> as func -> 
                Func<'TResult>( fun _ -> (func.Invoke(ContractRequestManager.DefaultRequestTimeoutInMs)).Result )
            | :? Func<Task<'TResult>> as func -> 
                Func<'TResult>( fun _ -> (func.Invoke()).Result )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<%s>" name typeof<'TResult>.FullName ))
                null
    /// Import a function Func<IEnumerable<'TResult>>
    member x.ImportLocalSeqFunction<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<IEnumerable<'TResult>> as func -> 
                func
            | :? Func<int, IEnumerable<'TResult>> as func -> 
                Func<IEnumerable<'TResult>>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | _ -> 
                let msg = ( sprintf "ContractStore: item of name %s is not of type Func<seq<%s>>" name typeof<'TResult>.FullName)
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s, always return an empty data set" msg ))
                Func<_>( fun _ -> Seq.empty )
    /// Import a function Func<'T,IEnumerable<'TResult>>
    member x.ImportLocalSeqFunction<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T, IEnumerable<'TResult>> as func -> 
                func
            | :? Func<'T, int, IEnumerable<'TResult>> as func -> 
                Func<'T, IEnumerable<'TResult>>( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | _ -> 
                let msg = ( sprintf "ContractStore: item of name %s is not of type Func<%s,seq<%s>>" name typeof<'T>.FullName typeof<'TResult>.FullName)
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s, always return an empty data set" msg ))
                Func<_,_>( fun _ -> Seq.empty )
    /// Import a function task:  Func<'T,Task<'TResult>>
    member x.ImportLocalFunctionTask<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T,Task<'TResult>> as func -> 
                func
            | :? Func<'T, int, Task<'TResult>> as func -> 
                Func<'T,Task<'TResult>>( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | :? Func<'T,'TResult> as func -> 
                Func<'T,Task<'TResult>>( fun inp -> new Task<_>( fun _ -> func.Invoke(inp) ) )
            | :? Func<'T, int, 'TResult> as func -> 
                Func<'T,Task<'TResult>>( fun inp -> new Task<_>( fun _ -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) ) )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<%s, Task<%s>>" name typeof<'T>.FullName typeof<'TResult>.FullName ))
                null 
    /// Import a function task:  Func<Task<'TResult>>
    member x.ImportLocalFunctionTask<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<Task<'TResult>> as func -> 
                func
            | :? Func<int, Task<'TResult>> as func -> 
                Func<Task<'TResult>>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )
            | :? Func<'TResult> as func -> 
                Func<Task<'TResult>>( fun _ -> new Task<_>( fun _ -> func.Invoke() ) )
            | :? Func<int, 'TResult> as func -> 
                Func<Task<'TResult>>( fun _ -> new Task<_>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) ) )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<Task<%s>>" name typeof<'TResult>.FullName ))
                null

    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import allows a timeout value
    // -------------------------------------------------------------------------------------------------------------------------------

    /// Import a function Func<'T,'TResult>
    member x.ImportLocalFunctionWithTimeout<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T, int, 'TResult> as func -> 
                func
            | :? Func<'T, 'TResult> as func -> 
                Func<'T, int, 'TResult>( fun inp _ -> func.Invoke( inp ) )
            | :? Func<'T, int, Task<'TResult>> as func -> 
                Func<'T, int, 'TResult>( fun inp timeout -> (func.Invoke( inp, timeout )).Result )
            | :? Func<'T, Task<'TResult>> as func -> 
                Func<'T, int, 'TResult>( fun inp _ -> (func.Invoke( inp )).Result )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<%s, [int,] %s> or " name typeof<'T>.FullName typeof<'TResult>.FullName ))
                null 
    /// Import a function Func<'T,'TResult>
    member x.ImportLocalFunctionWithTimeout<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<int, 'TResult> as func -> 
                func
            | :? Func< 'TResult> as func -> 
                Func<int, 'TResult>( fun _ -> func.Invoke() )
            | :? Func<int, Task<'TResult>> as func -> 
                Func<int, 'TResult>( fun timeout -> (func.Invoke(timeout)).Result )
            | :? Func<Task<'TResult>> as func -> 
                Func<int, 'TResult>( fun timeout -> (func.Invoke()).Result )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<[int,]%s>" name typeof<'TResult>.FullName ))
                null
    /// Import a function Func<IEnumerable<'TResult>>
    member x.ImportLocalSeqFunctionWithTimeout<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<int, IEnumerable<'TResult>> as func -> 
                func
            | :? Func<IEnumerable<'TResult>> as func -> 
                Func<int, IEnumerable<'TResult>>( fun _ -> func.Invoke() )
            | _ -> 
                let msg = ( sprintf "ContractStore: item of name %s is not of type Func<[int,IEnumerable<%s>>" name typeof<'TResult>.FullName)
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s, always return an empty data set" msg ))
                Func<int, _>( fun _ -> Seq.empty )
    /// Import a function Func<'T,IEnumerable<'TResult>>
    member x.ImportLocalSeqFunctionWithTimeout<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T, int, IEnumerable<'TResult>> as func -> 
                func
            | :? Func<'T, IEnumerable<'TResult>> as func -> 
                Func<'T, int, IEnumerable<'TResult>>( fun inp timeout -> func.Invoke(inp) )
            | _ -> 
                let msg = ( sprintf "ContractStore: item of name %s is not of type Func<%s,[int,]seq<%s>>" name typeof<'T>.FullName typeof<'TResult>.FullName)
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "%s, always return an empty data set" msg ))
                Func<_,_,_>( fun _ _ -> Seq.empty )
    /// Import a function task:  Func<'T,Task<'TResult>>
    member x.ImportLocalFunctionTaskWithTimeout<'T,'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<'T, int, Task<'TResult>> as func -> 
                func
            | :? Func<'T, Task<'TResult>> as func -> 
                Func<'T, int, Task<'TResult>>( fun inp timeout -> func.Invoke(inp) )
            | :? Func<'T, int, 'TResult> as func -> 
                Func<'T, int, Task<'TResult>>( fun inp timeout -> new Task<_>( fun _ -> func.Invoke(inp, timeout)) )
            | :? Func<'T, 'TResult> as func -> 
                Func<'T, int, Task<'TResult>>( fun inp timeout -> new Task<_>( fun _ -> func.Invoke(inp)) )
            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<%s, [int,]Task<%s>>" name typeof<'T>.FullName typeof<'TResult>.FullName ))
                null 
    /// Import a function task:  Func<Task<'TResult>>
    member x.ImportLocalFunctionTaskWithTimeout<'TResult>( name) = 
        let obj = x.ImportInternal( name )
        if Utils.IsNull obj then 
            null 
        else
            match obj with 
            | :? Func<int, Task<'TResult>> as func -> 
                func
            | :? Func<Task<'TResult>> as func -> 
                Func<int, Task<'TResult>>( fun timeout -> func.Invoke() )
            | :? Func<int, 'TResult> as func -> 
                Func<int, Task<'TResult>>( fun timeout -> new Task<_>( fun _ -> func.Invoke(timeout)) )
            | :? Func<'TResult> as func -> 
                Func<int, Task<'TResult>>( fun timeout -> new Task<_>( fun _ -> func.Invoke()) )

            | _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ContractStore: find contract of name %s, but is not of type Func<[int,]Task<%s>>" name typeof<'TResult>.FullName ))
                null
    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. We use C# interface for usage by language other than F#. 
    // For contract at servers, it will install the contract locally, so that next import will be a simple concurrent dictionary lookup
    // -------------------------------------------------------------------------------------------------------------------------------

    /// <summary> 
    /// Import a Action from both local &amp; server, with name, input parameter, if successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportAction<'T>( serverInfo, name ) = 
        let act = x.ImportLocalAction<'T> name
        if Utils.IsNull act then 
            let act = ContractStoreAtProgram.Current.ImportAction<'T> serverInfo name
            if not (Utils.IsNull act ) then 
                x.ExportInternal( name, act, true )
            act    
        else
            act 
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunctionWithTimeout<'T,'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunction<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionWithTimeout<'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunctionWithTimeout<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionUnit<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionTaskWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunctionTaskWithTimeout<'T,'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionTask<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionTaskWithTimeout<'TResult>( serverInfo, name ) = 
        let func = x.ImportLocalFunctionTaskWithTimeout<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionTaskUnit<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportSeqFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalSeqFunctionWithTimeout<'T,'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportSeqFunction<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportSeqFunctionWithTimeout<'TResult>( serverInfo, name )= 
        let func = x.ImportLocalSeqFunctionWithTimeout<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportUnitSeqFunction<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
            func   
        else
            func
    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. Default timeout is used. 
    // -------------------------------------------------------------------------------------------------------------------------------
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunction<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunction<'T,'TResult> name
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunction<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                // Always install function with timeout for external
                x.ExportInternal( name, func, true )
                Func<'T,'TResult>( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) )
            else
                null
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunction<'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunction<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionUnit<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                // Always install function with timeout for external
                x.ExportInternal( name, func, true )
                Func<'TResult>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )
            else
                null
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionTask<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalFunctionTask<'T,'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionTask<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
                Func<'T,Task<'TResult>> ( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) )
            else
                null
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportFunctionTask<'TResult>( serverInfo, name ) = 
        let func = x.ImportLocalFunctionTask<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportFunctionTaskUnit<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
                Func<Task<'TResult>> ( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )   
            else
                null
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportSeqFunction<'T,'TResult>( serverInfo, name )= 
        let func = x.ImportLocalSeqFunction<'T,'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportSeqFunction<'T,'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
                Func<'T,_>( fun inp -> func.Invoke( inp, ContractRequestManager.DefaultRequestTimeoutInMs ) ) 
            else
                // Sequence Function can be returned an empty sequence to indicate non availability. 
                Func<'T,_>( fun inp -> Seq.empty )
        else
            func
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    member x.ImportSeqFunction<'TResult>( serverInfo, name )= 
        let func = x.ImportLocalSeqFunction<'TResult> name 
        if Utils.IsNull func then 
            let func = ContractStoreAtProgram.Current.ImportUnitSeqFunction<'TResult> serverInfo name
            if not (Utils.IsNull func ) then 
                x.ExportInternal( name, func, true )
                Func<_>( fun _ -> func.Invoke( ContractRequestManager.DefaultRequestTimeoutInMs ) )    
            else
                Func<_>( fun _ -> Seq.empty )
        else
            func



