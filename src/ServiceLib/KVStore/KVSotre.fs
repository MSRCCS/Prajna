(*---------------------------------------------------------------------------
    Copyright 2013 Microsoftnsed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limit

    Liceations under the License.                                                      

    File: 
        KVStore.fs
  
    Description: 
        Implementing a key-value store service. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        May. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.KVStoreService

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Net
open System.Net.Sockets
open System.Net.Http
open System.Linq

open Prajna.Tools
open Prajna.Tools.FileTools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network
open Prajna.Core

open Prajna.Service.FSharp


/// <summary>
/// This class contains the parameter used to start the monitor service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
/// <param name="serviceName"> Name of the key-value store service </param>
/// <param name="comparerKey"> A function to that compares whether two key are equal </param>
/// <param name="setInitialValue"> A function that sets the initial value of a particular key, the function takes a key as parameter, and returns the
/// initial value for the key. </param>
/// <param name="updateValue"> A function that update a particular key. The function takes three parameters: 1) key, 2) current value, 
/// 3) delta value, and returns the resultant value of the key </param>
/// <param name="isEachKVPerFile"> If true, put each KV pair in its separate file (useful if each KV is huge), otherwise, put all KV pair in one file. </param>
[<AllowNullLiteral; Serializable>]
type internal KVServiceParam<'K,'V>( serviceName:string, 
                                        comparerKey: IEqualityComparer<'K>,
                                        setInitialValue: 'K -> 'V, 
                                        updateValue:  ('K*'V*'V) -> 'V, 
                                        isEachKVPerFile: bool ) as this =
    inherit WorkerRoleInstanceStartParam()
    static member val internal ServicePrefix = "KVStore_" with get
    member val internal ServiceName = serviceName with get, set
    member val internal SetInitialValue = setInitialValue with get, set
    member val internal CompareKey = comparerKey with get, set
    member val internal UpdateValue = updateValue with get, set
    member val internal IsEachKVPerFile = isEachKVPerFile with get, set
    /// A function that customize the data directory of the KV Store
    member val GetKVStorePath = this.DefaultKVStorePathImpl with get, set
    /// A function that customize the filename/directory name of the KV Store
    member val GetKVStoreName = this.DefaultGetKVStoreNameImpl with get, set 
    /// A function that customize the filename of key of the KV Store, only used if IsEachKVPerFile is true
    member val GetKeyName = this.DefaultGetKeyNameImpl with get, set
    /// Interval to save a snapshot of a KVStore, in Ticks (default is one second for each snapshot)
    member val SnapshotIntervalTicks = TimeSpan.TicksPerSecond with get, set
    /// Interval to save a new version of the KV Store, default is a year (always use the same name for the KV store) 
    member private x.DefaultKVStorePathImpl( ) = 
        let folder = Path.Combine( RemoteExecutionEnvironment.GetServiceFolder(), "KVStore_" + serviceName )
        folder
    member private x.DefaultGetKVStoreNameImpl( ) = 
        let folder = x.GetKVStorePath()
        FileTools.DirectoryInfoCreateIfNotExists folder |> ignore 
        Path.Combine( folder, sprintf "%s.dat" serviceName )
    member private x.DefaultGetKeyNameImpl( key: 'K) = 
        "Key_" + key.ToString() + ".dat"


            
/// <summary>
/// This class represent a instance to monitor the network interface. The developer may extend MonitorInstance class, to implement the missing functions.
/// </summary> 
[<AllowNullLiteral>]
type internal KVService<'K,'V >() =
    inherit WorkerRoleInstance<KVServiceParam<'K,'V>>()
    // Local copy of the KV store in the form of a concurrent dictionary
    let mutable localCollection = null
    // Information on whether a key has been changed since last update
    let mutable localChange = null
    member val internal Param : KVServiceParam<'K,'V> = null with get, set
    /// OnStart is run once during the start of the  MonitorInstance
    /// It is generally a good idea in OnStart to copy the specific startup parameter to the local instance. 
    override x.OnStart param =
        // Setup distribution policy
        let mutable bSuccess = true 
        x.Param <- param 
        if x.Param.IsEachKVPerFile then 
            localChange <- ConcurrentDictionary<_,bool>(x.Param.CompareKey)
        localCollection <- ConcurrentDictionary<_,'V>(x.Param.CompareKey)
        x.LoadSnapshot()
        x.LastSnapShotTicks := DateTime.UtcNow.Ticks
        ContractStore.ExportAction<'K*'V>( param.ServiceName + "_StoreKey", x.Store, true )
        ContractStore.ExportFunction<'K,'V>( param.ServiceName + "_LatestVersion", x.LastVersion, true )
        ContractStore.ExportAction<'K*'V>( param.ServiceName + "_UpdateKey", x.Update, true )
        ContractStore.ExportAction<unit>( param.ServiceName + "_Clear", x.Clear, true )
        if bSuccess then 
            x.EvTerminated.Reset() |> ignore 
        else
            x.EvTerminated.Set() |> ignore
        bSuccess 
    member val internal LastSnapShotTicks = ref DateTime.MinValue.Ticks with get
    member val internal LastUpdateTicks = ref DateTime.MinValue.Ticks with get
    // Store Key
    member internal x.Store (kv: 'K*'V ) =
        let key, value = kv
        let value1 = localCollection.AddOrUpdate(key,value,(fun k v -> value) ) 
        if x.Param.IsEachKVPerFile then 
            localChange.GetOrAdd( key, true) |> ignore 
        x.LastUpdateTicks := DateTime.UtcNow.Ticks
        ()
    // Clear the KV store
    member internal x.Clear() = 
        // Clear KV Store 
        if x.Param.IsEachKVPerFile then 
            localChange.Clear() 
        localCollection.Clear() 
        if not x.Param.IsEachKVPerFile then 
            let filename = x.Param.GetKVStoreName()
            if File.Exists filename then 
                File.Delete filename
        else
            let dirname = x.Param.GetKVStoreName()
            if Directory.Exists dirname then 
                Directory.Delete dirname
    // Get the latest version of the key 
    member internal x.LastVersion( key: 'K ) =
        let value = localCollection.GetOrAdd( key, x.Param.SetInitialValue )
        value
    // Update key 
    member internal x.Update( key: 'K, delta_value:'V ) = 
        let addFunc key = 
            let value0 = x.Param.SetInitialValue key
            x.Param.UpdateValue( key, value0, delta_value)
        let value = localCollection.AddOrUpdate( key, addFunc, fun key value0 -> x.Param.UpdateValue(key, value0, delta_value) ) 
        if x.Param.IsEachKVPerFile then 
            localChange.GetOrAdd( key, true) |> ignore     
        x.LastUpdateTicks := DateTime.UtcNow.Ticks
        ()   
    // Get 'last' ( max ) version      
//    member internal x.LastVersion( fn ) =
//        x.LocalCollection.Keys.Max<_, _>(fn)
    // Save a snapshot of the store 
    member internal x.SaveSnapshot() = 
        lock ( x ) ( x.SaveSnapshotOnce )
    // Save a snapshot of the store 
    member internal x.SaveSnapshotOnce() = 
        if not x.Param.IsEachKVPerFile then 
            let curTicks = DateTime.UtcNow.Ticks
            use ms = new MemStream()
            ms.WriteInt32( 0 )
            let mutable cnt = 0 
            for kvpair in localCollection do 
                let key = kvpair.Key
                let value = kvpair.Value
                ms.SerializeFromWithTypeName( key )
                ms.SerializeFromWithTypeName( value )
                cnt <- cnt + 1
            let pos = ms.Position
            ms.Seek( 0L, SeekOrigin.Begin ) |> ignore 
            ms.WriteInt32( cnt )
            ms.Seek( pos, SeekOrigin.Begin ) |> ignore 
            let filename = x.Param.GetKVStoreName()
            FileTools.WriteBytesToFile filename (ms.GetBuffer())
        else
            let dirName = x.Param.GetKVStoreName()
            DirectoryInfoCreateIfNotExists dirName |> ignore 
            for kvpair in localCollection do
                let key = kvpair.Key
                let value = kvpair.Value
                let bExist, bValue = localChange.TryRemove( key )
                if bExist && bValue then 
                    use ms = new MemStream()
                    ms.SerializeFromWithTypeName( key )
                    ms.SerializeFromWithTypeName( value )
                    let filename = Path.Combine( dirName, x.Param.GetKeyName(key) )
                    FileTools.WriteBytesToFile filename (ms.GetBuffer())
    // Load a snapshot from store 
    member internal x.LoadSnapshot() = 
        lock ( x ) ( x.LoadSnapshotOnce )
    member internal x.LoadSnapshotOnce() = 
        localCollection.Clear()
        let filename = x.Param.GetKVStoreName()
        if File.Exists( filename ) then 
            let byt = FileTools.ReadBytesFromFile filename
            let ms = new MemStream( byt, 0, byt.Length, false, true )
            let cnt = ms.ReadInt32()
            for i = 0 to cnt - 1 do 
                let key = ms.DeserializeObjectWithTypeName() :?> 'K
                let value = ms.DeserializeObjectWithTypeName() :?> 'V
                localCollection.GetOrAdd( key, value) |> ignore
        else
            let dirName = x.Param.GetKVStoreName()
            if Directory.Exists( dirName ) then 
                for kvpair in localCollection do
                    let byt = FileTools.ReadBytesFromFile filename
                    use ms = new MemStream( byt, 0, byt.Length, false, true)
                    let key = ms.DeserializeObjectWithTypeName() :?> 'K
                    let value = ms.DeserializeObjectWithTypeName() :?> 'V
                    localCollection.Item( key ) <- value
                    localChange.TryRemove( key ) |> ignore
    override x.Run() = 
        let mutable ticksStart = (DateTime.UtcNow.Ticks)
        while not (x.EvTerminated.WaitOne(0)) do 
            let bUpdate = x.LastUpdateTicks > x.LastSnapShotTicks
            if not bUpdate then 
                let updateTicks = Math.Max( DateTime.UtcNow.Ticks, !x.LastSnapShotTicks + x.Param.SnapshotIntervalTicks )
                x.LastSnapShotTicks := updateTicks
                let waitMS = ( DateTime.UtcNow.Ticks - !x.LastSnapShotTicks )/ TimeSpan.TicksPerMillisecond
                if waitMS > 0L then 
                    x.EvTerminated.WaitOne( int waitMS ) |> ignore 
            else
                x.LastSnapShotTicks := DateTime.UtcNow.Ticks
                // Update the store 
                x.SaveSnapshot()
                let waitMS = x.Param.SnapshotIntervalTicks / TimeSpan.TicksPerMillisecond
                if waitMS > 0L then 
                    x.EvTerminated.WaitOne( int waitMS ) |> ignore 
    override x.OnStop() = 
        x.SaveSnapshot()
        // Cancel all pending jobs. 
        x.EvTerminated.Set() |> ignore 
    override x.IsRunning() = 
        not (x.EvTerminated.WaitOne(0))


/// <summary>
/// KV class implements a distributed key-value store. 
/// </summary> 
[<AllowNullLiteral>]
type KVStore<'K,'V> private (storeName: string) = 
    member val private StoreKeyAction = None with get, set
    member val private GetKeyTask : ('K->Task<'V>) option = None with get, set
    member val private UpdateKeyAction = None with get, set
    member val private ClearAction: ( unit->unit) option = None with get, set
    member val StoreName = storeName with get
    /// Update a key, 
    /// The input will be a key and its delta value. 
    member x.Update(key: 'K, delta_value:'V) =
        match x.UpdateKeyAction with 
        | None -> 
            failwith ("Update Key Hasn't been implemented in the KV Store " + x.StoreName)
        | Some updateKey -> 
            updateKey( key, delta_value )
    /// Store a key with value 
    member x.Put(key: 'K, value:'V ) = 
        match x.StoreKeyAction with 
        | None -> 
            failwith ("Store Key Hasn't been implemented in the KV Store " + x.StoreName)
        | Some storeKey -> 
            storeKey( key, value )
    /// Get value from key
    member x.Get(key: 'K ) = 
        match x.GetKeyTask with 
        | None -> 
            failwith ("Get Key Hasn't been implemented in the KV Store " + x.StoreName)
        | Some getKeyFunc -> 
            let ta = getKeyFunc( key )
            ta.Result
    /// Get value from key (async interface) 
    member x.GetAsync(key: 'K ) = 
        match x.GetKeyTask with 
        | None -> 
            failwith ("Get Key Hasn't been implemented in the KV Store " + x.StoreName)
        | Some getKeyFunc -> 
            let ta = getKeyFunc( key )
            ta
    /// Clear KV store
    member x.Clear() = 
        match x.ClearAction with 
        | None -> 
            failwith ("Clear Hasn't been implemented in the KV Store " + x.StoreName)
        | Some clearAction -> 
            clearAction()
    /// <summary>
    /// Launch a kv store service on a particular store 
    /// </summary>
    /// <param name="serviceName"> Name of the key-value store service </param>
    /// <param name="sererInfo"> A cluster of servers that the key-value store service is to be launched </param>
    /// <param name="comparerKey"> A function to that compares whether two key are equal </param>
    /// <param name="setInitialValue"> A function that sets the initial value of a particular key, the function takes a key as parameter, 
    /// and returns the initial value for the key. </param>
    /// <param name="updateValue"> A function that update a particular key. The function takes three parameters: 1) key, 2) current value, 
    /// 3) delta value, and returns the resultant value of the key </param>
    /// <param name="isEachKVPerFile"> If true, put each KV pair in its separate file (useful if each KV is huge), otherwise, put all KV pair in one file. </param>
    static member StartService( kvStoreName, serverInfo: ContractServersInfo, comparerKey, setInitialValue, updateValue, isEachKVPerFile  ) = 
        let server = ContractServerInfoLocal.Parse(serverInfo)
        let clusters = server.GetClusterCollection()
        let param = KVServiceParam<'K,'V>( KVServiceParam<_,_>.ServicePrefix + kvStoreName, comparerKey, setInitialValue, updateValue, isEachKVPerFile)
        for cluster in clusters do 
            RemoteInstance.Start( cluster, param.ServiceName, param, fun _ -> KVService<'K,'V >() )
        ()
    /// <summary>
    /// Stop a kv store service on a particular store 
    /// </summary>
    /// <param name="serviceName"> Name of the key-value store service </param>
    /// <param name="sererInfo"> A cluster of servers that the key-value store service is to be launched </param>
    static member StopService( kvStoreName, serverInfo: ContractServersInfo ) = 
        let server = ContractServerInfoLocal.Parse(serverInfo)
        let clusters = server.GetClusterCollection()
        let serviceName = KVServiceParam<_,_>.ServicePrefix + kvStoreName
        for cluster in clusters do 
            RemoteInstance.Stop( cluster, serviceName )
        ()

    member val internal LocalCollection = ConcurrentDictionary<'K,'V >() with get
    /// A collection of distributed key value store 
    static member val internal KVStoreCollection = ConcurrentDictionary<String*ContractServersInfo,KVStore<'K,'V>>(StringTComparer<ContractServersInfo>(StringComparer.Ordinal)) with get
    /// Find a specific distributed key value store to use, import the remote function to the local KVStore
    static member GetKVStore( kvStoreName, serverInfo: ContractServersInfo) = 
        let store = KVStore.KVStoreCollection.GetOrAdd( (kvStoreName,serverInfo), KVStore<'K,'V>.ResolveKVStore )
        store
    static member internal ResolveKVStore<'K,'V> ( kvStoreName, serverInfo ) = 
        let store = KVStore<'K,'V>(kvStoreName) 
        let serviceName = KVServiceParam<_,_>.ServicePrefix + kvStoreName 
        let storeKeyAction = ContractStore.ImportAction<'K*'V>( serverInfo, serviceName + "_StoreKey" )
        if Utils.IsNull storeKeyAction then 
            failwith (sprintf "Failed to resolve Put function for %s" serviceName )
        let getKeyTask = ContractStore.ImportFunctionTask<'K,'V>( serverInfo, serviceName + "_LatestVersion" )
        if Utils.IsNull getKeyTask then 
            failwith (sprintf "Failed to resolve Get function for %s" serviceName )
        let updateKeyAction = ContractStore.ImportAction<'K*'V>( serverInfo, serviceName + "_UpdateKey" )
        if Utils.IsNull updateKeyAction then 
            failwith (sprintf "Failed to resolve update function for %s" serviceName )
        let mutable clearAction = ContractStore.ImportAction<unit>( serverInfo, serviceName + "_Clear" )
        if Utils.IsNull clearAction then 
            clearAction <- ( fun _ -> () )
        store.StoreKeyAction <- Some storeKeyAction
        store.GetKeyTask <- Some getKeyTask
        store.UpdateKeyAction <- Some updateKeyAction
        store.ClearAction <- Some clearAction
        store

    




