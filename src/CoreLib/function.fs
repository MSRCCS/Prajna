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
        function.fs
  
    Description: 
        Functional module of Prajna Client. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open System.Runtime.CompilerServices

/// FunctionType is a one byte mask defines the type of the 
/// function, 
type internal FunctionKind = 
    | None = 0x00              // No operation is applied
    | Native = 0x01            // Native .Net function (few)
    | DotNet = 0x02            // DotNet function with assemblies
    | Unmanaged = 0x03         // Function with unmanaged code
    | GPGPU = 0x04             // Function with GPGPU code
    | HasAssembly = 0x80       // if HasAssembly is set, then an assembly is attached with the function. 

/// Transform type is a one byte mask defines the type of the transform 
/// that is used by Prajna
type internal TransformKind =      // Type of transform
    | None = 0                  // No function used. 
    | FilteringKey = 1          // 'K -> bool
    | ChooseKey = 2             // 'K -> 'K1 option
    | Filtering = 3             // 'K * 'V -> bool
    | Repartition = 4           // Repartition
    | MappingValue = 128        // 'V -> 'U
    | Mapping = 129             // 'K, 'V -> 'K, 'V
    | CorrelatedMix = 130  
    | CrossJoin = 131           // Cross join function     
    | AggregateValue = 200      // two functions: State 'V -> State, State State ->State
    | CollectValue = 201        // 'V -> seq<'U>

type internal MapToKind = 
    | OBJECT = 0
    | MEMSTREAM = 1

//type JoinOps = 
//    | InnerJoin = 0 
//    | OuterJoin = 1

/// Function that used in DSet.AsyncReadChunk
/// input parameter is parti, serial, numElems, ms
/// Output parameter is seq<parti, serial, numElems, ms>
[<AllowNullLiteral; Serializable>]
type internal MetaFunction() = 
    let nullDecode( meta:BlobMetadata, ms:StreamBase<byte> ) =
        ( meta, System.Object() )
    let nullEncode( meta:BlobMetadata, o: Object ) = 
        let ms = new MemStream() :> StreamBase<byte>
        ms.Serialize( o ) 
        ( meta, ms )
    let nullMap( meta:BlobMetadata, o: Object, mapTo:MapToKind ) = 
        Seq.singleton ( meta, o )  
    let nullConstructPartitionCache( cacheType:CacheKind, parti:int, serializationLimit:int ) : PartitionCacheBase = 
        null
    let nullDeposit parenti ( meta:BlobMetadata, o: Object ) = 
        ()
    static member NullExecute (parti:int) : seq< BlobMetadata * Object> = 
        Seq.empty
//    let mutable internalSerializationLimit = 0
    member val ValidFunc = false with get, set
    /// Decode a stream from upstream, with meta, MemStream as input, and meta, Object as output
    member val Decode = nullDecode with get, set
    /// Encode a stream to downstream, with meta, Object as input, and meta, msStream as output
    member val Encode = nullEncode with get, set
    /// Map Function, signature: meta, Object -> seq<meta, Object>
    /// when Object is null, this signals the end of the partition. The null should be passed down the pipe to signal the end of the stream. 
    /// It is important that if the intermediate app returns a null Object, it does not call the downstreaming app, as this signals the end of the stream
    member val MapFunc = nullMap with get, set
    ///　Deposit from a parent DSet 
    member val DepositFunc = nullDeposit with get, set
    /// Execute 
    member val ExecuteFunc = MetaFunction.NullExecute with get, set
//    /// Decode upstream 1, 2, ... n
//    /// This function allows us not to install a Decoder at DSetPeer
//    member val DecodeUpstream = Array.create 1 nullDecode with get, set
    /// Function used at PrajnaClient, 
    /// Get partitions assignment for a array of key object and value object, 
    /// GetPartitionsForElem( numElems, keyArray, valueArray, numPartitions )
    member val GetPartitionsForElem = MetaFunction.RandomPartition with get, set
    
    /// Serialization Change Listner, any derived function that wants to be notified if serialization limits changes?
//    member val SerializationLimitChangeListener = List<_>() with get
    /// Serialization Limit will be used by some function so it is exposed here. 
    /// This will be automatically updated. 
//    member x.SerializationLimit with get() = internalSerializationLimit
//                                 and set( l ) = internalSerializationLimit <- l
//                                                for listener in x.SerializationLimitChangeListener do 
//                                                    listener( l )
    member val SerializationLimit = 0 with get, set
    /// Default partition function, assign a random key, value to a partition. 
    static member RandomPartition( meta:BlobMetadata, o:Object , numPartitions ) =
        let rnd = Random()
        Array.init meta.NumElems ( fun i -> rnd.Next( numPartitions ) )
    member val internal InitializePartitioner = MetaFunction.DefaultInitializePartitioner with get, set
    static member internal  DefaultInitializePartitioner( cluster:Cluster, loadBlancer:LoadBlanceAlg, x:Prajna.Core.Partitioner ) = 
        // If the numPartitions has been specified by the user, do not override its value. 
        if x.NumPartitions<0 then 
            // Default, create n^2 partition for a N node cluster or for a small cluster, 8N partitions. 
            let nTotalCores = cluster.ClusterInfo.ListOfClients |> Array.map ( fun cl -> cl.ProcessorCount ) |> Array.sum
            let node2 = cluster.NumNodes * Math.Min ( 100, cluster.NumNodes )
            // This partition information will be propagated. 
            x.numPartitionsInternal := Math.Max( node2, nTotalCores )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Use %d partitions ........... " x.NumPartitions ))
    static member MetaString( meta:BlobMetadata ) = 
        sprintf "partition:%d serial:%d numElems:%d" meta.Parti meta.Serial meta.NumElems 
    member val PartitionMetadata = ConcurrentDictionary<_,_>() with get
    member x.GetMetadataForPartition( meta:BlobMetadata, parti:int, numElems:int ) = 
        x.PartitionMetadata.AddOrUpdate( parti, (fun parti -> BlobMetadata( meta, parti, 0L, numElems)), 
                                                (fun parti (oldMetadata:BlobMetadata) -> BlobMetadata( meta, parti, oldMetadata.Serial + int64 oldMetadata.NumElems, numElems )) )
    member x.GetFinalMetadata parti = 
        let retMeta = ref Unchecked.defaultof<_>
        if x.PartitionMetadata.TryGetValue( parti, retMeta ) then 
            BlobMetadata( !retMeta, (!retMeta).Serial + int64 (!retMeta).NumElems, 0 )
        else
            BlobMetadata( parti, 0L, 0 )
    member x.BaseReset() = 
        x.PartitionMetadata.Clear()
    abstract Reset: unit -> unit
    default x.Reset() = 
        x.BaseReset()
    abstract InitAll: unit -> unit 
    default x.InitAll() = 
        ()
    abstract UnblockAll: unit -> unit 
    default x.UnblockAll() = 
        ()
    member val internal ConstructPartitionCache = nullConstructPartitionCache with get, set

type internal CastFunction<'U>() = 
    static member CastTo (o:Object) = 
        match o with 
        | :? (('U)[]) as arr ->
            arr
        | :? StreamBase<byte> as ms -> 
            Strm.DeserializeTo<'U[]>(ms)
        | _ -> 
            let msg = sprintf "CastFunction.CastTo, the input object is not of type ('U)[] or MemStream, but type %A with information %A" (o.GetType()) o
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    static member ForceTo (o:Object) = 
        CastFunction<'U>.CastTo( o ) :> Object    
    static member MapTo (elemArray:'U[]) mapTo = 
        match mapTo with
        | MapToKind.OBJECT -> 
            elemArray :> Object
        | MapToKind.MEMSTREAM -> 
            let ms = new MemStream() 
            let orgpos = ms.Position // Position 0, 
            Strm.SerializeFrom( ms, elemArray )
            ms.Seek( orgpos, SeekOrigin.Begin ) |> ignore
            ms :> Object
        | _ -> 
            let msg = sprintf "Error in CastFunction.MapTo, unsupported mapTo %A" mapTo
            Logger.Log( LogLevel.Error, msg )
            failwith msg            

/// Function that used in DSet.AsyncReadChunk
/// input parameter is parti, serial, numElems, ms
/// Output parameter is parti, serial, numElems, ms
[<AllowNullLiteral; Serializable>]
type internal MetaFunction<'U>() as x = 
    inherit MetaFunction()
    do
        x.Decode <- x.DecodeFuncToObj
        x.Encode <- x.EncodeFuncFromObj 
        x.GetPartitionsForElem <-  x.WrapperPartitionFuncFromObj    
        x.ConstructPartitionCache <- x.GenericConstructionPartitionCache
        x.ValidFunc <- true
    member internal x.GenericConstructionPartitionCache( tuple ) = 
        PartitionCacheQueue<'U>( tuple ) :> PartitionCacheBase
    member x.WrapperPartitionFuncFromObj( meta:BlobMetadata, o:Object , numPartitions ) = 
        let elemArray = o :?> ('U)[]
        let parti = Array.zeroCreate elemArray.Length
        for i = 0 to elemArray.Length - 1 do           
            parti.[i] <- x.GetPartition( elemArray.[i], numPartitions )
        parti
    member val GetPartition = MetaFunction<'U>.HashPartitionerKey with get, set
    static member HashPartitionerKey( elem: 'U, numPartitions ) = 
        Utils.GetHashCode(elem) % numPartitions
    member x.DecodeFuncToObj( meta, ms ) = 
        let meta, elemArray = x.DecodeFunc( meta, ms )
        meta, elemArray :> Object
    /// Input: metadata, MemStream
    /// Output: metadata, elemObject
    /// It is OK for value object to be null. However, a null KeyObject signals the end of the stream. 
    member x.DecodeFunc(meta, ms : StreamBase<byte> ) =
        // Decode Key Value
        try
            if Utils.IsNotNull ms then 
                let elemArray = Strm.DeserializeTo<'U[]>(ms)
                if Utils.IsNull elemArray then 
                    let msg = ( sprintf "in MetaFunction<'U>.DecodeFunc, it is unusual for a null key value object to be stored (and deserialized): %s" (MetaFunction.MetaString(meta))  )
                    Logger.Log( LogLevel.Warning, msg )
                    failwith msg
                else
// Release mode, don't check KeyArray size information. 
#if DEBUG
                    if meta.NumElems<>elemArray.Length then 
                        let msg = sprintf "Inconsistency in MetaFunction<'K, 'V>.DecodeFunc, the length of key-value array don't match that of numElems %s (%d actual keys)" (MetaFunction.MetaString(meta)) elemArray.Length
                        Logger.Log( LogLevel.Error, msg )
                        failwith msg
#endif
                    ( meta, elemArray )
            else
                // use null to indicate end of streams
                ( meta, null )
        with 
        | e -> 
            let msg = sprintf "Error in MetaFunction<'K, 'V>.decodeFunc, most probably fail to cast with %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member x.EncodeFuncFromObj(meta, o:Object ) : BlobMetadata*StreamBase<byte> =
        try
            if Utils.IsNotNull o then 
                x.EncodeFunc( meta, (o:?>('U)[]) )
            else
                x.EncodeFunc( meta, null )
        with 
        | e -> 
            let msg = sprintf "Error in MetaFunction<'K, 'V>.EncodeFuncFromObj, most probably fail to cast with %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    // Encoding
    member x.EncodeFunc(meta, elemArray ) : BlobMetadata*StreamBase<byte> =
        // Encode Key Value
        if Utils.IsNotNull elemArray then 
            // Encode will update the metadata 
            let encMeta = meta // x.GetMetadataForPartition( meta, meta.Parti, elemArray.Length ) 
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "EncodeFunc, type %A input metadata %s output metadata %s" (elemArray.GetType()) (meta.ToString()) (encMeta.ToString()) ))
            if encMeta.NumElems<>elemArray.Length then 
                let msg = sprintf "Error in MetaFunction<'U>.EncodeFunc, the length of key array doesn't match that of numElems, %s (actual %d key-values)" (MetaFunction.MetaString(meta)) elemArray.Length
                Logger.Log( LogLevel.Warning, msg )            
           // let ms = new MemStream() :> StreamBase<byte>
            let ms = new MemoryStreamB() :> StreamBase<byte>
            let orgpos = ms.Position // Position 0, 
            Strm.SerializeFrom( ms, elemArray )
//            if Utils.IsNotNull valueArray then 
//                if numElems<>valueArray.Length then 
//                    let msg = sprintf "Error in MetaFunction<'K, 'V>.encodeFunc, the length of value array doesn't match that of numElems, part:%d, serial:%d, numElems:%d (%d keys, %d values)" parti serial numElems keyArray.Length valueArray.Length
//                    Logger.Log(LogLevel.Error, msg)
//                    failwith msg
//            else
//                ms.Serialize( null )
            // Set position to be read by the DecodeFunc
            ms.Seek( orgpos, SeekOrigin.Begin ) |> ignore
            ( encMeta, ms )
        else
            // Encode will update the metadata 
            let encMeta = meta // x.GetMetadataForPartition( meta, meta.Parti, 0 ) 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "EncodeFunc, input metadata %s output metadata %s" (meta.ToString()) (encMeta.ToString()) ))
            // Signal the end of stream. 
            ( encMeta, null )


/// Mapping function wrapper: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal InitFunctionWrapper<'U >( initFunc, partitionSizeFunc ) as x =   
    inherit MetaFunction<'U >()
    do
        x.MapFunc <- x.WrapperInitFunc
//        x.MapStream <- x.WrapperInitFuncStream
    /// sizeFunc: parti -> size of partition i
    member val PartitionSizeFunc = partitionSizeFunc with get, set
    member val InitFunc: int * int -> 'U = initFunc with get, set
    /// Wrapper Mapping Function for Init, which is a DSet with DSetDependencyType.Source 
    /// The calling signature should be parti, serial, 0, null, null (numElems and o and v are not used). 
    /// The return signature should be (parti, serial, numElems, ko, vo) in which SerializationLimit worth of objects are generated.
    /// The InitFunc will be called for ( parti, serial) to generate the object associated with the serialization
    /// The InitFunc signals the end when it sends parti, serial, numElems, null, null
    member x.WrapperInitFunc( meta, o, mapTo ) = 
        let nEndOfPartition = x.PartitionSizeFunc( meta.Partition )
        let nSerial = int meta.Serial
        if nSerial >= nEndOfPartition then 
            // issue null to signal the end of the partition            
            let newMeta = BlobMetadata( meta, 0 )
            Seq.singleton ( newMeta, null )
        else
            let slimit = if x.SerializationLimit<=0 then DeploymentSettings.DefaultSerializationLimit else x.SerializationLimit
            let nItems = Math.Min( slimit, nEndOfPartition - nSerial )
            let elemArray = Array.zeroCreate<_> nItems
            for i = 0 to nItems - 1 do
                elemArray.[i] <- x.InitFunc( meta.Partition, nSerial + i )
            let newMeta = BlobMetadata( meta, nItems )
            match mapTo with 
            | MapToKind.OBJECT -> 
                Seq.singleton ( newMeta, elemArray :> Object )
            | MapToKind.MEMSTREAM -> 
                let cmeta, cms = x.Encode( newMeta, elemArray :> Object )
                Seq.singleton( cmeta, cms :> Object )
            | _ -> 
                let msg = sprintf "Error in WrapperInitFunc, unsupported mapTo %A" mapTo
                Logger.Log( LogLevel.Error, msg )
                failwith msg           
                
/// Mapping function wrapper: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal SourceFunctionWrapper<'U >( sourceSeqFunc ) as x =   
    inherit MetaFunction<'U >()
    do
        x.MapFunc <- x.WrapperSourceFunc
//        x.MapStream <- x.WrapperInitFuncStream
    /// sizeFunc: parti -> size of partition i
    member val private SourceSeqFunc: unit -> seq<'U> = sourceSeqFunc with get, set
    member val private KeyCount = 0L with get, set
    member val private SourceEnumerator = null with get, set
    member val private EndReached = false with get, set
    override x.Reset() = 
        x.BaseReset() 
        x.KeyCount <- 0L
        x.SourceEnumerator <- null
        x.EndReached <- false
    /// Wrapper Mapping Function for Init, which is a DSet with DSetDependencyType.Source 
    /// The calling signature should be parti, serial, 0, null, null (numElems and o and v are not used). 
    /// The return signature should be (parti, serial, numElems, ko, vo) in which SerializationLimit worth of objects are generated.
    /// The InitFunc will be called for ( parti, serial) to generate the object associated with the serialization
    /// The InitFunc signals the end when it sends parti, serial, numElems, null, null
    member x.WrapperSourceFunc( meta, o, mapTo ) = 
        if x.EndReached  then 
            let newMeta = BlobMetadata( meta, int64 x.KeyCount, 0 )
            Seq.singleton ( newMeta, null )
        else
            if x.KeyCount = 0L then 
                try 
                    x.SourceEnumerator <- x.SourceSeqFunc().GetEnumerator()
                with 
                | e -> 
                    let errMsg = sprintf "Failed in GetEnumerator() of sourceSeqFunc: %A" e
                    Logger.Log( LogLevel.Error, errMsg  )
            if Utils.IsNull x.SourceEnumerator then 
                // Empty Seq<'U>
                let newMeta = BlobMetadata( meta, int64 x.KeyCount, 0 )
                x.EndReached <- true
                Seq.singleton ( newMeta, null )
            else
                let slimit = if x.SerializationLimit<=0 then DeploymentSettings.DefaultSerializationLimit else x.SerializationLimit
                // Don't open a list with too many elements if we don't use it. 
                let lst = if slimit > DeploymentSettings.DefaultSerializationLimit then List<_>( DeploymentSettings.DefaultSerializationLimit ) else List<_>( slimit ) 
                let mutable bContinue = true
                while bContinue do
                    if x.SourceEnumerator.MoveNext() then 
                        lst.Add( x.SourceEnumerator.Current )
                        if lst.Count >= slimit then  
                            bContinue <- false
                    else
                        bContinue <- false
                        x.EndReached <- true
                        x.SourceEnumerator <- null
                let newMeta = BlobMetadata( meta, int64 x.KeyCount, lst.Count )
                x.KeyCount <- x.KeyCount + int64 lst.Count
                match mapTo with 
                | MapToKind.OBJECT -> 
                    Seq.singleton ( newMeta, lst.ToArray() :> Object )
                | MapToKind.MEMSTREAM -> 
                    let cmeta, cms = x.Encode( newMeta, lst.ToArray() :> Object )
                    Seq.singleton( cmeta, cms :> Object )
                | _ -> 
                    let msg = sprintf "Error in WrapperSourceFunc, unsupported mapTo %A" mapTo
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg

type internal SourceNPartitionHolder<'U>() = 
    member val internal KeyCount = 0L with get, set
    member val internal SourceEnumerator : IEnumerator<'U> = null with get, set
    member val internal EndReached = false with get, set
    
/// Mapping function wrapper: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal SourceNFunctionWrapper<'U>( numSource, sourceNSeqFunc ) as x =   
    inherit MetaFunction<'U>()
    do
        x.MapFunc <- x.WrapperSourceNFunc
//        x.MapStream <- x.WrapperInitFuncStream
    /// sizeFunc: parti -> size of partition i
    member val private SourceNSeqFunc: int -> seq<'U> = sourceNSeqFunc with get, set
    member val private Store = ConcurrentDictionary<_,SourceNPartitionHolder<'U>>() with get, set
    override x.Reset() = 
        x.BaseReset() 
        x.Store.Clear()
    /// Wrapper Mapping Function for Init, which is a DSet with DSetDependencyType.Source 
    /// The calling signature should be parti, serial, 0, null, null (numElems and o and v are not used). 
    /// The return signature should be (parti, serial, numElems, ko, vo) in which SerializationLimit worth of objects are generated.
    /// The InitFunc will be called for ( parti, serial) to generate the object associated with the serialization
    /// The InitFunc signals the end when it sends parti, serial, numElems, null, null
    member x.WrapperSourceNFunc( meta, o, mapTo ) = 
        let sourcei = meta.Partition % numSource
        let y = x.Store.GetOrAdd( sourcei, fun _ -> SourceNPartitionHolder() )
        if y.EndReached  then 
            let newMeta = BlobMetadata( meta, int64 y.KeyCount, 0 )
            Seq.singleton ( newMeta, null )
        else
            if y.KeyCount = 0L then 
                try 
                    y.SourceEnumerator <- x.SourceNSeqFunc(sourcei).GetEnumerator()
                with 
                | e -> 
                    let errMsg = sprintf "Failed in GetEnumerator() of sourceSeqFunc: %A" e
                    Logger.Log( LogLevel.Error, errMsg  )
            if Utils.IsNull y.SourceEnumerator then 
                // Empty Seq<'U>
                let newMeta = BlobMetadata( meta, int64 y.KeyCount, 0 )
                y.EndReached <- true
                Seq.singleton ( newMeta, null )
            else
                let slimit = if x.SerializationLimit<=0 then DeploymentSettings.DefaultSerializationLimit else x.SerializationLimit
                // Don't open a list with too many elements if we don't use it. 
                let lst = if slimit > DeploymentSettings.DefaultSerializationLimit then List<_>( DeploymentSettings.DefaultSerializationLimit ) else List<_>( slimit ) 
                let mutable bContinue = true
                while bContinue do
                    if y.SourceEnumerator.MoveNext() then 
                        lst.Add( y.SourceEnumerator.Current )
                        if lst.Count >= slimit then  
                            bContinue <- false
                    else
                        bContinue <- false
                        y.EndReached <- true
                        y.SourceEnumerator <- null
                let newMeta = BlobMetadata( meta, int64 y.KeyCount, lst.Count )
                y.KeyCount <- y.KeyCount + int64 lst.Count
                match mapTo with 
                | MapToKind.OBJECT -> 
                    Seq.singleton ( newMeta, lst.ToArray() :> Object )
                | MapToKind.MEMSTREAM -> 
                    let cmeta, cms = x.Encode( newMeta, lst.ToArray() :> Object )
                    Seq.singleton( cmeta, cms :> Object )
                | _ -> 
                    let msg = sprintf "Error in WrapperSourceFunc, unsupported mapTo %A" mapTo
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg           


/// Mapping function wrapper: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal SourceIFunctionWrapper<'U>( numPartitions, sourceISeqFunc ) as x =   
    inherit MetaFunction<'U>()
    do
        x.MapFunc <- x.WrapperSourceIFunc
//        x.MapStream <- x.WrapperInitFuncStream
    /// sizeFunc: parti -> size of partition i
    member val private SourceISeqFunc: int -> seq<'U> = sourceISeqFunc with get, set
    member val private Store = ConcurrentDictionary<_,SourceNPartitionHolder<'U>>() with get, set
    override x.Reset() = 
        x.BaseReset() 
        x.Store.Clear()
    /// Wrapper Mapping Function for Init, which is a DSet with DSetDependencyType.Source 
    /// The calling signature should be parti, serial, 0, null, null (numElems and o and v are not used). 
    /// The return signature should be (parti, serial, numElems, ko, vo) in which SerializationLimit worth of objects are generated.
    /// The InitFunc will be called for ( parti, serial) to generate the object associated with the serialization
    /// The InitFunc signals the end when it sends parti, serial, numElems, null, null
    member x.WrapperSourceIFunc( meta, o, mapTo ) = 
        let sourcei = meta.Partition
        let y = x.Store.GetOrAdd( sourcei, fun _ -> SourceNPartitionHolder() )
        if y.EndReached  then 
            let newMeta = BlobMetadata( meta, int64 y.KeyCount, 0 )
            Seq.singleton ( newMeta, null )
        else
            if y.KeyCount = 0L then 
                try 
                    y.SourceEnumerator <- x.SourceISeqFunc(sourcei).GetEnumerator()
                with 
                | e -> 
                    let errMsg = sprintf "Failed in GetEnumerator() of sourceSeqFunc: %A" e
                    Logger.Log( LogLevel.Error, errMsg  )
            if Utils.IsNull y.SourceEnumerator then 
                // Empty Seq<'U>
                let newMeta = BlobMetadata( meta, int64 y.KeyCount, 0 )
                y.EndReached <- true
                Seq.singleton ( newMeta, null )
            else
                let slimit = if x.SerializationLimit<=0 then DeploymentSettings.DefaultSerializationLimit else x.SerializationLimit
                // Don't open a list with too many elements if we don't use it. 
                let lst = if slimit > DeploymentSettings.DefaultSerializationLimit then List<_>( DeploymentSettings.DefaultSerializationLimit ) else List<_>( slimit ) 
                let mutable bContinue = true
                while bContinue do
                    if y.SourceEnumerator.MoveNext() then 
                        lst.Add( y.SourceEnumerator.Current )
                        if lst.Count >= slimit then  
                            bContinue <- false
                    else
                        bContinue <- false
                        y.EndReached <- true
                        y.SourceEnumerator <- null
                let newMeta = BlobMetadata( meta, int64 y.KeyCount, lst.Count )
                y.KeyCount <- y.KeyCount + int64 lst.Count
                match mapTo with 
                | MapToKind.OBJECT -> 
                    Seq.singleton ( newMeta, lst.ToArray() :> Object )
                | MapToKind.MEMSTREAM -> 
                    let cmeta, cms = x.Encode( newMeta, lst.ToArray() :> Object )
                    Seq.singleton( cmeta, cms :> Object )
                | _ -> 
                    let msg = sprintf "Error in WrapperSourceFunc, unsupported mapTo %A" mapTo
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg           
 

[<AllowNullLiteral; Serializable>]
type internal FunctionWrapper<'U, 'U1 >( func: (BlobMetadata* ('U)[] ) -> BlobMetadata * ('U1)[] ) as x =   
    inherit MetaFunction<'U1>()
    do
        // If there is upstream, the decoding function is here. 
        x.MapFunc <- x.WrapperMapFunc
//        x.MapStream <- x.WrapperMapStream    
    member val private UpstreamCodec = MetaFunction<'U>() with get
    member val private holdElemArray = ConcurrentDictionary<_,_>() with get
    /// Control whether UseFunc is called at final, with null key & value array parameter. 
    member val CallNullAtFinal = false with get, set
    /// Wrapped UseFunc, with (int * int64 * 'K[] * 'V[] ) -> ( int * int64 * 'K1[] * 'V1[] )
    member val UseFunc = func with get, set
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperMapFunc( meta, o: Object, mapTo ) = 
        try
            let outputSeq = List<_>()
            if Utils.IsNotNull o then 
                // Decode if necessary 
                let curMeta, elemArray = 
                    match o with 
                    | :? (('U)[]) as arr ->
                        meta, arr
                    | :? StreamBase<byte> as ms -> 
                        x.UpstreamCodec.DecodeFunc( meta, ms )
                    | _ -> 
                        let msg = sprintf "FunctionWrapper.WrapperMapFunc, the input object is not of type ('U)[] or MemStream, but type %A with information %A" (o.GetType()) o
                        Logger.Log( LogLevel.Error, msg )
                        failwith msg
                // Map 
                let retMeta, newElemArray = x.UseFunc( curMeta, elemArray )
                let bNullReturn = Utils.IsNull newElemArray
                if not bNullReturn then  
                    // Update NumElems, so there is no need to implement NumElems update in the UseFunction
                    let newNumElems = if bNullReturn then 0 else newElemArray.Length 
                    if x.SerializationLimit<=0 then 
                        // No reserialization
                        let newMeta = x.GetMetadataForPartition( retMeta, retMeta.Parti, newNumElems )
                        outputSeq.Add( newMeta, newElemArray :> Object )
                    else
                        let mutable pos = 0 
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Add %d Elems to part %d" newNumElems retMeta.Partition ))
                        let holdElemRef = ref null
                        while not (x.holdElemArray.TryGetValue( retMeta.Parti, holdElemRef )) do
                            // initialization
                            x.holdElemArray.TryAdd( retMeta.Parti, List<_>() ) |> ignore
                        let holdElem = !holdElemRef
                        if Utils.IsNull holdElem then 
                            Logger.Log( LogLevel.Error, "holdElem should not be null, should be at least List<_>()" )
                        if holdElem.Count > 0 then 
                            // There is prior information, note that holdKey and holdValue should have less than SerializationLimit worth of data. 
                            let extraNeeded = x.SerializationLimit - (holdElem.Count)
                            pos <- Math.Min( extraNeeded, newNumElems )
                            holdElem.AddRange( Array.sub newElemArray 0 pos )
                            if holdElem.Count >= x.SerializationLimit then 
                                let useMeta = x.GetMetadataForPartition( retMeta, retMeta.Parti, x.SerializationLimit )
                                outputSeq.Add( useMeta, holdElem.ToArray() :> Object  )
                                holdElem.Clear()
                        if holdElem.Count = 0 then 
                            // Output remaining of the array, otherwise, the entire output sequence has been absorted to holdKey and holdValue, 
                            // no need to output remaining of the array. 
                            while pos + x.SerializationLimit <= newNumElems do
                                // There is at least a full serialization limit of keys and values available. 
                                // Use a new metadata, so that each metadata have different parameter
                                let useMeta = x.GetMetadataForPartition( retMeta, retMeta.Parti, x.SerializationLimit )
                                outputSeq.Add( useMeta, Array.sub newElemArray pos x.SerializationLimit :> Object )
                                pos <- pos + x.SerializationLimit
                            if pos < newNumElems then 
                                holdElem.AddRange( Array.sub newElemArray pos (newNumElems-pos) )
            else
                let newMeta, newElemArray = if x.CallNullAtFinal then x.UseFunc( meta, null ) else meta, null
                let bNullResult = Utils.IsNull newElemArray
                let newNumElems = if bNullResult then 0 else newElemArray.Length 
                if true then 
                    if x.SerializationLimit<=0 then 
                        // something is pushed out by final use func, adds it. 
                        // It is possible to have wrong metadata here. 
                        if newNumElems>0 then 
                            let newMeta = x.GetMetadataForPartition( newMeta, newMeta.Parti, newNumElems )
                            outputSeq.Add( newMeta, newElemArray :> Object )
                        // Allow NumElems to go through
                        let finalMeta = x.GetMetadataForPartition( newMeta, newMeta.Parti, 0 )
                        outputSeq.Add( finalMeta, null )
                    else
                        let mutable pos = 0 
                        let holdElemRef = ref null
                        while not (x.holdElemArray.TryGetValue( newMeta.Parti, holdElemRef )) do
                            // initialization
                            x.holdElemArray.TryAdd( newMeta.Parti, List<_>() ) |> ignore
                        let holdElem = !holdElemRef
                        if holdElem.Count > 0  then 
                            // There is prior information, note that holdKey and holdValue should have less than SerializationLimit worth of data. 
                            let extraNeeded = x.SerializationLimit - (holdElem.Count)
                            pos <- Math.Min( extraNeeded, newNumElems )
                            if pos > 0 then 
                                holdElem.AddRange( Array.sub newElemArray 0 pos )
                            let useMeta = x.GetMetadataForPartition( newMeta, newMeta.Parti, holdElem.Count )
                            // Final, always output what is remain in holdKey and holdValue
                            outputSeq.Add( useMeta, (holdElem.ToArray() :> Object) )
                            let pos1 = pos
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Reorg flush parti (%d)%d, %d Elems with %d Elems pending (pos=%d)" newMeta.Partition useMeta.Partition holdElem.Count newNumElems pos1 ))
                            holdElem.Clear() 

                        // Output remaining of the array, otherwise, the entire output sequence has been absorted to holdKey and holdValue, 
                        // no need to output remaining of the array. 
                        while pos + x.SerializationLimit <= newNumElems do
                            // There is at least a full serialization limit of keys and values available. 
                            let useMeta = x.GetMetadataForPartition( newMeta,  newMeta.Parti, x.SerializationLimit )
                            outputSeq.Add( useMeta, Array.sub newElemArray pos x.SerializationLimit :> Object )
                            pos <- pos + x.SerializationLimit
                        if pos < newNumElems then 
                            // Any left over data? 
                            let useMeta = x.GetMetadataForPartition( newMeta, newMeta.Parti, newNumElems-pos )
                            outputSeq.Add( useMeta, Array.sub newElemArray pos (newNumElems-pos) :> Object )
                            pos <- newNumElems
                        // Final output
                        let finalMeta = x.GetMetadataForPartition( newMeta, newMeta.Parti, 0 )
                        outputSeq.Add( finalMeta, null )
            match mapTo with 
            | MapToKind.OBJECT -> 
                outputSeq |> Seq.cast
            | MapToKind.MEMSTREAM -> 
                outputSeq |> Seq.map ( fun (meta, o) -> let cmeta, cms = x.Encode( meta, o )
                                                        cmeta, cms :> Object )
            | _ -> 
                let msg = sprintf "Error in FunctionWrapper<'U, 'U1 >.WrapperMapFunc, unsupported mapTo %A" mapTo
                Logger.Log( LogLevel.Error, msg )
                failwith msg                                        
        with 
        | e -> 
            let msg = sprintf "Error in FunctionWrapper<'U, 'U1 >.WrapperMapFunc, with exception %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg          

/// ----------------------------------------------------------------------------------------------------------------------------------------------
/// Used for .mix and mixby 
/// ----------------------------------------------------------------------------------------------------------------------------------------------
[<AllowNullLiteral; Serializable>]
type internal MixFunctionWrapper<'V>(num, bTerminateWhenAnyParentReachEnd ) as x =
    inherit MetaFunction<'V>()
    let nullDerivedExecute (parti:int) (numElems:int) : seq<BlobMetadata*Object> = 
        Seq.empty
    do 
        x.DepositFunc <- x.DepositForMix
        x.ExecuteFunc <- x.WrapperExecuteFunc
    member val BlockOnMixDeposit = DeploymentSettings.BlockOnMixDeposit with get, set
    member val internal DerivedExecuteFunc = nullDerivedExecute with get, set
    member val internal CastFunc = Array.zeroCreate< Object -> Object > num with get, set
    member val internal DepositBuffer = null with get, set
    member val internal DepositNumElems = null with get, set
    member val internal DepositLength = null with get, set
    member val internal DepositEndReached = null with get, set
    member val internal DepositAllEndReached = null with get, set
    member val internal DepositWaitHandle = null with get, set
    member val internal ExecutionWaitHandle = null with get, set
    override x.Reset() = 
        x.DepositBuffer <- null
        x.UnblockAll() // This needs to be follow the x.DepositBuffer <- null
                       // Everytime we wait on handle, we check if x.DepositBuffer is null. 
                       // The thread will not block if x.DepositBuffer is null. 
        x.BaseReset() 
        x.DepositNumElems <- null
        x.DepositLength <- null
        x.DepositEndReached <- null
        x.DepositAllEndReached <- null 
        x.DepositWaitHandle <- null
        x.ExecutionWaitHandle <- null
    override x.InitAll() = 
        if Utils.IsNull x.DepositBuffer then 
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.DepositBuffer then 
                    x.DepositBuffer <- Array.init<_> num ( fun _ -> ConcurrentDictionary<_,ConcurrentQueue<BlobMetadata*Object>>() )
                    x.DepositNumElems <- Array.init<_> num ( fun _ -> ConcurrentDictionary<int,int>() )
                    x.DepositLength <- Array.init<_> num ( fun _ -> ConcurrentDictionary<int,int64>() )
                    x.DepositEndReached <- Array.init<_> num ( fun _ -> ConcurrentDictionary<_,bool>() )
                    x.DepositAllEndReached <- ConcurrentDictionary<_,bool>()
                    x.DepositWaitHandle <- ConcurrentDictionary<_,ManualResetEvent[]>()
                    x.ExecutionWaitHandle <- ConcurrentDictionary<_,ManualResetEvent>()
            )
    // Set all handles to true, those unblock everything
    override x.UnblockAll() = 
        // use local variable, so that if some other threads calls Reset(), the current function can still execute without exception
        let depositWaitHandle = x.DepositWaitHandle
        let executionWaitHandle = x.ExecutionWaitHandle
        if Utils.IsNotNull depositWaitHandle then 
            x.DepositWaitHandle <- null
            for tuple in depositWaitHandle do
                let handles = tuple.Value
                for handle in handles do 
                    handle.Set() |> ignore
        if Utils.IsNotNull executionWaitHandle then 
            x.ExecutionWaitHandle <- null
            for tuple in executionWaitHandle do
                let handle = tuple.Value
                handle.Set() |> ignore
    member internal x.DepositForMix parenti (meta, o:Object ) = 
        let executionWaitHandle = x.ExecutionWaitHandle
        let depositEndReached = x.DepositEndReached
        if Utils.IsNull o then            
            Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "DepositForMix: partition %i, o = null" meta.Partition)
            let depositAllEndReached = x.DepositAllEndReached
            if Utils.IsNull x.DepositBuffer then 
                // Reset has been called, do nothing. 
                ()
            else
                // End reached
                let dicEnd = depositEndReached.[parenti]
                dicEnd.Item( meta.Partition ) <- true
                // Verify if End is reached for all partitions 
                let bAllEndReached = ref true
                if not bTerminateWhenAnyParentReachEnd then 
                    let mutable pi = 0 
                    while (!bAllEndReached) && pi < num do 
                        let dicEndi = depositEndReached.[pi]
                        if not (dicEndi.TryGetValue( meta.Partition, bAllEndReached )) then 
                            bAllEndReached := false
                        pi <- pi + 1 
                if ( !bAllEndReached ) then 
                    Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "DepositForMix: partition %i, all end reached" meta.Partition)
                    depositAllEndReached.Item( meta.Partition ) <- true   
        else
            Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "DepositForMix: partition %i, o <> null" meta.Partition)
            let depositBuffer = x.DepositBuffer
            let depositNumElems = x.DepositNumElems
            let depositLength = x.DepositLength
            if Utils.IsNull x.DepositBuffer then 
                // Reset has been called, do nothing
                () 
            else
                let dicEnd = depositEndReached.[parenti]
                let bEndReached = ref false
                if dicEnd.TryGetValue( meta.Partition, bEndReached ) && !bEndReached then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "DepositForMix, end marked for parent %d partition %d but deposit still coming in %s" 
                                                                   parenti meta.Partition (meta.ToString()) ))
                else
                    // If blocked on mix, wait below
                    x.ToBlockOnDeposit parenti meta.Partition 
                    if Utils.IsNotNull x.DepositBuffer then 
                        Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "DepositForMix: partition %i, deposit" meta.Partition)
                        let co = x.CastFunc.[parenti]( o )
                        let dic = depositBuffer.[parenti]
                        let addFunc _ =
                            let lst = ConcurrentQueue<_>()
                            lst.Enqueue( (meta, co) )
                            lst
                        let updateFunc _ (lst:ConcurrentQueue<_>) = 
                            let newLst = if Utils.IsNotNull lst then lst else ConcurrentQueue<_>() 
                            newLst.Enqueue( (meta, co) )
                            newLst
                        let lst = dic.AddOrUpdate( meta.Partition, addFunc, updateFunc )
                        // Count # of Key-Values in o 
                        let elemArray = co :?> System.Array
                        let dicNumElems = depositNumElems.[parenti]
                        let valueUpdate pi existLen = 
                            existLen + elemArray.Length
                        dicNumElems.AddOrUpdate( meta.Partition, elemArray.Length, valueUpdate ) |> ignore
                        let dicLength = depositLength.[parenti]
                        let lenUpdate pi existLen = 
                            existLen + int64 meta.BlobLength
                        dicLength.AddOrUpdate( meta.Partition, int64 meta.BlobLength, lenUpdate ) |> ignore
        if Utils.IsNotNull executionWaitHandle then 
            let _, numElemsCanExecute = x.NumElemsToExecute meta.Partition
            if x.CanExecute numElemsCanExecute meta.Partition then 
                let handle = executionWaitHandle.GetOrAdd( meta.Partition, fun _ -> new ManualResetEvent(false))
                handle.Set() |> ignore // Unblock execution
                Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "DepositForMix: Set handle for partition %i" meta.Partition)
    member internal x.NumElemsToExecute parti =
        let depositNumElems = x.DepositNumElems
        let depositEndReached = x.DepositEndReached

        if Utils.IsNotNull depositNumElems && Utils.IsNotNull depositEndReached then 
            // numElems = min of pNumElems from parents, where pNumElems = In32.MaxValue if # of elems = 0 and reached end, otherwise # of elems
            // The result will be used by x.DerivedExecuteFunc
            let mutable numElems = Int32.MaxValue
            // numElemsCanExecute = min pNumElemsCanExecute from parents, where pNumElemsCanExecute = In32.MaxValue if reached end, otherwise # of elems
            // The result will be used by x.CanExecute. 
            // Note: In the case where some parent has "0 < # of elems < SerializationLimit and reached end"
            //       numElems = # of elems, numElemsCanExecute = Int32.MaxValue
            //       If "numElems" are given to x.CanExecute, it would return false, and stall the execution until all parents reached end
            //       By giving numElemsCanExecute to x.CanExecute would allow progress
            let mutable numElemsCanExecute = Int32.MaxValue
            for pi = 0 to num - 1 do 
                let dicNumElems = depositNumElems.[pi]
                let dicEnd = depositEndReached.[pi]
                let reachedEnd = ref false
                dicEnd.TryGetValue( parti, reachedEnd ) |> ignore
                let retVal = ref 0
                dicNumElems.TryGetValue( parti, retVal ) |> ignore
                let pNumElems = if !retVal = 0 && !reachedEnd then Int32.MaxValue else !retVal
                numElems <- Math.Min( numElems, pNumElems )
                let pNumElemsCanExecute = if !reachedEnd then Int32.MaxValue else !retVal
                numElemsCanExecute <- Math.Min( numElemsCanExecute, pNumElemsCanExecute )
            numElems, numElemsCanExecute
        else
            0, 0
    member internal x.CanExecute numElems parti = 
        let cmpVal = if x.SerializationLimit<=0 then 1 else x.SerializationLimit
        if numElems >= cmpVal then 
            // Note: when any parent has reached the end, numElems = Int32.MaxValue, thus >= cmpVal, thus can execute
            true
        else if Utils.IsNull x.DepositBuffer then 
            true
        else
            false

    // JinL: 2/26/2015, this may cause deadlock, need to rethink logic. 
    member internal x.ToBlockOnDeposit parenti parti = 
            let depositWaitHandle = x.DepositWaitHandle
            if Utils.IsNotNull depositWaitHandle then 
                if x.bBlockOnDeposit parenti parti then 
                    // If Reset is called, bBlock will return false
                    if Utils.IsNotNull x.DepositBuffer then 
                        // The statement "Utils.IsNotNull x.DepositBuffer" is necessary here to make sure that UnblockAll() has not been called 
                        // This avoids the deadlock
                        let handles = depositWaitHandle.GetOrAdd( parenti, fun _ -> Array.init num ( fun _ -> new ManualResetEvent(false)) )
                        ThreadPoolWaitHandles.safeWaitOne( handles.[parenti], shouldReset = true ) |> ignore
    member internal x.bBlockOnDeposit parenti parti = 
        let depositNumElems = x.DepositNumElems
        let depositLengths = x.DepositLength
        let depositWaitHandle = x.DepositWaitHandle
        let depositEndReached = x.DepositEndReached
        if Utils.IsNull x.DepositBuffer then 
            // Reset is called, not blocking
            false
        else
            // We assume end is not reached, otherwise, this function will not be called. 
            let retVal = ref 0
            let dicNumElems = depositNumElems.[parenti]
            if dicNumElems.TryGetValue( parti, retVal ) && (!retVal)>=Math.Max( 1, x.SerializationLimit) then 
                // The partition can be executed, further test is in order. 
                let dicLength = depositLengths.[parenti]
                let retLength = ref 0L
                if dicLength.TryGetValue( parti, retLength ) && (!retLength)>=x.BlockOnMixDeposit then 
                    true
                else
                    false
            else    
                false

    // Execute numElems, we assume that there is numElems in parenti & parti
    // Return an array of numElems element, consolidate the rest of the element back to list for subsequent access. 
    member internal x.ToExecute<'U> parenti parti numElems = 
        let depositNumElems = x.DepositNumElems
        let depositLengths = x.DepositLength
        let depositWaitHandle = x.DepositWaitHandle
        let depositBuffer = x.DepositBuffer
        if Utils.IsNotNull x.DepositBuffer then 
            let dic = depositBuffer.[parenti]
            let retVal = ref null // Hold the array to be returned 
            let retMeta = ref Unchecked.defaultof<_> 
            let executeAdd parti = 
                null
            let executeUpdate (parti:int) (queue:ConcurrentQueue<BlobMetadata*Object>) = 
                let tuple0 = ref Unchecked.defaultof<_>
                if Utils.IsNotNull queue && queue.TryDequeue( tuple0 ) then 
                    // Something in queue, get the 1st element. 
                    let meta0, o0 = !tuple0
                    retMeta := meta0
                    let elems0Array = o0 :?> ('U[]) 
                    if elems0Array.Length = numElems || numElems=Int32.MaxValue then 
                        // Great we only need to dequeue first element
                        retVal := elems0Array
                        let dicNumElems = depositNumElems.[parenti] 
                        dicNumElems.AddOrUpdate( parti, 0, fun pi existLen -> existLen - numElems ) |> ignore
                        let dicLengths = depositLengths.[parenti]
                        dicLengths.AddOrUpdate( parti, 0L, fun pi existLen -> existLen - int64 meta0.BlobLength ) |> ignore
                        queue
                    else
                        retVal := Array.zeroCreate<_> numElems
                        let mutable index = 0
                        let mutable numLeft = 0
                        let mutable bQueueEmpty = false
                        while index < numElems && not bQueueEmpty do
                            let meta0, o0 = !tuple0
                            let elems0Array = o0 :?> ('U[]) 
                            let numCopy = Math.Min( numElems - index, elems0Array.Length ) 
                            Array.Copy( elems0Array, 0, !retVal, index, numCopy ) 
                            index <- index + numCopy 
                            numLeft <- elems0Array.Length - numCopy
                            if numLeft = 0 then 
                                if index < numElems then 
                                    bQueueEmpty <- not (queue.TryDequeue( tuple0 ))
                                let dicNumElems = depositNumElems.[parenti] 
                                dicNumElems.AddOrUpdate( parti, 0, fun pi existLen -> existLen - numCopy ) |> ignore
                                let dicLengths = depositLengths.[parenti]
                                dicLengths.AddOrUpdate( parti, 0L, fun pi existLen -> existLen - int64 meta0.BlobLength ) |> ignore
                            else
                                // numLeft > 0, index should be equal to numElems
                                ()        
                        if index > numElems then 
                            let msg = sprintf "MixFunctionWrapper.ToExecute, logic error of too many keys, we exepect %d Keys for parentDSet %d, part %d, but find %d Keys" numElems parenti parti index 
                            failwith msg
                        elif index < numElems && numElems<Int32.MaxValue then 
                            let msg = sprintf "MixFunctionWrapper.ToExecute, logic error of too few keys, we exepect %d Keys for parentDSet %d, part %d, but find %d Keys" numElems parenti parti index 
                            failwith msg
                        else
                            if numLeft = 0 then 
                                queue
                            else
                                let newqueue = ConcurrentQueue<_>()
                                let meta0, o0 = !tuple0
                                let elems0Array = o0 :?> ('U[]) 
                                let remainingArray = Array.sub elems0Array (elems0Array.Length - numLeft) numLeft
                                let numUsed = elems0Array.Length - numLeft
                                let blobLenLeft = int ( int64 meta0.BlobLength * int64 numLeft / int64 elems0Array.Length )
                                let blobLenUsed = meta0.BlobLength - blobLenLeft
                                let updateMeta = BlobMetadata( meta0, meta0.Partition, meta0.Serial + int64 (elems0Array.Length - numLeft), numLeft, blobLenLeft )
                                newqueue.Enqueue( (updateMeta, remainingArray:> Object) ) 
                                let dicNumElems = depositNumElems.[parenti] 
                                dicNumElems.AddOrUpdate( parti, 0, fun pi existLen -> existLen - numUsed ) |> ignore
                                let dicLengths = depositLengths.[parenti]
                                dicLengths.AddOrUpdate( parti, 0L, fun pi existLen -> existLen - int64 blobLenUsed ) |> ignore
                                while queue.TryDequeue( tuple0 ) do 
                                    newqueue.Enqueue( !tuple0 )    
                                newqueue
                else
                    let msg = sprintf "MixFunctionWrapper.ToExecute, logic error of empty queue, we exepect %d Keys for parentDSet %d, part %d, but find none" numElems parenti parti 
                    failwith msg
            dic.AddOrUpdate( parti, executeAdd, executeUpdate ) |> ignore
            if not( x.bBlockOnDeposit parenti parti ) then 
                let handles = depositWaitHandle.GetOrAdd( parenti, fun _ -> Array.init num (fun _ ->  new ManualResetEvent(false)))
                (handles).[parenti].Set() |> ignore
            !retMeta, !retVal 
        else
            x.GetFinalMetadata parti, null
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperExecuteFunc parti = 
        try
            let depositAllEndReached = x.DepositAllEndReached
            let executionWaitHandle = x.ExecutionWaitHandle
            let numElems, numElemsCanExecute = x.NumElemsToExecute parti
            if x.CanExecute numElemsCanExecute parti then 
                if numElems > 0 && numElems <> Int32.MaxValue then
                    x.DerivedExecuteFunc parti numElems
                else
                    // Can execute, but numElems is 0, we may reached the end 
                    let bAllEndReached = ref false
                    if Utils.IsNull x.DepositBuffer || ( depositAllEndReached.TryGetValue( parti, bAllEndReached ) && !bAllEndReached ) then 
                        Seq.singleton ((x.GetFinalMetadata parti), null)
                    else
                        Seq.empty
            else
                if Utils.IsNotNull executionWaitHandle then
                    let numElems, numElemsCanExecute = x.NumElemsToExecute parti
                    if not (x.CanExecute numElemsCanExecute parti) then 
                        // If CanExecute remains false, go ahead to block. Otherwise, a "Set" may have happened before handle.Reset, thus a signal is lost, should not block
                        if Utils.IsNotNull x.DepositBuffer then 
                            // The statement "Utils.IsNotNull x.DepositBuffer" is necessary here to make sure that UnblockAll() has not been called 
                            // This avoids the deadlock
                            let handle = executionWaitHandle.GetOrAdd( parti, fun _ -> new ManualResetEvent(false) )
                            // block on execution
                            Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "WrapperExecuteFunc: safeWaitOne on handle for partition %i" parti)
                            ThreadPoolWaitHandles.safeWaitOne( handle, shouldReset = true ) |> ignore
                            Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "WrapperExecuteFunc: safeWaitOne on handle for partition %i returned" parti)
                    Seq.empty
                else
                    Seq.empty
        with 
        | e -> 
            let msg = sprintf "Error in WrapperExecuteFunc, with exception %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg                                        


[<AllowNullLiteral; Serializable>]
type internal CorrelatedMix2FunctionWrapper<'U0, 'U1, 'V>( bTerminateWhenAnyParentReachEnd, func: 'U0 -> 'U1 -> 'V ) as x =   
    inherit MixFunctionWrapper<'V>(2, bTerminateWhenAnyParentReachEnd)
    do 
        x.CastFunc.[0] <- CastFunction<'U0>.ForceTo
        x.CastFunc.[1] <- CastFunction<'U1>.ForceTo
        x.DerivedExecuteFunc <- x.WrapperDerivedExecuteFunc
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperDerivedExecuteFunc parti numElems = 
        let m0, elem0 = x.ToExecute<'U0> 0 parti numElems
        let m1, elem1 = x.ToExecute<'U1> 1 parti numElems
        if Utils.IsNull elem0 || Utils.IsNull elem1 then 
            Seq.singleton (m1, null)
        else
            let retElem = Array.zeroCreate<_> numElems
            for i = 0 to numElems - 1 do 
                retElem.[i] <- func elem0.[i] elem1.[i]
            let retMeta = x.GetMetadataForPartition( m0, parti, numElems )
            Seq.singleton (retMeta, retElem :> Object)           

[<AllowNullLiteral; Serializable>]
type internal CorrelatedMix3FunctionWrapper<'U0, 'U1, 'U2, 'V>( bTerminateWhenAnyParentReachEnd, func: 'U0 -> 'U1 -> 'U2 -> 'V ) as x =   
    inherit MixFunctionWrapper<'V>(3, bTerminateWhenAnyParentReachEnd)
    do 
        x.CastFunc.[0] <- CastFunction<'U0>.ForceTo
        x.CastFunc.[1] <- CastFunction<'U1>.ForceTo
        x.CastFunc.[2] <- CastFunction<'U2>.ForceTo
        x.DerivedExecuteFunc <- x.WrapperDerivedExecuteFunc
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperDerivedExecuteFunc parti numElems = 
        let m0, elem0 = x.ToExecute<'U0> 0 parti numElems
        let m1, elem1 = x.ToExecute<'U1> 1 parti numElems
        let m2, elem2 = x.ToExecute<'U2> 2 parti numElems
        if Utils.IsNull elem0 || Utils.IsNull elem1 || Utils.IsNull elem2 then 
            Seq.singleton (m2, null)
        else
            let retElem = Array.zeroCreate<_> numElems
            for i = 0 to numElems - 1 do 
                retElem.[i] <- func elem0.[i] elem1.[i] elem2.[i]
            let retMeta = x.GetMetadataForPartition( m0, parti, numElems )
            Seq.singleton (retMeta, retElem :> Object)

[<AllowNullLiteral; Serializable>]
type internal CorrelatedMix4FunctionWrapper<'U0, 'U1, 'U2, 'U3, 'V>( bTerminateWhenAnyParentReachEnd, func: 'U0 -> 'U1 -> 'U2 -> 'U3 -> 'V ) as x =   
    inherit MixFunctionWrapper<'V>(4, bTerminateWhenAnyParentReachEnd)
    do 
        x.CastFunc.[0] <- CastFunction<'U0>.ForceTo
        x.CastFunc.[1] <- CastFunction<'U1>.ForceTo
        x.CastFunc.[2] <- CastFunction<'U2>.ForceTo
        x.CastFunc.[3] <- CastFunction<'U3>.ForceTo
        x.DerivedExecuteFunc <- x.WrapperDerivedExecuteFunc
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperDerivedExecuteFunc parti numElems = 
        let m0, elem0 = x.ToExecute<'U0> 0 parti numElems
        let m1, elem1 = x.ToExecute<'U1> 1 parti numElems
        let m2, elem2 = x.ToExecute<'U2> 2 parti numElems
        let m3, elem3 = x.ToExecute<'U3> 3 parti numElems
        if Utils.IsNull elem0 || Utils.IsNull elem1 
            || Utils.IsNull elem2 || Utils.IsNull elem3 then 
            Seq.singleton (m3, null)
        else
            let retElem = Array.zeroCreate<_> numElems
            for i = 0 to numElems - 1 do 
                retElem.[i] <- func elem0.[i] elem1.[i] elem2.[i] elem3.[i]
            let retMeta = x.GetMetadataForPartition( m0, parti, numElems )
            Seq.singleton (retMeta, retElem :> Object)

/// used for sortedJoin and joinBykey
[<AllowNullLiteral; Serializable>]
type internal JoinByMergeFunctionWrapper<'V>(num, bTerminateWhenAnyParentReachEnd ) =
    inherit MixFunctionWrapper<'V>(num, bTerminateWhenAnyParentReachEnd ) 
    // Get a 'U[] that is from parenti & parti
    member internal x.GetBlob<'U> parenti parti = 
        let depositBuffer = x.DepositBuffer
        if Utils.IsNotNull x.DepositBuffer then 
            let dic = depositBuffer.[parenti]
            let retVal = ref null // Hold the array to be returned 
            let retMeta = ref Unchecked.defaultof<_> 
            let executeUpdate (parti:int) (queue:ConcurrentQueue<BlobMetadata*Object>) = 
                let tuple0 = ref Unchecked.defaultof<_>
                if Utils.IsNotNull queue && queue.TryDequeue( tuple0 ) then 
                    // Something in queue, get the 1st element. 
                    let meta0, o0 = !tuple0
                    retMeta := meta0
                    retVal := o0 :?> ('U[]) 
                else
                    retMeta := x.GetFinalMetadata parti
                    retVal := null
                queue
            dic.AddOrUpdate( parti, ( fun _ -> null), executeUpdate ) |> ignore
            !retMeta, !retVal, 0 
        else
            x.GetFinalMetadata parti, null, 0
    member internal x.UseBlob<'U> parenti parti (meta0:BlobMetadata) (elems0Array:'U[]) numLastTime numCurrent = 
        let depositNumElems = x.DepositNumElems
        let depositLengths = x.DepositLength
        let depositWaitHandle = x.DepositWaitHandle
        let depositBuffer = x.DepositBuffer
        if Utils.IsNotNull depositBuffer then 
            let numUsed = numCurrent - numLastTime
            let dicNumElems = depositNumElems.[parenti] 
            dicNumElems.AddOrUpdate( parti, 0, fun pi existLen -> existLen - numUsed ) |> ignore
            let dicLengths = depositLengths.[parenti]
            let blobLenUsed = int ( int64 meta0.BlobLength * int64 numCurrent / int64 elems0Array.Length - int64 meta0.BlobLength * int64 numLastTime / int64 elems0Array.Length )
            dicLengths.AddOrUpdate( parti, 0L, fun pi existLen -> existLen - int64 blobLenUsed ) |> ignore
            if not( x.bBlockOnDeposit parenti parti ) then 
                // Unblock 
                let handles = depositWaitHandle.GetOrAdd( parenti, fun _ -> Array.init num ( fun _ -> new ManualResetEvent(false)) )
                handles.[parenti].Set() |> ignore  
    member internal x.StoreBackBlob<'U> parenti parti (meta0:BlobMetadata) (elems0Array:'U[]) numUsed =        
        let depositBuffer = x.DepositBuffer
        if Utils.IsNotNull elems0Array && Utils.IsNotNull depositBuffer && elems0Array.Length > numUsed then 
            let executeUpdate (parti:int) (queue:ConcurrentQueue<BlobMetadata*Object>) = 
                let newqueue = ConcurrentQueue<_>()
                let remainingArray = Array.sub elems0Array numUsed (elems0Array.Length - numUsed)
                let numLeft = elems0Array.Length - numUsed 
                let blobLenUsed = int ( int64 meta0.BlobLength * int64 numUsed / int64 elems0Array.Length )
                let blobLenLeft = meta0.BlobLength - blobLenUsed
                let updateMeta = BlobMetadata( meta0, meta0.Partition, meta0.Serial + int64 (elems0Array.Length - numLeft), numLeft, blobLenLeft )
                newqueue.Enqueue( (updateMeta, remainingArray:> Object) ) 
                if Utils.IsNotNull queue then do
                    let tuple0 = ref Unchecked.defaultof<_>
                    while queue.TryDequeue( tuple0 ) do 
                        newqueue.Enqueue( !tuple0 ) 
                newqueue 
            let dic = depositBuffer.[parenti]
            dic.AddOrUpdate( parti, ( fun _ -> null) , executeUpdate ) |> ignore

[<AllowNullLiteral; Serializable>]
type internal JoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func: 'U0->'K, k1Func:'U1->'K, comp:IComparer<'K> ) as x =   
    inherit JoinByMergeFunctionWrapper<'V>(2, bTerminateWhenAnyParentReachEnd)
    let nullJoinFunc (lst:List<'V>) (u0:'U0) (u1:'U1) = 
        ()
    let nullLeftJoinFunc (lst:List<'V>) (u0:'U0) = 
        ()
    let nullRightJoinFunc (lst:List<'V>) (u1:'U1) = 
        ()
    do 
        x.CastFunc.[0] <- CastFunction<'U0>.ForceTo
        x.CastFunc.[1] <- CastFunction<'U1>.ForceTo
        x.DerivedExecuteFunc <- x.WrapperDerivedExecuteFunc
    member val internal InnerJoin = nullJoinFunc with get, set
    member val internal LeftOuterJoin = nullLeftJoinFunc with get, set
    member val internal RightOuterJoin = nullRightJoinFunc with get, set
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperDerivedExecuteFunc parti numElems = 
        let blob0 = (x.GetBlob<'U0> 0 parti)
        let blob1 = (x.GetBlob<'U1> 1 parti)
        let mutable bDone = false
        let meta0, k0, c0 = blob0
        let meta1, k1, c1 = blob1
        let baseMeta = if Utils.IsNotNull k0 then meta0 else meta1

        let retElem = List<'V>( numElems )

        let mutable m0 = meta0
        let mutable m1 = meta1
        let mutable elem0 = k0
        let mutable elem1 = k1
        let mutable p0 = c0
        let mutable p1 = c1

        while not bDone do
       
            if Utils.IsNull elem0 && Utils.IsNull elem1 then 
                // Both are empty
                bDone <- true
            elif Utils.IsNull elem0 then
                // only elem1 has elems
                x.RightOuterJoin retElem elem1.[p1]
                p1 <- p1 + 1
            elif Utils.IsNull elem1 then
                // only elem0 has elems
                x.LeftOuterJoin retElem elem0.[p0]
                p0 <- p0 + 1
            else
                // both have Elems
                let v0 = elem0.[p0]
                let v1 = elem1.[p1]
                let cmpVal = comp.Compare( k0Func v0, k1Func v1 )
                if cmpVal = 0 then
                    x.InnerJoin retElem v0 v1
                    p0 <- p0 + 1
                    p1 <- p1 + 1
                elif cmpVal < 0 then 
                     x.LeftOuterJoin retElem v0
                     p0 <- p0 + 1
                else 
                     x.RightOuterJoin retElem v1
                     p1 <- p1 + 1
            
            if retElem.Count >= x.SerializationLimit then 
                bDone <- true

            if not bDone then
                if Utils.IsNotNull elem0 && p0 >= elem0.Length then
                    x.UseBlob<'U0> 0 parti m0 elem0 c0 p0
                    let meta0, k0, c0 = x.GetBlob<'U0> 0 parti
                    m0 <- meta0
                    elem0 <- k0
                    p0 <- c0
                if Utils.IsNotNull elem1 && p1 >= elem1.Length then
                    x.UseBlob<'U1> 1 parti m1 elem1 c1 p1
                    let meta1, k1, c1 = x.GetBlob<'U1> 1 parti
                    m1 <- meta1
                    elem1 <- k1
                    p1 <- c1
            else
                if Utils.IsNotNull elem0 then
                    x.UseBlob<'U0> 0 parti m0 elem0 c0 p0
                if Utils.IsNotNull elem1 then
                    x.UseBlob<'U1> 1 parti m1 elem1 c1 p1

        x.StoreBackBlob<'U0> 0 parti m0 elem0 p0
        x.StoreBackBlob<'U1> 1 parti m1 elem1 p1
        if retElem.Count > 0 then 
            let retMeta = x.GetMetadataForPartition( baseMeta, parti, retElem.Count )
            let retArr = retElem.ToArray()
            Seq.singleton( retMeta, retArr :> Object )
        else
            Seq.empty

[<AllowNullLiteral; Serializable>]
type internal InnerJoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func: 'U0->'K, k1Func:'U1->'K, comp:IComparer<'K>, func: 'U0 -> 'U1 -> 'V ) as x =   
    inherit JoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func, k1Func, comp )
    let innerJoin (lst:List<'V>) (u0:'U0) (u1:'U1) = 
        lst.Add( func u0 u1 )
    do 
        x.InnerJoin <- innerJoin    

[<AllowNullLiteral; Serializable>]
type internal LeftOuterJoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func: 'U0->'K, k1Func:'U1->'K, comp:IComparer<'K>, func: 'U0 -> 'U1 option -> 'V ) as x =   
    inherit JoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func, k1Func, comp )
    let innerJoin (lst:List<'V>) (u0:'U0) (u1:'U1) = 
        lst.Add( func u0 (Some u1) )
    let leftOuterJoin (lst:List<'V>) (u0:'U0) = 
        lst.Add( func u0 (None) )
    do 
        x.InnerJoin <- innerJoin    
        x.LeftOuterJoin <- leftOuterJoin

[<AllowNullLiteral; Serializable>]
type internal RightOuterJoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func: 'U0->'K, k1Func:'U1->'K, comp:IComparer<'K>, func: 'U0 option -> 'U1 -> 'V ) as x =   
    inherit JoinByMergeFunctionWrapper<'U0, 'U1, 'V, 'K>( bTerminateWhenAnyParentReachEnd, k0Func, k1Func, comp )
    let innerJoin (lst:List<'V>) (u0:'U0) (u1:'U1) = 
        lst.Add( func (Some u0) u1 )
    let rightOuterJoin (lst:List<'V>) (u1:'U1) = 
        lst.Add( func None u1 )
    do 
        x.InnerJoin <- innerJoin    
        x.RightOuterJoin <- rightOuterJoin


/// ----------------------------------------------------------------------------------------------------------------------------------------------
/// Used for Cross Join
/// ----------------------------------------------------------------------------------------------------------------------------------------------
/// It uses three functions:
/// DepositFunc parti (meta, o): 
///     Deposit outer object before loop of inner object starts
/// MapFunc (meta, o) 
///     Execute inner cross join loop, note here the object is further wrapped as (parti, o) to pass the parti information. 
/// ExecuteFunc parti (meta, o)
///     Final function for cross join, after all the partition has been executed upon. 
[<AllowNullLiteral; Serializable>]
type internal DepositFunctionWrapper<'U0, 'U>() as x =
    inherit MetaFunction<'U>()
    do 
        x.DepositFunc <- x.DepositBlob
    member val BufferInitialized = ref 0 with get
    member val DepositBuffer = null with get, set
    member x.DepositOneReset() = 
        x.BufferInitialized := 0
        x.DepositBuffer <- null       
    member x.DepositOneInitAll() = 
        if Utils.IsNull x.DepositBuffer then 
            if Interlocked.CompareExchange( x.BufferInitialized, 1, 0 )=0 then 
                x.DepositBuffer <- ConcurrentDictionary<_,_>()
                true
            else
                false
        else
            false
    override x.Reset() = 
        x.DepositOneReset()
    override x.InitAll() = 
        x.DepositOneInitAll() |> ignore
    member internal x.DepositBlob parenti (meta, o:Object ) = 
        let uArray = if Utils.IsNull o then null else ( CastFunction<'U0>.CastTo o )
        x.DepositBuffer.Item( parenti ) <- ( meta, uArray )

[<AllowNullLiteral; Serializable>]
type internal CrossJoinChooseFunctionWrapper<'U0,'U1,'U>(mapFunc: 'U0->'U1->'U option) as x =
    inherit DepositFunctionWrapper<'U0, 'U>()
    do
        x.MapFunc <- x.CrossJoinChooseInnerMapFunc
    member val internal ReturnFunc = x.DefaultMapReturnFunc with get, set
    member x.DefaultMapReturnFunc (meta:BlobMetadata) (resultArray:'U option[]) mapTo = 
        let retArray = resultArray |> Array.choose (Operators.id)
        let retMeta = x.GetMetadataForPartition( meta, meta.Partition, retArray.Length )
        Seq.singleton ( retMeta, CastFunction<'U>.MapTo retArray mapTo )
    member x.CrossJoinChooseInnerMapFunc( innerMeta:BlobMetadata, combinedObj:Object, mapTo:MapToKind) = 
        let parti, obj = combinedObj :?> (int * Object ) 
        let u1Array = 
            match obj with 
            | :? (('U1)[]) as arr ->
                arr
            | :? StreamBase<byte> as ms -> 
                Strm.DeserializeTo<'U1[]>(ms)
            | _ -> 
                let msg = sprintf "CrossJoinFunctionWrapper.CrossJoinMapFunc, the input object is not of type ('U1)[] or MemStream, but type %A with information %A" (obj.GetType()) obj
                Logger.Log( LogLevel.Error, msg )
                failwith msg
        let u0ref = ref Unchecked.defaultof<_>
        if x.DepositBuffer.TryGetValue( parti, u0ref ) then 
            let u0meta, u0Array = !u0ref
            let resultArray = Array.zeroCreate ( u0Array.Length * u1Array.Length ) 
            for u0i = 0 to u0Array.Length - 1 do
                let t0i = u1Array.Length * u0i
                for u1i = 0 to u1Array.Length - 1 do 
                    resultArray.[ t0i + u1i ] <- mapFunc u0Array.[u0i] u1Array.[u1i]    
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Crossjoin computation: %A x %A, yield %d entries" u0meta innerMeta resultArray.Length )           )
            x.ReturnFunc u0meta resultArray mapTo
        else
            let msg = sprintf "Program logic error, there is no blob deposisted for partition %d" parti
            Logger.Log( LogLevel.Error, msg )
            failwith msg

[<AllowNullLiteral; Serializable>]
type internal CrossJoinFunctionWrapper<'U0,'U1,'U>(mapFunc: 'U0->'U1->'U) =
    inherit CrossJoinChooseFunctionWrapper<'U0,'U1,'U>(fun u0 u1 -> (mapFunc u0 u1) |> Some) 

[<AllowNullLiteral; Serializable>]
type internal CrossJoinFoldFunctionWrapper<'U0,'U1,'U,'S>(mapFunc: 'U0->'U1->'U, foldFunc: 'S->'U->'S, initialState:'S) as x =
    inherit DepositFunctionWrapper<'U0, 'S>()
    do
        x.DepositFunc <- x.DepositBlobForFold
        x.MapFunc <- x.CrossJoinFoldInnerMapFunc
        x.ExecuteFunc <- x.ExecuteFoldFunc
    member val StateBuffer = null with get, set
    member val FoldMapTo = MapToKind.OBJECT with get, set
    override x.Reset() = 
        x.StateBuffer <- null
        x.DepositOneReset()
    override x.InitAll() = 
        if x.DepositOneInitAll() then 
            x.StateBuffer <- ConcurrentDictionary<_,_>()
        else
            while Utils.IsNull x.StateBuffer do
                // Spinning to wait for the StateBuffer to be initialized by other threads
                ()
    member internal x.DepositBlobForFold parenti (meta, o:Object ) = 
        let uArray = if Utils.IsNull o then null else ( CastFunction<'U0>.CastTo o )
        x.DepositBuffer.Item( parenti ) <- ( meta, uArray )
        x.StateBuffer.Item( parenti ) <- Array.create uArray.Length initialState
    member x.CrossJoinFoldInnerMapFunc( innerMeta:BlobMetadata, combinedObj:Object, mapTo:MapToKind) = 
        let parti, obj = combinedObj :?> (int * Object ) 
        let u1Array = 
            match obj with 
            | :? (('U1)[]) as arr ->
                arr
            | :? StreamBase<byte> as ms -> 
                Strm.DeserializeTo<'U1[]>(ms)
            | _ -> 
                let msg = sprintf "CrossJoinFunctionWrapper.CrossJoinMapFunc, the input object is not of type ('U1)[] or MemStream, but type %A with information %A" (obj.GetType()) obj
                Logger.Log( LogLevel.Error, msg )
                failwith msg
        let u0ref = ref Unchecked.defaultof<_>
        let s0ref = ref Unchecked.defaultof<_>
        if x.DepositBuffer.TryGetValue( parti, u0ref ) && x.StateBuffer.TryGetValue( parti, s0ref) then 
            let u0meta, u0Array = !u0ref
            let s0Array = !s0ref
            let resultArray = Array.zeroCreate ( u0Array.Length * u1Array.Length ) 
            for u0i = 0 to u0Array.Length - 1 do
                let t0i = u1Array.Length * u0i
                for u1i = 0 to u1Array.Length - 1 do 
                    s0Array.[u0i] <- foldFunc s0Array.[u0i] (mapFunc u0Array.[u0i] u1Array.[u1i])    
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossjoinFold computation: %A x %A" u0meta innerMeta )           )
            x.FoldMapTo <- mapTo
            Seq.empty
        else
            let msg = sprintf "Program logic error, there is no blob deposisted for partition %d" parti
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member x.ExecuteFoldFunc parti = 
        let u0ref = ref Unchecked.defaultof<_>
        let s0ref = ref Unchecked.defaultof<_>
        if x.DepositBuffer.TryGetValue( parti, u0ref ) && x.StateBuffer.TryGetValue( parti, s0ref) then 
            let u0meta, u0Array = !u0ref
            let s0Array = !s0ref
            let s0Len = if Utils.IsNull s0Array then 0 else s0Array.Length
            let retMeta = x.GetMetadataForPartition( u0meta, parti, s0Len )
            Seq.singleton ( retMeta, CastFunction<'S>.MapTo s0Array x.FoldMapTo )
        else
            Seq.empty



        

/// --------------------------------------------------------------------------------------------------------------------------------
/// Wrapper function: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal FunctionWrapperSet<'U, 'U1 > = 
    inherit FunctionWrapper<'U, 'U1> 
    new ( func ) = 
        { inherit FunctionWrapper<'U, 'U1> 
            ( let usefunc( meta, elemArray ) = 
                ( meta, func( elemArray ) )
              usefunc ) }

/// Wrapper function: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'K*'V> -> DSet<'K*'V1>
[<AllowNullLiteral; Serializable>]
type internal FunctionWrapperValue<'K, 'V, 'V1 > = 
    inherit FunctionWrapper<'K*'V, 'K*'V1> 
    new ( func ) = 
        { inherit FunctionWrapper<'K*'V, 'K*'V1> 
            ( let usefunc( meta, elemArray ) = 
                let keyArray = Array.map ( fun (k,v ) -> k ) elemArray
                let valueArray = Array.map ( fun (k,v ) -> v ) elemArray
                let newValueArray = func( valueArray ) 
                let newElemArray = Array.map2( fun k v -> (k, v) ) keyArray newValueArray
                ( meta, newElemArray )
              usefunc  ) }

/// Wrapper function: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
[<AllowNullLiteral; Serializable>]
type internal FunctionWrapperMap<'U > = 
    inherit FunctionWrapper<'U, 'U> 
    new ( func ) = 
        { inherit FunctionWrapper<'U, 'U> 
            ( let usefunc( meta, elemArray ) = 
                ( meta, func( elemArray ) )
              usefunc ) }

/// Wrapper function: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'K*'V> -> DSet<'K*'V1>
[<AllowNullLiteral; Serializable>]
type internal FunctionWrapperMapKeyValue<'K, 'V > = 
    inherit FunctionWrapper<'K*'V, 'K*'V> 
    new ( func ) = 
        { inherit FunctionWrapper<'K*'V, 'K*'V> 
            ( let usefunc( meta, elemArray ) = 
                ( meta, func( elemArray ) )
              usefunc ) }


/// Mapping function wrapper: helps to deserialize and serialize Memstreams
/// Generic, allows mapping from DSet<'U> -> DSet<'U1>
/// This function specifically allows repartition 
/// Note that repartition function does not do row reorg, i.e., SerializationLimit will be ignored and always treated as 0. 
/// The main reason is that repartition will result in deposit of keys to differnt queue, and it will be a mess to reshuffle keys here. 
[<AllowNullLiteral; Serializable>]
type internal FunctionWrapperPartition<'U, 'U1 >(func: (BlobMetadata* ('U)[] ) -> IEnumerable< int * ('U1)[]> ) as x =   
    inherit MetaFunction<'U1>()
    do
        x.MapFunc <- x.WrapperMapFunc
//        x.MapStream <- x.WrapperMapStream    
    // If there is upstream, the decoding function is here. 
    member val private UpstreamCodec = MetaFunction<'U>() with get
    member val private holdElemArray = ConcurrentDictionary<_,_>() with get
    /// Control whether UseFunc is called at final, with null key & value array parameter. 
    member val CallNullAtFinal = false with get, set
    member val private UseFunc = func with get
    /// Wrapper Mapping Function, notice that when KeyArray or ValueArray is null, the UseFunc will not be called, but null will passed down as the end of stream symbols. 
    /// It is the calling function's responsibility to make sure that when x.UseFunc return null, the subsequent class is not called, as that will signal the termination 
    /// of the partition. 
    member private x.WrapperMapFunc( meta, o: Object, mapTo ) = 
        try
            let outputSeq = List<_>()
            if Utils.IsNotNull o then 
                // Decode if necessary 
                let curMeta, elemArray = 
                    match o with 
                    | :? (('U)[]) as arr ->
                        meta, arr
                    | :? StreamBase<byte> as ms -> 
                        x.UpstreamCodec.DecodeFunc( meta, ms )
                    | _ -> 
                        let msg = sprintf "FunctionWrapperPartition.WrapperMapFunc, the input object is not of type ('U)[] or MemStream, but type %A with information %A" (o.GetType()) o
                        Logger.Log( LogLevel.Error, msg )
                        failwith msg
                let returns = x.UseFunc( curMeta, elemArray )
                for oneReturn in returns do 
                    let parti, newElemArray = oneReturn 
                    let bNullReturn = Utils.IsNull newElemArray
                    if not bNullReturn then  
                        // Update NumElems, so there is no need to implement NumElems update in the UseFunction
                        let newNumElems = newElemArray.Length 
                        if newNumElems > 0 then 
                        // No reserialization
                            let newMeta = x.GetMetadataForPartition( meta, parti, newNumElems )
                            outputSeq.Add( newMeta, newElemArray :> Object )
            else
                let returns = if x.CallNullAtFinal then x.UseFunc( meta, null ) else Seq.empty
                for oneReturn in returns do 
                    let parti, newElemArray = oneReturn
                    let bNullReturn = Utils.IsNull newElemArray                   
                    if not bNullReturn then  
                        let newNumElems = newElemArray.Length 
                        if newNumElems>0 then 
                            let newMeta = x.GetMetadataForPartition( meta, parti, newNumElems )
                            outputSeq.Add( newMeta, newElemArray :> Object )
            match mapTo with 
            | MapToKind.OBJECT -> 
                outputSeq |> Seq.cast
            | MapToKind.MEMSTREAM -> 
                outputSeq |> Seq.map ( fun (meta, o) -> let cmeta, cms = x.Encode( meta, o )
                                                        cmeta, cms :> Object )
            | _ -> 
                let msg = sprintf "Error in FunctionWrapper<'U, 'U1 >.WrapperMapFunc, unsupported mapTo %A" mapTo
                Logger.Log( LogLevel.Error, msg )
                failwith msg                                        
        with 
        | e -> 
            let msg = sprintf "Error in FunctionWrapperPartition<'U, 'U1 >.WrapperMapFunc, with exception %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg                                        


[<AllowNullLiteral; Serializable>]
type internal FunctionPartition<'U >( part: Prajna.Core.Partitioner, bPartitionByKey:bool, partFunc: 'U -> int ) =   
    inherit FunctionWrapperPartition<'U, 'U>( FunctionPartition<'U>.WrapperFunc part partFunc )
    do 
        part.bPartitionByKey <- bPartitionByKey
    /// Wrapper Mapping Function for Init, which is a DSet with DSetDependencyType.Source 
    /// The calling signature should be parti, serial, 0, null, null (numElems and o and v are not used). 
    /// The return signature should be (parti, serial, numElems, ko, vo) in which SerializationLimit worth of objects are generated.
    /// The InitFunc will be called for ( parti, serial) to generate the object associated with the serialization
    /// The InitFunc signals the end when it sends parti, serial, numElems, null, null
    static member WrapperFunc (part: Prajna.Core.Partitioner) (partFunc: 'U-> int ) (meta, elemArray) = 
        let partArr = elemArray |> Array.map ( fun elem -> partFunc(elem) % part.NumPartitions )
        let partContent = Array.init part.NumPartitions ( fun i -> List<_>() )
        Array.iter2 ( fun parti elem -> partContent.[parti].Add( elem ) ) partArr elemArray
        let res = partContent |> Seq.mapi ( fun i elem -> i, elem.ToArray() ) |> Seq.filter ( fun (i,elem) -> elem.Length >0 )
        res 

[<AllowNullLiteral; Serializable>]
type internal EnumeratorFromCommonConcurrentQueue<'U> (src: ConcurrentQueue<'U>) = 
    let currentRef = ref Unchecked.defaultof<_>
    interface IEnumerator<'U> with 
        member x.Current = !currentRef
    interface System.Collections.IEnumerator with 
        member x.Current = box (!currentRef) 
        member x.MoveNext() = 
            src.TryDequeue( currentRef )
        member x.Reset() = 
            raise (new System.NotSupportedException("EnumeratorFromCommonConcurrentQueue doesn't support reset") )
    interface System.IDisposable with 
        member x.Dispose() = () 



// Prajna functions
[<AllowNullLiteral; Serializable>]
type internal Function () = 
    let nullFunc( parti, serial, numElems, ms )= 
        ( parti, serial, numElems, ms )
    member val FunctionType = FunctionKind.None with get, set
    member val TransformType = TransformKind.None with get, set
//    member val Assembly : AssemblyEx = null with get, set
    member val FunctionObj : MetaFunction = null with get, set
    member val ParamTypes : Type[] = [| |] with get, set
    member x.Pack( ms: StreamBase<byte> ) = 
        ms.WriteByte( byte x.FunctionType )
        ms.WriteByte( byte x.TransformType )
//        if ( x.FunctionType &&& FunctionKind.HasAssembly )<>FunctionKind.None then 
//            if Utils.IsNotNull x.Assembly then 
//                ms.WriteString( x.Assembly.FullName ) 
//                ms.WriteBytesWLen( x.Assembly.Hash )
//            else
//                let msg = sprintf "Function.Pack, function has HasAssembly flag, but without valid assembly information"
//                Logger.Log(LogLevel.Error, msg)
//                failwith msg
        match x.TransformType with 
        | TransformKind.None ->
            ()
        | _ ->
            ms.Serialize( x.ParamTypes )
            // We wrap around Function object serialization so that this portion can be skipped at deserialization. 
            use msFunc = new MemStream( 1024 )
            msFunc.Serialize( x.FunctionObj )
            ms.WriteMemStream(msFunc) |> ignore
            Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Success in deserialization functions. "))
    static member MakeSeq f = 
        { new IEnumerable<'U> with 
                member x.GetEnumerator() = f()
            interface System.Collections.IEnumerable with 
                member x.GetEnumerator() = (f() :> System.Collections.IEnumerator) }
    static member Unpack( ms:StreamBase<byte>, bUnpackFunc ) = 
        let functionType = enum<FunctionKind>( ms.ReadByte() )
        let transformType = enum<TransformKind>( ms.ReadByte() )
//        if ( x.FunctionType &&& FunctionKind.HasAssembly )<> FunctionKind.None then 
//            let assemblyName = ms.ReadString()
//            let aseemblyVer = ms.ReadInt64()
//            // To fill in code to load assembly 
//            ()
//        else
//            x.Assembly <- null
        let paramTypes = 
            match transformType with 
            | TransformKind.None ->
                [| |]
            | _ ->
                ms.Deserialize() :?> Type[]
        let functionObj = 
            match transformType with 
            | TransformKind.None ->
                null
            | _ ->
                let bytearray = ms.ReadBytesWLen()
                use msFunc = new MemStream( bytearray, 0, bytearray.Length, false, true  )
                if DeploymentSettings.LoadCustomAssebly && bUnpackFunc then 
                    try
                        let o = msFunc.Deserialize() 
                        Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "PrajnaFunctionq.Unpack: deserialized to type %s" (o.GetType().FullName)))
                        o:?> MetaFunction
                    with
                    | e -> 
                        let msg = sprintf "Error in Function.Unpack, fail to deserialize Function (corrupted assembly?) with %A, the stream is %dB" e bytearray.Length
                        Logger.Log( LogLevel.Error, msg )
                        failwith msg
                else
                    // Don't unpack function
                    null
//                let fmt = Formatters.Binary.BinaryFormatter()
//                fmt.Deserialize( ms )
        Function( FunctionType = functionType, 
            TransformType = transformType, 
            ParamTypes = paramTypes, 
            FunctionObj = functionObj )
    static member NullFunctioN() = 
        Function( FunctionType = FunctionKind.None, TransformType = TransformKind.None )
    /// It is a contract that keyArray and valueArray will not be null. 
    static member FilteringKeyFunction<'K, 'V >( filterFunc: 'K -> bool ) = 
        let useFilterFunc( elemArray:('K*'V)[] ) = 
            let keySeq = elemArray |> Seq.map ( fun (k,v) -> k )
            let boolmask = keySeq |> Seq.map( filterFunc ) |> Seq.toArray
            let count = Array.fold( fun sum v -> if v then sum+1 else sum ) 0 boolmask
            if count = elemArray.Length then 
                // Special passthrough case, no new array is created
                ( elemArray )
            elif count = 0 then 
                null 
            else
                let newElemArray = Array.zeroCreate<_> count
                let j = ref 0 
                for i=0 to elemArray.Length - 1 do
                    if boolmask.[i] then 
                        newElemArray.[!j] <- elemArray.[i]
                        j := !j + 1
                ( newElemArray )
        let wrapperFunc = FunctionWrapperMapKeyValue<'K, 'V >( useFilterFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.FilteringKey, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'K>; typeof<bool> |] )
    /// Mapping Values
    static member MappingValueFunction<'K, 'V, 'V1>( mapFunc: 'V -> 'V1 ) = 
        let useMapFunc( valueArray:'V[] ) = 
            let newElemArray = Array.map( mapFunc ) valueArray
            ( newElemArray )
        let wrapperFunc = FunctionWrapperValue<'K, 'V, 'V1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'V>; typeof<'V1> |] )
    /// Mapping Both Key & Values
    static member MappingFunction<'U, 'U1>( mapFunc: 'U -> 'U1 ) = 
        let useMapFunc( elemArray:('U)[] ) = 
            Array.map( mapFunc ) elemArray
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<'U1> |] )
    /// Mapping Both Key & Values
    static member MappingCollectionFunction<'U, 'U1>( mapFunc: 'U[] -> 'U1[] ) = 
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( mapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U[]>; typeof<'U1[]> |] )    
    /// Mapping Both Key & Values, with partition & serial information
    static member MappingIFunction<'U, 'U1>( mapFunc: int -> int64 ->'U -> 'U1 ) = 
        let useMapFunc( meta:BlobMetadata, elemArray: ('U)[] ) = 
            ( meta, Array.mapi( fun i elem -> mapFunc meta.Partition (meta.Serial + int64 i) elem)  elemArray )
        let wrapperFunc = FunctionWrapper<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| |] )
    /// Async mapping function, using Async.Parallel to execute the mapping
    static member AsyncMappingValueFunction<'K, 'V, 'V1>( mapFunc: 'V -> Async<'V1> ) = 
        let useMapFunc( valueArray:'V[] ) = 
            try
                let newValueArray = Async.RunSynchronously ( valueArray |> Array.map( mapFunc ) |> Async.Parallel, timeout=DeploymentSettings.SingleTaskTimeout )
                ( newValueArray )
            with 
            | e ->
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "timeout when executing AsyncMappingValueFunction, results are nullified"))
                ( null )
        let wrapperFunc = FunctionWrapperValue<'K, 'V, 'V1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'V>; typeof<Async<'V1>> |] )
    /// Async mapping function, using Async.Parallel to execute the mapping
    static member AsyncMappingFunction<'U, 'U1>( mapFunc: 'U -> Async<'U1> ) = 
        let useMapFunc( elemArray:('U)[] ) = 
            try 
                Async.RunSynchronously( Array.map( fun elem -> mapFunc(elem)) elemArray |> Async.Parallel, timeout=DeploymentSettings.SingleTaskTimeout )
            with
            | e ->
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "timeout when executing AsyncMappingFunction, results are nullified"))
                ( null )
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<Async<'U1>> |] )
    /// Async mapping function, using Async.Parallel to execute the mapping, with partition & serial information
    static member AsyncMappingIFunction<'U, 'U1>( mapFunc: int -> int64 -> 'U -> Async<'U1> ) = 
        let useMapFunc( meta:BlobMetadata, elemArray:('U)[] ) = 
            let asyncTasks = Array.mapi( fun i elem -> mapFunc meta.Partition (meta.Serial + int64 i) elem) elemArray |> Async.Parallel 
            let newCombinedArray = 
                try
                    Async.RunSynchronously( asyncTasks, timeout=DeploymentSettings.SingleTaskTimeout )
                with
                | e ->
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "timeout when executing AsyncMappingFunction, results are nullified"))
                    ( null )
            ( meta, newCombinedArray )
        let wrapperFunc = FunctionWrapper<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| |] )
    /// Execution of Tasks, handle cases where some task get cancelled during execution
    static member ParallelTaskExecution<'U>( tasks: Task<'U>[] ) = 
        try 
            if tasks.Length > 0 then 
                let taArray = tasks |> Array.map ( fun ta -> ta :> Task )
                let bCompleted = Task.WaitAll( taArray, DeploymentSettings.SingleTaskTimeout ) 
                if bCompleted then 
                    tasks |> Array.map ( fun ta -> ta.Result ) 
                else
                    tasks |> Array.choose ( fun ta -> if ta.IsCompleted then Some ta.Result else None )
            else
                Array.empty
        with 
        | e -> 
            match e with 
            | :? System.AggregateException as exn -> 
                let orgLen = tasks.Length
                let remainingTasks = tasks |> Array.filter ( fun ta -> not ta.IsCanceled && not ta.IsFaulted ) 
                let newLen = remainingTasks.Length
                if newLen < orgLen then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ParallelTaskExecution: %d of %d tasks has been filtered due to cancellation or fault. "                                                                            
                                                                           (orgLen-newLen) orgLen ))
                    Function.ParallelTaskExecution<'U>( remainingTasks )
                else
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "ParallelTaskExecution: encounter AggregateException, but %d of %d tasks has been filtered due to cancellation or fault. "
                                                                           (orgLen-newLen) newLen ))
                    reraise()
            | _ -> 
                reraise()
                
    /// Parallel mapping function for value only, we used task to get a timeout 
    static member ParallelMappingValueFunction<'K, 'V, 'V1>( mapFunc: 'V -> Task<'V1> ) = 
        let useMapFunc( valueArray:'V[] ) = 
            let tasks = valueArray |> Array.map ( mapFunc )
            Function.ParallelTaskExecution<_>( tasks ) 
        let wrapperFunc = FunctionWrapperValue<'K, 'V, 'V1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'V>; typeof<'V1> |] )
    /// Parallel mapping function, Parallel.For is used to execute the mapping in parallel fashion
    static member ParallelMappingFunction<'U, 'U1>( mapFunc: 'U -> Task<'U1> ) = 
        let useMapFunc( elemArray:('U)[] ) = 
            let tasks = elemArray |> Array.map ( mapFunc )
            Function.ParallelTaskExecution<_>( tasks ) 
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<'U1> |] )
    /// Parallel mapping function, Parallel.For is used to execute the mapping in parallel fashion
    static member ParallelMappingIFunction<'U, 'U1>( mapFunc:int -> int64 -> 'U -> Task<'U1> ) = 
        let useMapFunc( meta:BlobMetadata, elemArray:('U)[] ) = 
            let tasks = elemArray |> Array.mapi ( fun i elem -> mapFunc meta.Partition (meta.Serial + int64 i)  elem  )
            meta, Function.ParallelTaskExecution<_>( tasks ) 
        let wrapperFunc = FunctionWrapper<'U, 'U1 >( useMapFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.MappingValue, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| |] )
    /// Filtering of Key & Value, 
    static member FilteringFunction<'U>( filterFunc: 'U -> bool ) = 
        let useFilteringFunc( elemArray:('U)[] ) = 
            let boolmask = Array.map( fun elem -> filterFunc(elem) ) elemArray 
            let count = Array.fold( fun sum v -> if v then sum+1 else sum ) 0 boolmask
            if count = elemArray.Length then 
                // Special passthrough case, no new array is created
                ( elemArray )
            elif count = 0 then 
                null
            else
                let newElemArray = Array.zeroCreate<_> count
                // For Filtering function, value array should not be null, otherwise, FilteringKey should be used. 
                let j = ref 0 
                for i=0 to elemArray.Length - 1 do
                    if boolmask.[i] then 
                        newElemArray.[!j] <- elemArray.[i]
                        j := !j + 1
                ( newElemArray )
        let wrapperFunc = FunctionWrapperMap<'U >( useFilteringFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Filtering, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<bool> |] )
     /// Filtering of Key & Value, 
    static member ChooseFunction<'U, 'U1>( filterFunc ) = 
        let useFilteringFunc( elemArray:('U)[] ) = 
            let mResult = Array.map( fun elem -> filterFunc(elem) ) elemArray
            let count = Array.fold( fun sum v -> match v with
                                                 | Some _ -> sum+1 
                                                 | None -> sum ) 0 mResult
            if count = 0 then 
                null
            else
                let newElemArray = Array.zeroCreate<_> count
                let j = ref 0 
                for i=0 to elemArray.Length - 1 do
                    match mResult.[i] with
                    | Some (newElem) ->
                        newElemArray.[!j] <- newElem
                        j := !j + 1
                    | None ->
                        ()
                ( newElemArray )
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( useFilteringFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<option<'U1>> |] )
    /// Filtering of Key & Value, 
    static member CollectFunction<'U, 'U1>( collectFunc:'U->seq<'U1> ) = 
        let useCollectFunc( elemArray:('U)[] ) = 
            let newElemArray = Array.collect( fun elem -> collectFunc(elem) |> Seq.toArray ) elemArray
            if newElemArray.Length > 0 then 
                newElemArray
            else
                null
        let wrapperFunc = FunctionWrapperSet<'U, 'U1 >( useCollectFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'U>; typeof<seq<'U1>> |] )
    /// Group by value, within partition only 
    static member GroupByValueFunction<'K, 'V when 'K : equality >( ) = 
        let useGroupByValueFunc( elemArray:('K*'V)[] ) = 
            let newElemArray = elemArray |> Seq.groupBy( fun (k,v) -> k ) 
                                     |> Seq.map ( fun (k, vseq ) -> ( k, System.Collections.Generic.List<_>( vseq |> Seq.map( fun (k,v) -> v ) ) ) )
                                     |> Seq.toArray
            ( newElemArray )
        let wrapperFunc = FunctionWrapperSet<'K*'V, 'K*System.Collections.Generic.List<'V> >( useGroupByValueFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'K*'V>; typeof<'K*System.Collections.Generic.List<'V>> |] )
    /// Group by value, within partition only 
    static member GroupByValueCollectionFunction<'K, 'V when 'K : equality >( ) = 
        let useGroupByValueFunc( elemArray:('K*System.Collections.Generic.List<'V>)[] ) = 
            let v1 = elemArray |> Seq.groupBy( fun (k,v) -> k ) 
            let v2 = v1 |> Seq.map ( fun (k, vseqseq ) -> let lst = System.Collections.Generic.List<_>()
                                                          for item in vseqseq do 
                                                              let _, vseq = item
                                                              lst.AddRange( vseq ) 
                                                          ( k, lst ) )
            
                        |> Seq.toArray
            ( v2 )
        let wrapperFunc = FunctionWrapperSet<'K*System.Collections.Generic.List<'V>, 'K*System.Collections.Generic.List<'V> >( useGroupByValueFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<unit>; typeof<unit> |] )    
    static member Identity<'U>( ) = 
        let identityFunc( elemArray ) =  
            ( elemArray )
        let wrapperFunc = FunctionWrapperMap<'U>( identityFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [|  |] )
    static member Init<'U>( initFunc, partitionSizeFunc ) =
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = InitFunctionWrapper<'U >( initFunc, partitionSizeFunc ), 
            ParamTypes = [|  |] )
    static member Source<'U>( sourceSeqFunc ) =
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = SourceFunctionWrapper<'U >( sourceSeqFunc ), 
            ParamTypes = [|  |] )
    static member SourceN<'U>( num, sourceNSeqFunc ) =
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = SourceNFunctionWrapper<'U >( num, sourceNSeqFunc ), 
            ParamTypes = [|  |] )
    static member SourceI<'U>( numParitions, sourceISeqFunc ) =
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = SourceIFunctionWrapper<'U >( numParitions, sourceISeqFunc ), 
            ParamTypes = [|  |] )
    /// Regroup is used to redefine 'K[] and 'V[] with a new serialization limit. 
    static member Regroup<'K, 'V >( filterFunc: 'K -> bool ) = 
        let useFilterFunc( elemArray:('K*'V)[] ) = 
            let keyArray = Array.map ( fun (k,v) -> k ) elemArray
            let boolmask = Array.map( filterFunc ) keyArray
            let count = Array.fold( fun sum v -> if v then sum+1 else sum ) 0 boolmask
            if count = elemArray.Length then 
                // Special passthrough case, no new array is created
                ( elemArray )
            elif count = 0 then 
                null 
            else
                let newElemArray = Array.zeroCreate<_> count
                let j = ref 0 
                for i=0 to elemArray.Length - 1 do
                    if boolmask.[i] then 
                        newElemArray.[!j] <- elemArray.[i]
                        j := !j + 1
                ( newElemArray )
        let wrapperFunc = FunctionWrapperMapKeyValue<'K, 'V >( useFilterFunc )
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.Mapping, 
            FunctionObj = wrapperFunc, 
            ParamTypes = [| typeof<'K>; typeof<bool> |] )        
    /// Regroup is used to redefine 'K[] and 'V[] with a new serialization limit. 
    static member internal RepartitionWithinNode<'U >( x: Prajna.Core.Partitioner, bPartitionByKey, partFunc: 'U -> int ) = 
        Function( FunctionType = FunctionKind.DotNet, 
                        TransformType = TransformKind.Repartition, 
                        FunctionObj = FunctionPartition<'U >( x, bPartitionByKey, partFunc ), 
                        ParamTypes = [| typeof<'U>; typeof<int> |] )   
    static member CorrelatedMix2<'U0,'U1,'V>( func: 'U0 -> 'U1 -> 'V ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = CorrelatedMix2FunctionWrapper<'U0,'U1,'V >( false, func ), 
            ParamTypes = [|  |] )
    static member CorrelatedMix3<'U0,'U1,'U2,'V>( func: 'U0 -> 'U1 -> 'U2 -> 'V ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = CorrelatedMix3FunctionWrapper<'U0,'U1,'U2,'V >( false, func ), 
            ParamTypes = [|  |] )
    static member CorrelatedMix4<'U0,'U1,'U2,'U3,'V>( func: 'U0 -> 'U1 -> 'U2 -> 'U3 -> 'V ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = CorrelatedMix4FunctionWrapper<'U0,'U1,'U2,'U3,'V >( false, func ), 
            ParamTypes = [|  |] )
    static member InnerSortedJoinSet<'U0,'U1,'V,'K>( k0Func, k1Func, comp, func ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = InnerJoinByMergeFunctionWrapper<'U0,'U1,'V,'K >( false, k0Func, k1Func, comp, func ), 
            ParamTypes = [|  |] )
    static member LeftOuterSortedJoinSet<'U0,'U1,'V,'K>( k0Func, k1Func, comp, func ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = LeftOuterJoinByMergeFunctionWrapper<'U0,'U1,'V,'K >( false, k0Func, k1Func, comp, func ), 
            ParamTypes = [|  |] )
    static member RightOuterSortedJoinSet<'U0,'U1,'V,'K>( k0Func, k1Func, comp, func ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = RightOuterJoinByMergeFunctionWrapper<'U0,'U1,'V,'K >( false, k0Func, k1Func, comp, func ), 
            ParamTypes = [|  |] )
    static member KeyFunc elem = 
        let key, _ = elem
        key
    static member InnerSortedJoinKV<'V0,'V1,'V,'K>( comp, func: 'V0 -> 'V1 -> 'V ) = 
        let wrapFunc elem0 elem1 = 
            let k0, v0 = elem0
            let k1, v1 = elem1
            k0, func v0 v1
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = InnerJoinByMergeFunctionWrapper<'K*'V0,'K*'V1,'K*'V,'K >( false, Function.KeyFunc, Function.KeyFunc, comp, wrapFunc ), 
            ParamTypes = [|  |] )
    static member LeftOuterSortedJoinKV<'V0,'V1,'V,'K>( comp, func: 'V0 -> 'V1 option -> 'V ) = 
        let wrapFunc elem0 elem1 = 
            let k0, v0 = elem0
            match elem1 with 
            | Some (k1, v1 ) -> 
                k0, func v0 ( Some v1)
            | None -> 
                k0, func v0 None
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = LeftOuterJoinByMergeFunctionWrapper<'K*'V0,'K*'V1,'K*'V,'K >( false, Function.KeyFunc, Function.KeyFunc, comp, wrapFunc ), 
            ParamTypes = [|  |] )
    static member RightOuterSortedJoinKV<'V0,'V1,'V,'K>( comp, func: 'V0 option -> 'V1 -> 'V ) = 
        let wrapFunc elem0 elem1 = 
            let k1, v1 = elem1
            match elem0 with 
            | Some (k0, v0 ) -> 
                k1, func (Some v0) v1
            | None -> 
                k1, func None v1
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = RightOuterJoinByMergeFunctionWrapper<'K*'V0,'K*'V1,'K*'V,'K >( false, Function.KeyFunc, Function.KeyFunc, comp, wrapFunc ), 
            ParamTypes = [|  |] )
    static member InnerHashedJoinSet<'U0,'U1,'V,'K>( k0Func, k1Func, comp, func ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CorrelatedMix, 
            FunctionObj = InnerJoinByMergeFunctionWrapper<'U0,'U1,'V,'K >( false, k0Func, k1Func, comp, func ), 
            ParamTypes = [|  |] )
    static member CrossJoin<'U0,'U1,'U>( mapFunc ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CrossJoin, 
            FunctionObj = CrossJoinFunctionWrapper<'U0,'U1,'U>( mapFunc ),
            ParamTypes = [|  |] )
    static member CrossJoinChoose<'U0,'U1,'U>( mapFunc ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CrossJoin, 
            FunctionObj = CrossJoinChooseFunctionWrapper<'U0,'U1,'U>( mapFunc ),
            ParamTypes = [|  |] )
    static member CrossJoinFold<'U0,'U1,'U,'S>( mapFunc, foldFunc, initialState ) = 
        Function( FunctionType = FunctionKind.DotNet, 
            TransformType = TransformKind.CrossJoin, 
            FunctionObj = CrossJoinFoldFunctionWrapper<'U0,'U1,'U,'S>( mapFunc, foldFunc, initialState ),
            ParamTypes = [|  |] )
    static member Distribute<'U>( lst : seq<'U> ) = 
        fun (cl:Cluster) peeri -> 
            let lstToPeeri = cl.SplitByPeers<'U> peeri lst
            Function.Source<'U>( fun _ -> lstToPeeri :> seq<_> )
    static member DistributeN<'U>( num, lst : seq<'U> ) = 
        fun (cl:Cluster) peeri -> 
            let lstToPeeri = cl.SplitByPeers<'U> peeri lst
            let commonQueue = ConcurrentQueue<'U>( lstToPeeri )
            let wrappedSourceFunc parti = 
                Function.MakeSeq ( fun _ -> new EnumeratorFromCommonConcurrentQueue<_>( commonQueue ) :> IEnumerator<_> )
            Function.SourceN<'U>( num, wrappedSourceFunc )

            


