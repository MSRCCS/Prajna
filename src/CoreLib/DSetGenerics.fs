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
        DSetGeneric.fs
  
    Description: 
        FSharp APIs for DSet<'U>

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Api.FSharp

open System
open System.IO
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Linq
open Microsoft.FSharp.Collections
open Prajna.Core
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Service



/// Generic distributed dataset. If storing the content of DSet to a persisted store, it is highly advisable that 'U used is either of .Net system type, 
/// or using a customized serializer/deserializer to serialize ('U)[]. Otherwise, the stored data may not be retrievable as the BinaryFormatter may fail 
/// to deserialize a class if the associated code has changed.
[<Serializable>]
type DSet<'U> () = 

    /// Public APIs
    /// * member functions use tuple form
    /// * static member functions use currying form

    inherit DSet()

    let mkSeq f = 
        { new IEnumerable<'U> with 
                member x.GetEnumerator() = f()
            interface IEnumerable with 
                member x.GetEnumerator() = (f() :> IEnumerator) }
    let codec = MetaFunction<'U>()
    let mutable partitionFuncInternal = 
        ( fun ( elem, numPartitions ) ->
            MetaFunction<'U>.HashPartitionerKey( elem, numPartitions ) )
    
    member val private bPartitionFuncSet = false with get, set
    /// Get and set the partition function, the function takes a Key and an int32 which is the number of partitions in DSet, 
    /// and return the partition that the key is mapped to. 
    /// Note: it's only used in "store" so far, make it temporary for now. Will check whether it's worth exposing it to allow
    ///       customized partition function here.
    member private x.PartitionFunc with get() = partitionFuncInternal
                                   and set( newPartitionFunc ) = 
                                        x.bPartitionFuncSet <- true
                                        partitionFuncInternal <- newPartitionFunc

    /// Get number of partitions. If partition has not been set, setup the partition
    member x.GetNumberOfPartitions() = 
        if x.NumPartitions <= 0 then 
            x.SetupPartitionMapping()
        x.NumPartitions

    /// Action associated with the current DSet
    member val private Action : DSetAction = null with get, set
    static member internal DefaultParam() = 
        DParam( )

    /// Helper function, derive a DSet from the current DSet (used for .Filter, .Map, etc..)
    member internal x.Derived<'U1>( flag, appendix ) = 
        let newDSet = DSet<'U1>()
        newDSet.CopyMetaData( x, flag )
        newDSet.Name <- x.Name + appendix
        newDSet

    /// Establish a bypass dependency
    member internal x.EstablishBypass( childDSets:seq<DSet> ) = 
        let depParent = DependentDSet(x)
        let depChilds = childDSets |> Seq.map ( fun d -> DependentDSet( d ) ) |> Seq.toArray
        for childDSet in childDSets do
            let sidewayDeps = List<_>( depChilds |> Seq.filter ( fun dep -> not (Object.ReferenceEquals( dep.Target, childDSet )) ) )
            childDSet.Dependency <- Bypass ( depParent, sidewayDeps )
        x.DependencyDownstream <- DistributeForward ( List<_>( depChilds ) )

    /// Establish a bypass dependency
    member internal x.EstablishCorrelatedMix ( parentDSets:seq<DSet> ) = 
        let depChild = DependentDSet(x)
        let depParents = parentDSets |> Seq.map ( fun d -> DependentDSet( d ) ) |> Seq.toArray
        let parent0 = parentDSets.First()
        let numPartitions = parent0.NumPartitions
        let mapping = parent0.GetMapping()
        let mutable first = true
        for parentDSet in parentDSets do
            if first then 
                first <- false
            else
                if parentDSet.NumPartitions <> numPartitions then
                    raise (System.ArgumentException("CorrelatedMix: all parent DSets must have the same number of partitions", "parentDSets"))
                if Array.exists2 (fun a1 a2 -> Array.exists2 (fun v1 v2 -> v1 <> v2) a1 a2) mapping (parentDSet.GetMapping()) then
                    raise (System.ArgumentException("CorrelatedMix: all parent DSets must have the same mapping", "parentDSets"))
            parentDSet.DependencyDownstream <- CorrelatedMixTo depChild
        x.Dependency <- CorrelatedMixFrom ( List<_>( depParents ) )

    /// Establish a bypass dependency
    member internal x.EstablishUnion ( parentDSets:seq<DSet> ) = 
        let depChild = DependentDSet(x)
        let depParents = parentDSets |> Seq.map ( fun d -> DependentDSet( d ) ) |> Seq.toArray
        for parentDSet in parentDSets do
            parentDSet.DependencyDownstream <- UnionTo depChild
        x.Dependency <- UnionFrom ( List<_>( depParents ) )

    /// Establish a cross join dependency
    member internal x.EstablishCrossJoin (joinDSet0:DSet) (joinDSet1:DSet)= 
        let depChild = DependentDSet(x)
        joinDSet0.DependencyDownstream <- CrossJoinTo depChild
        joinDSet1.DependencyDownstream <- CrossJoinTo depChild
        x.Dependency <- CrossJoinFrom ( DependentDSet(joinDSet0), DependentDSet(joinDSet1) )

    /// Establish a hash join dependency
    member internal x.EstablishHashJoin (joinDSet0:DSet) (joinDSet1:DSet)= 
        let depChild = DependentDSet(x)
        joinDSet0.DependencyDownstream <- HashJoinTo depChild
        joinDSet1.DependencyDownstream <- HashJoinTo depChild
        x.Dependency <- HashJoinFrom ( DependentDSet(joinDSet0), DependentDSet(joinDSet1) )

    /// to base class
    static member internal toBase (x:DSet<'U>) = 
        x :> DSet

    /// <summary> 
    /// Save DSet to a persisted store with a certain name
    /// </summary>
    /// <param name="storageKind"> Type of storage that the DSet is stored </param>
    /// <param name="name"> Name of the new DSet that is persisted </param>
    member private x.SaveToDStream storageKind name =
        x.Name <- name
        x.Version <- (PerfADateTime.UtcNow())
        let encStream = DStream( x )
        x.DependencyDownstream <- SaveTo (DependentDStream(encStream) )
        encStream.Dependency <- SaveFrom (DependentDObject(x) )
        encStream.StorageType <- storageKind
        encStream

    /// Encode to a DStream
    member private x.EncodeTo() = 
        let encStream = DStream( x )
        x.DependencyDownstream <- EncodeTo (DependentDStream(encStream) )
        encStream.Dependency <- EncodeFrom (DependentDObject(x) )
        encStream.StorageType <- StorageKind.Passthrough
        encStream

    /// Install an identity mapping if the current DSet is the source. This ensures that an encode function is installed on DSet. 
    member private x.IfSourceIdentity() = 
        if x.Dependency = DSetDependencyType.StandAlone then 
            x.Identity()
        else
            x

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function will be applied per collection as the dataset is being distributedly iterated. 
    /// The entire dataset never materialize entirely. 
    member private x.MapImpl ( func:'U -> 'U1 ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_Map" )
        newDSet.Function <- Function.MappingFunction<'U, 'U1>( func )
        newDSet


    /// Creates a new dataset containing only the elements of the dataset for which the given predicate returns true.
    member private x.FilterImpl func = 
        let newDSet = x.Derived<'U>( DSetMetadataCopyFlag.Passthrough, "_Filter" )
        // Deterministic name, 
        newDSet.Function <- Function.FilteringFunction<'U>( func )
        newDSet

    // ============================================================================================================
    // Store a sequence to a persisted DSet
    // ============================================================================================================

    /// Store a sequence to a persisted DSet
    member x.StoreInternal (o:seq<('U)> ) = 
        try 
            x.Initialize()
            x.BeginWriteToNetwork()
            let written = Array.zeroCreate<int> x.NumPartitions
            let endCalled = Array.create x.NumPartitions false
            let arr = Array.init x.NumPartitions ( fun i -> List<'U>() )
            let rec writeout( parti, peers:int[], bFlush ) = 
                x.ClearTimeout( parti ) 
                let wLen = Math.Min( arr.[parti].Count, x.SerializationLimit )
                if wLen > 0 then 
                    use ms = new MemStream()
                    // Write DSet begins with Name + Version
                    // Write DSet format
                    // Name, Version, partition, serial, number of KVs
                    ms.WriteGuid( x.WriteIDForCleanUp )
                    ms.WriteString( x.Name )
                    ms.WriteInt64( x.Version.Ticks )
                    ms.WriteVInt32(parti)
                    let serial = x.GetSerialForWrite( parti, wLen )
                    ms.WriteInt64( serial )
                    ms.WriteVInt32( wLen )
                    let meta = BlobMetadata( parti, serial, wLen )
                    let kvList = arr.[parti].GetRange(0, wLen)
                    let _, msOutput = codec.EncodeFunc( meta, kvList.ToArray() ) 
                    ms.Append(msOutput)
                    (msOutput :> IDisposable).Dispose()
                    written.[parti] <- written.[parti] + wLen
                    if x.NumReplications<=1 || peers.Length > 1 then 
                        peers |> Array.iter( fun peeri -> 
                            x.Write( parti, peeri, ms ) )
                    else
                        let peeri = peers.[0]
                        x.WriteAndReplicate( parti, peeri, ms )
                            
                    if wLen >= arr.[parti].Count then 
                        arr.[parti].Clear()
                    else
                        arr.[parti].RemoveRange( 0, wLen )
                    if bFlush && arr.[parti].Count>0 then 
                        let newpeers = x.CanWrite( parti )
                        if newpeers.[0] >=0 then 
                            writeout( parti, newpeers, bFlush )

            o |> Seq.iteri ( fun i elem -> 
                    let parti = x.PartitionFunc( elem, x.NumPartitions )
                    arr.[parti].Add( elem )
                    if arr.[parti].Count >= x.SerializationLimit then 
                        let peers = x.CanWrite( parti )
                        let peeri = peers.[0]
                        if peeri >= 0 then 
                            writeout( parti, peers, false )
                        else 
                            if peeri<>Int32.MinValue then 
                                if arr.[parti].Count >= x.SerializationLimit * x.TimeoutMultiple then 
                                    if not (x.IsTimeout(parti)) then 
                                        Threading.Thread.Sleep( x.TimeoutSleep )
                                        Logger.LogF( x.WriteIDForCleanUp, LogLevel.WildVerbose, ( fun _ -> sprintf "Partition %d timeout triggered. No peer can be found to write out content, write operation back up to %d records." parti arr.[parti].Count ))
                                    else
                                        let msg = ( sprintf "Partition %d timeout. No peer can be found to write out content, write operation back up to %d records." parti arr.[parti].Count )
                                        Logger.LogF( x.WriteIDForCleanUp, LogLevel.Error, ( fun _ -> msg ))
                                        failwith msg
                            else
                                let msg = ( sprintf "No valid peer to write out partition %d. There are %d records in partition. " parti arr.[parti].Count )
                                Logger.Log( LogLevel.Error, msg )
                                failwith msg
                    ()
                )
            // Flush all key values 
            let mutable bAllFlushed = false
            while not bAllFlushed do
                bAllFlushed <- true
                for parti = 0 to arr.Length-1 do 
                    if arr.[parti].Count>0 then
                        let peers = x.CanWrite( parti )
                        let peeri = peers.[0]
                        if peeri >= 0 then 
                            writeout( parti, peers, true )
                        else 
                            if peeri<>Int32.MinValue then 
                                if arr.[parti].Count >= x.SerializationLimit * x.TimeoutMultiple then 
                                    if not (x.IsTimeout(parti)) then 
                                        Threading.Thread.Sleep( x.TimeoutSleep )
                                        Logger.LogF( x.WriteIDForCleanUp, LogLevel.WildVerbose, ( fun _ -> sprintf "During flush, partition %d timeout triggered. No peer can be found to write out content, write operation back up to %d records." parti arr.[parti].Count ))
                                    else
                                        let msg = ( sprintf "Partition %d timeout. No peer can be found to write out content, write operation back up to %d records." parti arr.[parti].Count )
                                        Logger.Log( LogLevel.Error, msg )
                                        failwith msg
                            else
                                let msg = ( sprintf "No enough valid peer to write out partition %d. There are %d records in partition. " parti arr.[parti].Count )
                                Logger.Log( LogLevel.Error, msg )
                                failwith msg
                    if arr.[parti].Count > 0 then 
                        bAllFlushed <- false
                    else
                        if not endCalled.[parti] then 
                            // Flush operation 
                            let peers = x.CanWrite( parti )
                            if x.NumReplications<=1 || peers.Length > 1 then 
                                peers |> Array.iter( fun peeri -> 
                                    if peeri>=0 && written.[parti]>0 then 
                                        x.EndParition parti peeri 
                                    else
                                        // Non has been written to peeri, as we reach the end too quick
                                        if written.[parti]=0 then 
                                            let partimapping = x.PartiMapping( parti )
                                            if x.bEncodeMapping then 
                                            // meaningful mapping    
                                                for peeri in partimapping do
                                                    x.ReportDSet( peeri, Array.create 1 (parti, 0, 0L), false )
                                            else
                                                x.ReportDSet( 0, Array.create 1 (parti, 0, 0L), false )
                                        )
                            else
                                let peeri = peers.[0]
                                if peeri >= 0 && written.[parti]>0 then 
                                    x.EndParition parti peeri
                                else
                                    if written.[parti]=0 then 
                                        let partimapping = x.PartiMapping( parti )
                                        if x.bEncodeMapping then 
                                        // meaningful mapping    
                                            for peeri in partimapping do
                                                x.ReportDSet( peeri, Array.create 1 (parti, 0, 0L), false )
                                        else
                                            x.ReportDSet( 0, Array.create 1 (parti, 0, 0L), false )
                            endCalled.[parti] <- true    
            let retVal = x.EndWriteToNetwork( x.MaxWait )
            if not retVal then 
                Logger.LogF( x.WriteIDForCleanUp, LogLevel.Info, ( fun _ -> sprintf "Not all Key-Values have been successfully written to DSet.Store..." ))
            x.Cluster.FlushCommunication()
        with 
        | ex -> 
            if Utils.IsNotNull ex then 
                Logger.LogF( x.WriteIDForCleanUp, LogLevel.Error, ( fun _ -> sprintf "DSet.Store is interrupted by exception %A" ex ))
            else
                Logger.LogF( x.WriteIDForCleanUp, LogLevel.Error, ( fun _ -> sprintf "DSet.Store is interrupted by exception with null e" ))
            x.CancelWriteByException( ex )
            reraise()
        ()
    /// Store a sequence to a persisted DSet
    member x.Store (o:seq<('U)> ) = 
        x.BeginWriteJob( )
        x.StoreInternal( o )
    /// Store a sequence to a persisted DSet
    member x.Store (o:seq<('U)>, cts:CancellationToken ) = 
        x.BeginWriteJob( cts) 
        x.StoreInternal( o )
    /// Store a sequence to a persisted DSet
    static member store (x:DSet<'U>) o = 
        x.Store( o ) 


    // ============================================================================================================
    // Load persisted DSet
    // ============================================================================================================

    /// <summary>
    /// Trigger to load metadata. 
    /// </summary>
    /// <returns> Dataset with loaded metadata. </returns>
    member x.LoadSource() = 
        let bSuccess = x.TryLoadDSetMetadata( false ) 
        if not bSuccess then 
            failwith (sprintf "Fail to find metadata for %s" x.Name)
        x

    /// <summary>
    /// Trigger to load metadata. 
    /// </summary>
    /// <returns> Dataset with loaded metadata. </returns>
    static member loadSource (x:DSet<'U>) = 
        x.LoadSource()

    // ============================================================================================================
    // Actions for DSet
    // ============================================================================================================

    /// Convert DSet to a sequence seq<'U>
    member x.ToSeq() = 
        mkSeq ( fun _ -> new DSetEnumerator<_>(x.IfSourceIdentity() ) :> IEnumerator<_> )    
  
    /// Convert DSet to a sequence seq<'U>
    static member toSeq (x:DSet<'U>) = 
        x.ToSeq()

    member private y.FoldAction (folder, aggrFunc, state:'GV, commonStatePerNode : bool) = 
        let x = y.IfSourceIdentity()
        use action = new DSetFoldAction<'U, 'GV >( CommonStatePerNode = commonStatePerNode, 
                                                   Param=x, 
                                                   FoldFunc=FoldFunction<'U, 'GV >(  folder ), 
                                                   AggreFunc=AggregateFunction<'GV>( aggrFunc ) )
        x.Action <- action :> DSetAction
        action.DoFold (state)


    /// <summary>
    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is broadcasted to each node, and is shared
    /// across partitions, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    /// </summary>
    /// <param name="folder"> 'State -> 'U -> 'State, update the state given the input elements </param>
    /// <param name="aggrFunc"> 'State -> 'State -> 'State, which aggregate the state from different partitions to a single state variable.</param>
    /// <param name="state"> initial state for each partition </param>
    member internal x.FoldWithCommonStatePerNode (folder, aggrFunc, state) = 
        x.FoldAction (folder, aggrFunc, state, commonStatePerNode = true)

    /// <summary>
    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is deserialized (separately) for each partition. 
    /// Within each partition, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    /// </summary>
    /// <param name="folder"> 'State -> 'U -> 'State, update the state given the input elements </param>
    /// <param name="aggrFunc"> 'State -> 'State -> 'State, which aggregate the state from different partitions to a single state variable.</param>
    /// <param name="state"> initial state for each partition </param>
    member public x.Fold (foldFunc: 'GV -> 'U -> 'GV, aggrFunc: 'GV -> 'GV -> 'GV, state:'GV) = 
        x.FoldAction (foldFunc, aggrFunc, state, commonStatePerNode = false)

    /// <summary>
    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is broadcasted to each partition. 
    /// Within each partition, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    /// </summary>
    /// <param name="folder"> 'State -> 'U -> 'State, update the state given the input elements </param>
    /// <param name="aggrFunc"> 'State -> 'State -> 'State, which aggregate the state from different partitions to a single state variable.</param>
    /// <param name="state"> initial state for each partition </param>
    static member fold folder aggrFunc state (x:DSet<'U>) = 
        x.Fold(folder, aggrFunc, state)

    /// <summary>
    /// Reduces the elements using the specified 'reducer' function
    /// </summary>
    member x.Reduce reducer = 
        let folder (r1: 'U option) (r2: 'U option) =
            match r1 with
            | None -> r2
            | Some v -> reducer v (r2.Value) |> Some
        let aggrWrapper (s1 : 'U option) (s2 : 'U option) = 
            reducer s1.Value s2.Value |> Some
        let y = x.MapImpl (fun v -> v |> Some)
        let resultOption = y.FoldWithCommonStatePerNode(folder, aggrWrapper, None)
        match resultOption with
        | Some value -> value
        | None -> failwith ("No result")

    /// <summary>
    /// Reduces the elements using the specified 'reducer' function
    /// </summary>
    static member reduce reducer (x:DSet<'U>) = 
        x.Reduce reducer

    /// Applies a function 'iterFunc' to each element
    member x.Iter (iterFunc: 'U -> unit ) = 
        let folder s elem =
            iterFunc elem
            s
        let aggr s1 s2 = 
            s1
        x.FoldWithCommonStatePerNode( folder, aggr, new Object() ) |> ignore

    /// Iterate the given function to each element. This is an action.
    static member iter iterFunc (x:DSet<'U>) = 
        x.Iter iterFunc

    /// <summary> 
    /// Count the number of elements(rows) in the DSet </summary>
    /// <return> number of elments(rows) </return>
    member x.Count() = 
        x.FoldWithCommonStatePerNode((fun count _ -> count + 1L), (fun c1 c2 -> c1 + c2), 0L)

    /// <summary> 
    /// Count the number of elements(rows) in the DSet </summary>
    /// <return> number of elments(rows) </return>
    static member count ( x:DSet<'U>) =
        x.Count()

    /// <summary>
    /// Read DSet back to local machine and apply a function on each value. Caution: the iter function will be executed locally as the entire DSet is read back. 
    /// </summary> 
    member x.LocalIter iterFunc = 
        x.ToSeq() |> Seq.iter iterFunc


    /// <summary>
    /// Read DSet back to local machine and apply a function on each value. Caution: the iter function will be executed locally as the entire DSet is read back. 
    /// </summary> 
    static member localIter iterFunc ( x:DSet<'U>) = 
        x.LocalIter iterFunc

    /// <summary>
    /// Read DSet back to local machine and print each value. Caution: the iter function will be executed locally as the entire DSet is read back. 
    /// </summary> 
    member x.Printfn fmt = 
        let iterFunc elem = 
            printfn fmt elem |> ignore
        x.LocalIter iterFunc

    /// <summary>
    /// Read DSet back to local machine and apply a function on each value. Caution: the iter function will be executed locally as the entire DSet is read back. 
    /// </summary> 
    static member printfn fmt ( x:DSet<'U>) = 
        x.Printfn fmt

   /// Save the DSet to the storage specifed by storageKind with name "name". This is an action.
    /// Note: make it public when we support more than one storage kind
    member private x.Save(storageKind, name) =
        let (pullDSet : DSet<'U>, pushDSet : DSet<'U>) = x.Bypass2()
        pushDSet.SaveToDStream storageKind name |> ignore
        // take an action on pullDSet to trigger the save on pushDSet branch
        pullDSet.Count() |> ignore

    /// Save the DSet to the storage specifed by storageKind with name "name". This is an action.
    /// Note: make it public when we support mroe than one storage kind
    static member private save storageKind name (x:DSet<'U>) =
        x.Save(storageKind, name)

    /// Save the DSet to HDD using "name". This is an action.
    member x.SaveToHDDByName name =
        x.Save(StorageKind.HDD, name)

    /// Save the DSet to HDD using "name". This is an action.
    static member saveToHDDByName name (x:DSet<'U>) =
        x.SaveToHDDByName name

    /// Save the DSet to HDD. This is an action.
    member x.SaveToHDD () =
        x.SaveToHDDByName x.Name

    /// Save the DSet to HDD. This is an action.
    static member saveToHDD (x:DSet<'U>) =
        x.SaveToHDD()

    /// Lazily save the DSet to the storage specifed by storageKind with name "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    /// Note: make it public when we support more than one storage kind
    member private x.LazySave(storageKind, name) =        
        x.SaveToDStream storageKind name |> ignore

    /// Lazily save the DSet to the storage specifed by storageKind with name "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    /// Note: make it public when we support mroe than one storage kind
    static member private lazySave storageKind name (x:DSet<'U>) =
        x.LazySave(storageKind, name)
                
    /// Lazily save the DSet to HDD using "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    member x.LazySaveToHDDByName name =
        x.LazySave (StorageKind.HDD, name)

    /// Lazily save the DSet to HDD using "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    static member lazySaveToHDDByName name (x:DSet<'U>) =
        x.LazySaveToHDDByName name

    /// Lazily save the DSet to HDD.
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    member x.LazySaveToHDD () =
        x.LazySaveToHDDByName x.Name

    /// Lazily save the DSet to HDD.
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    static member lazySaveToHDD (x:DSet<'U>) =
        x.LazySaveToHDD()

    /// Save the DSet to the storage specifed by storageKind with name "name". 
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    /// Note: make it public when we support more than one storage kind
    member private x.SaveWithMonitor(monitorFunc, localIterFunc, storageKind, name) =
        let (pullDSet : DSet<'U>, pushDSet : DSet<'U>) = x.Bypass2()
        pushDSet.SaveToDStream storageKind name |> ignore
        // take an action on pullDSet to trigger the save on pushDSet branch
        pullDSet.FilterImpl(monitorFunc).LocalIter(localIterFunc)

    /// Save the DSet to the storage specifed by storageKind with name "name". This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    /// Note: make it public when we support mroe than one storage kind
    static member private saveWithMonitor monitorFunc localIterFunc storageKind name (x:DSet<'U>) =
        x.SaveWithMonitor(monitorFunc, localIterFunc, storageKind, name)

    /// Save the DSet to HDD using "name". This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    member x.SaveToHDDByNameWithMonitor(monitorFunc, localIterFunc, name) =
        x.SaveWithMonitor(monitorFunc, localIterFunc, (StorageKind.HDD), name)

    /// Save the DSet to HDD using "name". This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    static member saveToHDDByNameWithMonitor monitorFunc localIterFunc name (x:DSet<'U>) =
        x.SaveToHDDByNameWithMonitor(monitorFunc, localIterFunc, name)

    /// Save the DSet to HDD. This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    member x.SaveToHDDWithMonitor(monitorFunc, localIterFunc) =
        x.SaveToHDDByNameWithMonitor(monitorFunc, localIterFunc, x.Name)

    /// Save the DSet to HDD. This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    static member saveToHDDWithMonitor monitorFunc localIterFunc (x:DSet<'U>) =
        x.SaveToHDDWithMonitor(monitorFunc, localIterFunc)

    // ============================================================================================================
    // APIs for populating the DSet
    // ============================================================================================================
    
    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate.
    /// </summary> 
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSizeFunc"> The functional delegate that returns the size of the partition,  the integer index passed to the function 
    /// indicates the partition </param> 
    member x.Init(initFunc, partitionSizeFunc) =
        x.NumPartitions <- x.Cluster.NumNodes
        x.NumReplications <- 1
        x.Dependency <- Source
        x.Function <- Function.Init<'U>( initFunc, partitionSizeFunc ) 
        // Trigger of setting the serialization limit parameter within functions. 
        // Automatic set serialization limit
        // x.SerializationLimit <- x.SerializationLimit
        x

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate.
    /// </summary> 
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSizeFunc"> The functional delegate that returns the size of the partition,  the integer index passed to the function 
    /// indicates the partition </param> 
    static member init initFunc partitionSizeFunc (x:DSet<'U>) =
        x.Init(initFunc, partitionSizeFunc)

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate.
    /// </summary> 
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSize"> Size of each partition </param> 
    member x.InitS(initFunc, partitionSize) =
        x.Init(initFunc, ( fun _ -> partitionSize ))

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate.
    /// </summary> 
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSize"> Size of each partition </param> 
    static member initS initFunc partitionSize (x:DSet<'U>) =
        x.InitS(initFunc, partitionSize)

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate, using
    /// a given number of parallel execution per node.
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSizeFunc"> The functional delegate that returns the size of the partition,  the integer index passed to the function 
    /// indicates the partition. 
    /// </param> 
    /// </summary> 
    member x.InitN(initFunc, partitionSizeFunc : int->int->int) =
        x.NumPartitions <- x.Cluster.NumNodes * x.NumParallelExecution
        x.NumReplications <- 1
        x.Dependency <- Source
        x.Function <- Function.Init<'U>( initFunc, partitionSizeFunc x.NumPartitions ) 
        // Trigger of setting the serialization limit parameter within functions. 
        // Automatic set serialization limit
        // x.SerializationLimit <- x.SerializationLimit
        x

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate, using
    /// a given number of parallel execution per node.
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSizeFunc"> The functional delegate that returns the size of the partition,  the integer index passed to the function 
    /// indicates the partition. 
    /// </param> 
    /// <param name="x"> The DSet to operate on </param>
    /// </summary> 
    static member initN(initFunc, partitionSizeFunc : int->int->int) (x:DSet<'U>) =
        x.InitN(initFunc, partitionSizeFunc)

    /// <summary> 
    /// Generate a distributed dataset through a customerized seq functional delegate running on each of the machine. 
    /// Each node runs a local instance of the functional delegate, which generates a seq('U) that forms one partition of DSet('U).
    /// The number of partitions of DSet('U) is N, which is the number of the nodes in the cluster. The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegate in that node. </summary>
    /// <param name="sourceSeqFun"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated DSet('U) </return>
    member x.Source sourceSeqFunc = 
        x.NumPartitions <- x.Cluster.NumNodes
        x.NumReplications <- 1
        x.Mapping <- Array.init x.Cluster.NumNodes ( fun i -> Array.create 1 i )
        x.Dependency <- Source
        x.Function <- Function.Source<'U>( sourceSeqFunc ) 
        x

    /// <summary> 
    /// Generate a distributed dataset through a customerized seq functional delegate running on each of the machine. 
    /// Each node runs a local instance of the functional delegate, which generates a seq('U) that forms one partition of DSet('U).
    /// The number of partitions of DSet('U) is N, which is the number of the nodes in the cluster. The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegate in that node. </summary>
    /// <param name="sourceSeqFun"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated DSet('U) </return>
    static member source sourceSeqFunc (x:DSet<'U>) = 
        x.Source sourceSeqFunc

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// Each node runs num local instance of the functional delegates, each of which forms one partition of the dataset.
    /// The number of partitions of dataset is N * num, where N is the number of the nodes in the cluster, and num is the number of partitions per node. 
    /// The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. </summary>
    /// <param name="sourceNSeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated dataset </return>
    member x.SourceN(num, sourceNSeqFunc) = 
        x.NumPartitions <- x.Cluster.NumNodes * num
        x.NumReplications <- 1
        // i%num is the thread on particular node. 
        x.Mapping <- Array.init (x.Cluster.NumNodes*num) ( fun i -> Array.create 1 (i/num) )
        x.Dependency <- Source
        x.Function <- Function.SourceN<'U>( num, sourceNSeqFunc ) 
        x

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// Each node runs num local instance of the functional delegates, each of which forms one partition of the dataset.
    /// The number of partitions of dataset is N * num, where N is the number of the nodes in the cluster, and num is the number of partitions per node. 
    /// The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. </summary>
    /// <param name="sourceNSeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated dataset </return>
    static member sourceN num sourceNSeqFunc (x:DSet<'U>) =
        x.SourceN(num, sourceNSeqFunc)

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// The number of partitions of dataset is numPartitions. 
    /// If any node in the cluster is not responding, the partition may be rerun at a different node. </summary>
    /// <param name="sourceISeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set, the input parameter is the index (0 based)of the partition.  </param>
    /// <return> generated dataset </return>
    member x.SourceI(numPartitions, sourceISeqFunc) = 
        x.NumPartitions <- numPartitions
        // i%num is the thread on particular node. 
        x.Mapping <- Array.init (numPartitions) ( fun i -> Array.create 1 (i%x.Cluster.NumNodes) )
        x.Dependency <- Source
        x.Function <- Function.SourceI<'U>( numPartitions, sourceISeqFunc ) 
        x

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// The number of partitions of dataset is numPartitions. 
    /// If any node in the cluster is not responding, the partition may be rerun at a different node. </summary>
    /// <param name="sourceISeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set, the input parameter is the index (0 based)of the partition.  </param>
    /// <return> generated dataset </return>
    static member sourceI numPartitions sourceISeqFunc (x:DSet<'U>) = 
        x.SourceI(numPartitions, sourceISeqFunc)

    /// <summary> 
    /// Generate a distributed dataset by importing a customerized functional delegate from a local contract store. The imported functional delegate 
    /// is usually exported by another service (either in the same remote container or in anotherPrajna remote container. 
    /// Each node runs one local instance of the functional delegate, which forms one partition of the dataset.
    /// The number of partitions of dataset is N, where N is the number of the nodes in the cluster. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. 
    /// </summary>
    /// <param name="server"> The servers infor from which the contracts are imported. </param>
    /// <param name="importName"> The name of the contract that is attached to the sequence functional delegate. </param>
    /// <return> generated dataset </return>
    member x.Import(serversInfo, importName) = 
        let wrappedSourceSeqFunc () = 
            let importedFunc = ContractStore.Current.ImportSeqFunction<'U>( serversInfo, importName )
            importedFunc.Invoke()
        x.Source wrappedSourceSeqFunc

    /// <summary> 
    /// Generate a distributed dataset by importing a customerized functional delegate from a local contract store. The imported functional delegate 
    /// is usually exported by another service (either in the same remote container or in anotherPrajna remote container. 
    /// Each node runs one local instance of the functional delegate, which forms one partition of the dataset.
    /// The number of partitions of dataset is N, where N is the number of the nodes in the cluster. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. 
    /// </summary>
    /// <param name="server"> The servers infor from which the contracts are imported. </param>
    /// <param name="importName"> The name of the contract that is attached to the sequence functional delegate. </param>
    /// <return> generated dataset </return>
    static member import servers importName (x:DSet<'U>) = 
        x.Import(servers, importName)

    /// <summary> 
    /// Generate a distributed dataset by importing customerized functional delegates from a local contract store. The imported functional delegate 
    /// is usually exported by another service (either in the same remote container or in anotherPrajna remote container. 
    /// Each node runs multiple local instance of the functional delegates, each of which forms one partition of the dataset.
    /// The number of partitions of dataset is N * num , where N is the number of the nodes in the cluster, and num is the number of contracts 
    /// in the contract list. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. 
    /// </summary>
    /// <param name="server"> The servers infor from which the contracts are imported. </param>
    /// <param name="importNames"> The names of the contracts that are to be imported. </param>
    /// <return> generated dataset </return>
    member x.ImportN(serversInfo, importNames:string[]) = 
        let wrappedSourceSeqFuncN idx = 
            let importedFunc = ContractStore.Current.ImportSeqFunction<'U>( serversInfo, importNames.[idx] )
            importedFunc.Invoke()
        let num = importNames.Length
        x.SourceN(num, wrappedSourceSeqFuncN)

    /// <summary> 
    /// Generate a distributed dataset by importing customerized functional delegates from a local contract store. The imported functional delegate 
    /// is usually exported by another service (either in the same remote container or in anotherPrajna remote container. 
    /// Each node runs multiple local instance of the functional delegates, each of which forms one partition of the dataset.
    /// The number of partitions of dataset is N * num , where N is the number of the nodes in the cluster, and num is the number of contracts 
    /// in the contract list. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. 
    /// </summary>
    /// <param name="server"> The servers infor from which the contracts are imported. </param>
    /// <param name="importNames"> The names of the contracts that are to be imported. </param>
    /// <return> generated dataset </return>
    static member importN servers importNames (x:DSet<'U>) = 
        x.ImportN(servers, importNames)

    // Implement distribute by using SourceI, which copies the complete source data to every node
    member private x.DistributeBySource num sourceSeq = 
        let sourceArr = sourceSeq |> Seq.toArray
        let numPartitions =  x.Cluster.NumNodes * num
        let rangeLength = sourceArr.Length / numPartitions        
        x.SourceI(numPartitions, ( fun i -> let rangeBegin = rangeLength * i
                                            let rangeCnt = if i < numPartitions - 1 then rangeLength else Math.Max (sourceArr.Length - rangeBegin, rangeLength)
                                            ( Array.sub sourceArr rangeBegin rangeCnt ) :> seq<_>
                                 ))

    // [Incomplete] Implement distribute by using sending the required data to every node
    member private x.DistributeBySending num sourceSeq =     
        raise (System.NotImplementedException())    
        x.NumPartitions <- x.Cluster.NumNodes * num 
        x.NumReplications <- 1
        x.Mapping <- Array.init (x.Cluster.NumNodes * num) ( fun i -> Array.create 1 (i/num) )
        x.Dependency <- Source
        x.UpdateFuncState <- DSetUpdateFunctionState.UpdateAtSerialization
        x.UpdateFuncObj <- Function.DistributeN<'U>(num, sourceSeq ) 
        x

    /// <summary> 
    /// Span a sequence (dataset) to a distributed dataset by splitting the sequence to N partitions, with N being the number of nodes in the cluster. 
    /// Each node get one partition of the dataset. The number of partitions of the distributed dataset is N, which is the number of the active nodes in the cluster. 
    /// </summary>
    /// <param name="sourceSeq"> Source dataset </param>
    /// <remarks> See code example DistributedKMeans </remarks>
    /// <return> Generated dataset. </return>
    member x.Distribute sourceSeq = 
        x.DistributeBySource 1 sourceSeq

    /// <summary> 
    /// Span a sequence (dataset) to a distributed dataset by splitting the sequence to N partitions, with N being the number of nodes in the cluster. 
    /// Each node get one partition of the dataset. The number of partitions of the distributed dataset is N, which is the number of the active nodes in the cluster. 
    /// </summary>
    /// <param name="sourceSeq"> Source dataset </param>
    /// <remarks> See code example DistributedKMeans </remarks>
    /// <return> Generated dataset. </return>
    static member distribute sourceSeq (x:DSet<'U>) = 
        x.Distribute sourceSeq

    /// <summary> 
    /// Span a sequence (dataset) to a distributed dataset by splitting the sequence to NUM * N partitions, with NUM being the number of partitions on each node, 
    /// and N being the number of nodes in the cluster. Each node get NUM partition of the dataset. 
    /// </summary>
    /// <param name="num"> Number of partitions in each node. </param>
    /// <param name="sourceSeq"> Source dataset. </param>
    /// <remarks> See code example DistributedKMeans. </remarks>
    /// <return> Generated dataset. </return>
    member x.DistributeN(num, sourceSeq) = 
        x.DistributeBySource num sourceSeq 

    /// <summary> 
    /// Span a sequence (dataset) to a distributed dataset by splitting the sequence to NUM * N partitions, with NUM being the number of partitions on each node, 
    /// and N being the number of nodes in the cluster. Each node get NUM partition of the dataset. 
    /// </summary>
    /// <param name="num"> Number of partitions in each node. </param>
    /// <param name="sourceSeq"> Source dataset. </param>
    /// <remarks> See code example DistributedKMeans. </remarks>
    /// <return> Generated dataset. </return>
    static member distributeN num sourceSeq (x:DSet<'U>) =
        x.DistributeN(num, sourceSeq)

    // ============================================================================================================
    // Execute a delegate remotely
    // ============================================================================================================


    /// <summary> 
    /// Execute a distributed customerized functional delegate on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegate does not run on that node. </summary>
    /// <param name="func"> The customerized functional delegate that runs on each remote container. </param>
    /// <remarks> Implemented via x.Source </remarks>
    /// <return> unit </return>
    member x.Execute func = 
        let wrappedFunc() =
            func() 
            Seq.empty
        let useDSet = x.Source wrappedFunc 
        useDSet.ToSeq() |> Seq.iter ( fun _ -> () ) 
        
    /// <summary> 
    /// Execute a distributed customerized functional delegate on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegate does not run on that node. </summary>
    /// <param name="func"> The customerized functional delegate that runs on each remote container. </param>
    /// <remarks> Implemented via x.Source </remarks>
    /// <return> unit </return>
    static member execute func (x:DSet<'U>) = 
        x.Execute func

    /// <summary> 
    /// Execute num distributed customerized functional delegates on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegates do not run on that node. </summary>
    /// <param name="num"> The number of instances run on each of the node. </param>
    /// <param name="funcN"> The customerized functional delegate that runs on each remote container, the input integer index (from 0) the instance of the functional delegate runs. </param>
    /// <remarks> Implemented via x.Source </remarks>
    /// <return> unit </return>
    member x.ExecuteN(num, funcN) = 
        let wrappedFuncN num = 
            funcN num
            Seq.empty
        let useDSet = x.SourceN(num, wrappedFuncN)
        useDSet.ToSeq() |> Seq.iter ( fun _ -> () ) 

    /// <summary> 
    /// Execute num distributed customerized functional delegates on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegates do not run on that node. </summary>
    /// <param name="num"> The number of instances run on each of the node. </param>
    /// <param name="funcN"> The customerized functional delegate that runs on each remote container, the input integer index (from 0) the instance of the functional delegate runs. </param>
    /// <remarks> Implemented via x.Source </remarks>
    /// <return> unit </return>
    static member executeN num funcN (x:DSet<'U>) =
        x.ExecuteN(num, funcN)

    // ============================================================================================================
    // Transformations for DSet
    // ============================================================================================================

    /// Identity Mapping, the new DSet is the same as the old DSet, with an encode function installed.
    member x.Identity( ) = 
        let newDSet = x.Derived<'U>( DSetMetadataCopyFlag.Passthrough, "_i" )
        newDSet.Function <- Function.Identity<'U>( )
        newDSet

    /// Identity Mapping, the new DSet is the same as the old DSet, with an encode function installed.
    static member identity (x:DSet<'U>) =
        x.Identity()

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate returns true.
    member x.Filter func = 
        x.FilterImpl func

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate returns true.
    static member filter func (x:DSet<'U>) =
        x.Filter func

    /// Applies the given function to each element of the dataset. 
    /// The new dataset comprised of the results for each element where the function returns Some with some value.
    member x.Choose func = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_Choose" )
        // Deterministic name, 
        newDSet.Function <- Function.ChooseFunction<'U, 'U1>( func )
        newDSet

    /// Applies the given function to each element of the dataset. 
    /// The new dataset comprised of the results for each element where the function returns Some with some value.
    static member choose func (x:DSet<'U>) =
        x.Choose func

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function will be applied per collection as the dataset is being distributedly iterated. 
    /// The entire dataset never materialize entirely. 
    member x.Map ( func:'U -> 'U1 ) = 
        x.MapImpl func

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function will be applied per collection as the dataset is being distributedly iterated. 
    /// The entire dataset never materialize entirely. 
    static member map func (x:DSet<'U>) =
        x.Map func

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition
    member x.Mapi ( func:int -> int64 -> 'U -> 'U1 ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_Mapi" )
        newDSet.Function <- Function.MappingIFunction<'U, 'U1>( func )
        newDSet

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition
    static member mapi ( func: int -> int64 -> 'U -> 'U1 ) (x:DSet<'U>) =
        x.Mapi func

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function is an async function, in which elements within the same collection are parallelly 
    /// executed with Async.Parallel
    member x.AsyncMap ( func: 'U -> Async<'U1> ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_aMap" )
        newDSet.Function <- Function.AsyncMappingFunction<'U, 'U1>( func )
        newDSet

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function is an async function, in which elements within the same collection are parallelly 
    /// executed with Async.Parallel
    static member asyncMap func (x:DSet<'U>) =
        x.AsyncMap func

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function is an async function, in which elements within the same collection are parallelly 
    /// executed with Async.Parallel.
    /// The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition.
    member x.AsyncMapi ( func: int -> int64 -> 'U -> Async<'U1> ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_aMapi" )
        newDSet.Function <- Function.AsyncMappingIFunction<'U, 'U1>( func )
        newDSet

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function is an async function, in which elements within the same collection are parallelly 
    /// executed with Async.Parallel.
    /// The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition.
    static member asyncMapi ( func: int -> int64 -> 'U -> Async<'U1> ) (x:DSet<'U>) =
        x.AsyncMapi func

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    member x.ParallelMap ( func: 'U -> Task<'U1> ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_pMap" )
        newDSet.Function <- Function.ParallelMappingFunction<'U, 'U1>( func )
        newDSet

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    static member parallelMap func (x:DSet<'U>) =
        x.ParallelMap func

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    member x.ParallelMapi ( func: int -> int64 -> 'U -> Task<'U1> ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_pMapi" )
        newDSet.Function <- Function.ParallelMappingIFunction<'U, 'U1>( func )
        newDSet

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    static member parallelMapi func (x:DSet<'U>) =
        x.ParallelMapi func

    /// Creates a new dataset whose elements are the results of applying the given function to each collection of the elements 
    /// of the dataset. In the input DSet, a parttition can have multiple collections, 
    /// the size of the which is determined by the SerializationLimit of the cluster.
    member x.MapByCollection ( func:'U[] -> 'U1[] ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_Mbc" )
        newDSet.Function <- Function.MappingCollectionFunction<'U, 'U1>( func )
        newDSet

    /// Creates a new dataset whose elements are the results of applying the given function to each collection of the elements 
    /// of the dataset. In the input DSet, a parttition can have multiple collections, 
    /// the size of the which is determined by the SerializationLimit of the cluster.
    static member mapByCollection (func:'U[] -> 'U1[] ) (x:DSet<'U>) =
         x.MapByCollection func    

    /// Reorganization collection in a dataset to accommodate a certain parallel execution degree. 
    member x.ReorgWDegree numParallelJobs = 
        // Calculating download limit
        let cl = x.GetCluster()
        let numNodes = cl.NumNodes
        let mutable numParallelJobsPerNode = cl.ClusterInfo.ListOfClients.[0].ProcessorCount * 2 // That is the default # of jobs to be run on each node. 
        let mutable nRows = numParallelJobs / numNodes / numParallelJobsPerNode
        if nRows <= 0 then 
            nRows <- 1
            numParallelJobsPerNode <- ( numParallelJobs + numNodes - 1 ) / numNodes
            // Limit # of task that can be launched 
            x.NumParallelExecution <- numParallelJobs
        let reorgDSet = x.RowsReorg( nRows) 
        reorgDSet

    /// Reorganization collection in a dataset to accommodate a certain parallel execution degree. 
    static member reorgWDegree numParallelJobs (x:DSet<'U>) =
        x.ReorgWDegree numParallelJobs

    /// Reorganization collection of a dataset. 
    /// If numRows = 1, the dataset is split into one row per collection. 
    /// If numRows = Int32.MaxValue, the data set is merged so that all rows in a partition is grouped into a single collection. 
    member x.RowsReorg (numRows:int) = 
        let newDSet = x.Identity()
        newDSet.SerializationLimit <- numRows
        newDSet
    /// Reorganization collection of a dataset. 
    /// If numRows = 1, the dataset is split into one row per collection. 
    /// If numRows = Int32.MaxValue, the data set is merged so that all rows in a partition is grouped into a single collection. 
    static member rowsReorg numRows (x:DSet<'U>) = x.RowsReorg numRows

    /// Reorganization collection of a dataset so that each collection has a single row. 
    member x.RowsSplit() = 
        x.RowsReorg 1

    /// Reorganization collection of a dataset so that each collection has a single row. 
    static member rowsSplit (x:DSet<'U>) = x.RowsSplit()

    /// merge all rows of a partition into a single collection object
    member x.RowsMergeAll() = 
        x.RowsReorg Int32.MaxValue

    /// merge all rows of a partition into a single collection object
    static member rowsMergeAll (x:DSet<'U>) = x.RowsMergeAll()

    /// Cache the DSet in RAM for later data analytic job use 
    member private x.CacheInRAMEnumerable() = 
        let newDSet = x.Identity()
        newDSet.Name <- x.Name + "_cEnum"
        newDSet.StorageType <- StorageKind.RAM
        newDSet.CacheType <- CacheKind.EnumerableRetrieve 
        newDSet.FunctionObj.ConstructPartitionCache <- PartitionCacheEnumerable<'U>.ConstructPartitionCacheEnumerable
        newDSet

    /// Cache the DSet in RAM for later data analytic job use 
    member private x.CacheInRAMList() = 
        let newDSet = x.Identity()
        newDSet.Name <- x.Name + "_cList"
        newDSet.StorageType <- StorageKind.RAM
        newDSet.CacheType <- CacheKind.EnumerableRetrieve 
        newDSet.FunctionObj.ConstructPartitionCache <- PartitionCacheList<'U>.ConstructPartitionCacheEnumerable
        newDSet

    //// Cache the DSet in RAM using a ConcurrentDictionary for quick lookup. 
    //    member private x.CacheInRAMDictionary() = 
    //        let newDSet = x.Identity()
    //        newDSet.Name <- x.Name + "_cDic"
    //        newDSet.StorageType <- StorageKind.RAM
    //        newDSet.CacheType <- CacheKind.EnumerableRetrieve ||| CacheKind.ConcurrectDictionary
    //        newDSet.FunctionObj.ConstructPartitionCache <- PartitionCacheKeyValueStore<'K,'V>.ConstructPartitionCacheKeyValue 
    //        newDSet

    /// <summary>
    /// Change the cache to use Concurrent Queue. Note this function is different from CacheInRAM(), it doesn't attach a cache to the DSet, but rather, it changes the cache behavior of the current DSet. 
    /// This function should be used for receiving network DSet, either of .Mutlicast, .Partition, etc..
    /// </summary>
    member private x.UseConcurrentQueueCache() =
        x.StorageType <- StorageKind.RAM
        x.CacheType <- CacheKind.EnumerableRetrieve 
        x.FunctionObj.ConstructPartitionCache <- PartitionCacheConcurrentQueue<'U>.ConstructPartitionCacheConcurrentQueue 
        x
    
    // Note:  We can expose a "cache" method that takes two paramemter:
    //        * One for storage kind
    //        * The other for cache data structure (list vs dictionary)
    //        We will do so when we support more than one storage kind, and when
    //        we identify there are transformations/actions exposed by public API 
    //        can benefit from dictionary powered cache.

    /// Cache DSet content in memory, in raw object form. 
    member x.CacheInMemory() = 
        x.CacheInRAMEnumerable() 

    /// Cache DSet content in RAM, in raw object form. 
    static member cacheInMemory (x:DSet<'U>) = 
        x.CacheInMemory()

    member private x.CacheInSSD() = 
        raise (System.NotImplementedException())
        let newDSet = x.Identity()
        newDSet.StorageType <- StorageKind.SSD      
        newDSet

    /// Cache DSet content in SSD, in bytestream form. 
    static member private cacheInSSD (x:DSet<'U>) = 
        // not implemented yet
        x.CacheInSSD()

    /// Applies the given function to each element of the dataset and concatenates all the results. 
    member x.Collect ( func ) = 
        let newDSet = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_Collect" )
        // Deterministic name, 
        newDSet.Function <- Function.CollectFunction<'U, 'U1>( func )
        newDSet

    /// Applies the given function to each element of the dataset and concatenates all the results. 
    static member collect func (x:DSet<'U>) = 
        x.Collect func

    /// Change Partition within a node
    member internal x.PartitionInNodeParam (param:DParam) (partFunc:'U -> int ) =
        let newDSet = x.Derived<'U>( DSetMetadataCopyFlag.Passthrough, "_partin" )
        x.DependencyDownstream <- MixTo (DependentDSet(newDSet) )
        newDSet.Dependency <- MixFrom (DependentDSet(x) )
        if Utils.IsNotNull param.Mapping then 
            newDSet.Mapping <- param.Mapping 
            newDSet.bEncodeMapping <- true
        if param.bParitionerSet then 
            if param.NumPartitions > 0 then 
                let nump = param.NumPartitions
                newDSet.NumPartitions <- nump
                if Utils.IsNull newDSet.Mapping || newDSet.Mapping.Length <> newDSet.NumPartitions then 
                    newDSet.OperationToGeneratePartitionMapping()
                    newDSet.bEncodeMapping <- true
            newDSet.IsPartitionByKey <- param.IsPartitionByKey
        newDSet.Function <- Function.RepartitionWithinNode<'U>( newDSet.Partitioner, x.IsPartitionByKey, partFunc )
        newDSet

    /// Change partition within a node
    static member internal partitionInNodeParam (param:DParam) (partFunc:'U -> int ) (x:DSet<'U>) =
        x.PartitionInNodeParam param partFunc

    /// <summary> Send a DSet over network. </summary>
    /// <param name="param"> Optional control parameter. The relevant parameters are param.Cluster, which specifies that the cluster DSet is sent to. 
    /// and param.Mapping, which direct which node a certain partition is sent to. </param>
    member private x.SendOverNetworkParam (param:DParam) = 
        let newDSet = x.Identity( )
        let encStream = x.EncodeTo() 
        if Utils.IsNotNull param.Cluster && param.Cluster<>newDSet.Cluster then 
            newDSet.Cluster <- param.Cluster
        let bNewMapping = newDSet.bClusterSet || Utils.IsNotNull param.Mapping 
        if bNewMapping then 
            if Utils.IsNotNull param.Mapping then 
                newDSet.Mapping <- param.Mapping
            else
                newDSet.SetupPartitionMapping()
        let networkStream = encStream.SendAcrossNetwork()
        newDSet.Dependency <- WildMixFrom ( DependentDSet(x), DependentDStream(networkStream) )
        x.DependencyDownstream <- WildMixTo ( DependentDSet(newDSet), DependentDStream(encStream) )
        networkStream.DependencyDownstream <- DecodeTo (DependentDObject(newDSet) )
        networkStream.Name <- x.Name + "_nstream"
        newDSet.Name <- x.Name + "_net"
        newDSet

    /// <summary> Encode and send a DSet across network. </summary>
    member private x.SendOverNetwork( ) = 
        let newDSet = x.Identity( )
        let encStream = x.EncodeTo() 
        let networkStream = encStream.SendAcrossNetwork()
        newDSet.Dependency <- WildMixFrom ( DependentDSet(x), DependentDStream(networkStream) )
        x.DependencyDownstream <- WildMixTo ( DependentDSet(newDSet), DependentDStream(encStream) )
        networkStream.DependencyDownstream <- DecodeTo (DependentDObject(newDSet) )
        networkStream.Name <- x.Name + "_nstream"
        newDSet.Name <- x.Name + "_net"
        newDSet

    /// <summary> Multicast a DSet over network, replicate its content to all peers in the cluster. </summary>
    /// <param name="param"> Optional control parameter. The relevant parameters are param.Cluster, which specifies that the cluster DSet is sent to. 
    /// and param.Mapping, which direct which node a certain partition is sent to. </param>
    member private y.MulticastParam (param:DParam)  = 
        let x = y.Identity()
        let newDSet = x.Identity( )
        let encStream = x.EncodeTo() 
        if Utils.IsNotNull param.Cluster && param.Cluster<>newDSet.Cluster then 
            newDSet.Cluster <- param.Cluster
        let networkStream = encStream.MulticastAcrossNetwork()
        newDSet.Dependency <- WildMixFrom ( DependentDSet(x), DependentDStream(networkStream) )
        x.DependencyDownstream <- WildMixTo ( DependentDSet(newDSet), DependentDStream(encStream) )
        networkStream.DependencyDownstream <- DecodeTo (DependentDObject(newDSet) )
        networkStream.Name <- x.Name + "_mcastst"
        newDSet.Name <- x.Name + "_mcast"
        newDSet.NumReplications <- newDSet.Cluster.NumNodes
        newDSet

    /// <summary> Multicast a DSet over network, replicate its content to all peers in the cluster. </summary>
    member x.Multicast()  = 
        x.MulticastParam (DSet.DefaultParam())

    /// <summary> Multicast a DSet over network, replicate its content to all peers in the cluster. </summary>
    static member multicast (x:DSet<'U>) = 
        x.Multicast()

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster
    member internal x.RepartitionToNodeParam (param:DParam) (partFunc:'U -> int ) =
        let stage1DSet = x.PartitionInNodeParam param partFunc
        stage1DSet.Name <- x.Name + "_p1"
//  move to PartitionInNodeParam
//        if param.bParitionerSet then 
//            if param.NumPartitions > 0 then 
//                stage1DSet.NumPartitions <- param.NumPartitions
//            stage1DSet.IsPartitionByKey <- param.IsPartitionByKey
        let stage2DSet = 
            if param.bSerializationLimitSet then 
                stage1DSet.RowsReorg( param.SerializationLimit )
            else
                stage1DSet           
        let finalDSet = stage2DSet.SendOverNetwork()
        finalDSet.Name <- x.Name + "_part"
        finalDSet

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster
    member private x.RepartitionToNode() = 
        x.RepartitionToNodeParam (DSet.DefaultParam()) (Utils.GetHashCode)

   /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    member x.RepartitionP (param:DParam, partFunc:'U -> int ) = 
        x.RepartitionToNodeParam param ( partFunc )

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    static member repartitionP (param:DParam) (partFunc:'U -> int ) (x:DSet<'U>) = 
        x.RepartitionP(param, partFunc)

    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    member x.RepartitionN(numPartitions, partFunc:'U -> int ) = 
        let param = DSet.DefaultParam()
        param.NumPartitions <- numPartitions
        x.RepartitionP(param, partFunc)

    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    static member repartitionN numPartitions (partFunc:'U -> int ) (x:DSet<'U>) = 
        x.RepartitionN(numPartitions, partFunc)

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    member x.Repartition (partFunc:'U -> int ) = 
        x.RepartitionP(DSet.DefaultParam(), partFunc)

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    static member repartition (partFunc:'U -> int ) (x:DSet<'U>) = 
        x.Repartition partFunc

    /// Group all values that correspond to a similar key in a partition to a List function
    static member private groupByValue (x:DSet<'K*'V>) = 
        let newDSet = x.Derived<'K*System.Collections.Generic.List<'V>>( DSetMetadataCopyFlag.Passthrough, "_gbv" )
        newDSet.Function <- Function.GroupByValueFunction<'K, 'V>( )
        newDSet

    /// filter DSet by Key, Value 
    /// MapReduce Function
    /// Map: 'K,'V -> IEnumerable<'K1,'V1>
    /// Reduce 'K1, List ('V1) -> <'K2, 'V2 >
    member private x.MapReduceInRamPWithPartitionFunction (param:DParam, mapFunc:'U->seq<'K1*'V1>, partFunc : 'K1 -> int, reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) =
        let collect1DSet = x.Collect( mapFunc )
        let collectDSet = 
            if param.PreGroupByReserialization>0 then 
                collect1DSet |> DSet.rowsReorg( param.PreGroupByReserialization )
            else 
                collect1DSet
        collectDSet.Name <- x.Name + "_mr1"
        let groupByDSet =  DSet<'K1*'V1>.groupByValue collectDSet
        groupByDSet.Name <- x.Name + "_mr2"
        // Map Reduce is always partition by Key
        param.IsPartitionByKey <- true
        let repartDSet = groupByDSet.RepartitionP(param, (fun (k, _) -> partFunc k))
        repartDSet.Name <- x.Name + "_mr4"
        let collectAllRows = repartDSet.RowsReorg( Int32.MaxValue ) 
        let groupByDst = collectAllRows.Derived<'K1*System.Collections.Generic.List<'V1>>( DSetMetadataCopyFlag.Passthrough, "_gbvs" )
        groupByDst.Function <- Function.GroupByValueCollectionFunction<'K1, 'V1>( )
        groupByDst.Name <- x.Name + "_mr5"
        let outputDSet = 
            if param.PostGroupByReserialization > 0 then 
                groupByDst.Map( reduceFunc ) |> DSet.rowsReorg( param.PostGroupByReserialization )
            else
                groupByDst.Map( reduceFunc )
        outputDSet.Name <- x.Name + "_mapreduce"
        outputDSet

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="partFunc"> After map, used for user defined repartition </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param> 
    member x.MapReducePWithPartitionFunction (param:DParam, mapFunc:'U->seq<'K1*'V1>, partFunc, reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) =
        x.MapReduceInRamPWithPartitionFunction (param, mapFunc, partFunc, reduceFunc)

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="partFunc"> After map, used for user defined repartition </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param> 
    static member mapReducePWithPartitionFunction (param:DParam) (mapFunc:'U->seq<'K1*'V1>) (partFunc) (reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) (x:DSet<'U>) =
        x.MapReducePWithPartitionFunction(param, mapFunc, partFunc, reduceFunc)

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param>  
    member x.MapReduceP (param:DParam, mapFunc:'U->seq<'K1*'V1>, reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) = 
        x.MapReduceInRamPWithPartitionFunction(param, mapFunc, (Utils.GetHashCode), reduceFunc)

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param> 
    static member mapReduceP (param:DParam) (mapFunc:'U->seq<'K1*'V1>) (reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) (x:DSet<'U>) = 
        x.MapReduceP(param, mapFunc, reduceFunc)

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param>  
    member x.MapReduce (mapFunc:'U->seq<'K1*'V1>, reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) = 
        x.MapReduceInRamPWithPartitionFunction((DSet.DefaultParam()), mapFunc, (Utils.GetHashCode), reduceFunc)

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param> 
    static member mapReduce (mapFunc:'U->seq<'K1*'V1>) (reduceFunc:'K1*System.Collections.Generic.List<'V1>->'U2 ) (x:DSet<'U>) = 
        x.MapReduce(mapFunc, reduceFunc)

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSortP (param:DParam, partFunc:'U->int, comparer:IComparer<'U>)=
        let collectDSet = 
            if param.PreGroupByReserialization>0 then 
                x |> DSet.rowsReorg( param.PreGroupByReserialization )
            else 
                x
        let sortedDSet = collectDSet.RepartitionP(param, partFunc)
        sortedDSet.Name <- x.Name + "_sort1"
        sortedDSet.StorageType <- StorageKind.RAM
        sortedDSet.CacheType <- CacheKind.SortedRetrieve
        sortedDSet.FunctionObj.ConstructPartitionCache <- 
            PartitionCacheSortedSet<'U>.ConstructPartitionCacheSortedSet( comparer )
        if param.PostGroupByReserialization > 0 then 
            sortedDSet.SerializationLimit <- param.PostGroupByReserialization    
        sortedDSet

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    static member binSortP param partFunc comparer (x:DSet<'U>) =
        x.BinSortP(param, partFunc, comparer)

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSortN(numPartitions, partFunc, comparer) =
        let param = DSet.DefaultParam()
        param.NumPartitions <- numPartitions
        x.BinSortP(param, partFunc, comparer)

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    static member binSortN numPartitions partFunc comparer (x:DSet<'U>) =
        x.BinSortN(numPartitions, partFunc, comparer)

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSort(partFunc, comparer) =
        x.BinSortP(DSet.DefaultParam(), partFunc, comparer)

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    static member binSort partFunc comparer (x:DSet<'U>) =
        x.BinSort(partFunc, comparer)

    /// <summary> 
    /// Merge the content of multiple dataset into a single dataset. The original dataset become partitions of the resultant dataset. 
    /// This can be considered as merge dataset by rows, and all dataset have the same column structure. </summary>
    /// <param name="source"> Source dataset </param>
    /// <return> Merged dataset </return>
    static member union (source:seq<DSet<'U>>) = 
        let srcArray = source |> Seq.toArray
        let newDSet = srcArray.[0].Derived<'U>( DSetMetadataCopyFlag.Passthrough, "_union" )
        newDSet.Function <- Function.Identity<'U>( )
        let dep = srcArray |> Array.map ( fun dset -> DependentDSet(dset)) 
        newDSet.Dependency <- UnionFrom (List<_>( dep ))
        let depDSet = DependentDSet(newDSet)
        for srcDSet in srcArray do 
            srcDSet.DependencyDownstream <- UnionTo depDSet
        newDSet

    /// <summary> 
    /// Merge the content of multiple dataset into a single dataset. The original dataset become partitions of the resultant dataset. 
    /// This can be considered as merge dataset by rows, and all dataset have the same column structure. </summary>
    /// <param name="source"> Source dataset </param>
    /// <return> Merged dataset </return>
    member x.Union (source:seq<DSet<'U>>) = 
        let newSeq = seq { yield x
                           yield! source }
        DSet<'U>.union newSeq

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the two DSets pairwise
    /// The two DSet must have the same partition mapping structure and same number of element (e.g.), established via Split.
    member x.Map2 (func: 'U -> 'U1 -> 'V, x1:DSet<'U1>) = 
        let newDSet = x.Derived<'V>( DSetMetadataCopyFlag.Passthrough, "_map2" )
        newDSet.EstablishCorrelatedMix( [| x; x1 |] )
        newDSet.Function <- Function.CorrelatedMix2<'U,'U1,'V>( func )
        newDSet

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the two DSets pairwise
    /// The two DSet must have the same partition mapping structure and same number of element (e.g.), established via Split.
    static member map2 (func: 'U -> 'U1 -> 'V) (x:DSet<'U>) (x1:DSet<'U1>) = 
        x.Map2(func, x1)

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the three DSets pairwise
    member x.Map3 (func: 'U -> 'U1 -> 'U2 -> 'V, x1:DSet<'U1>, x2:DSet<'U2>) = 
        let newDSet = x.Derived<'V>( DSetMetadataCopyFlag.Passthrough, "_map2" )
        newDSet.EstablishCorrelatedMix( [| x; x1; (x2) |] )
        newDSet.Function <- Function.CorrelatedMix3<'U,'U1,'U2,'V>( func )
        newDSet

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the three DSets pairwise
    static member map3 (func: 'U -> 'U1 -> 'U2 -> 'V) (x:DSet<'U>) (x1:DSet<'U1>) (x2:DSet<'U2>)= 
        x.Map3(func, x1, x2)

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the four DSets pairwise
    member x.Map4 (func: 'U -> 'U1 -> 'U2 -> 'U3 -> 'V, x1:DSet<'U1>, x2:DSet<'U2>, x3:DSet<'U3>) = 
        let newDSet = x.Derived<'V>( DSetMetadataCopyFlag.Passthrough, "_map4" )
        newDSet.EstablishCorrelatedMix( [| x; x1; x2; x3 |] )
        newDSet.Function <- Function.CorrelatedMix4<'U,'U1,'U2,'U3,'V>( func )
        newDSet

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the four DSets pairwise
    static member map4 (func: 'U -> 'U1 -> 'U2 -> 'U3 -> 'V) (x:DSet<'U>) (x1:DSet<'U1>) (x2:DSet<'U2>) (x3:DSet<'U3>) = 
        x.Map4(func, x1, x2, x3)

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix (x1:DSet<'U1>) = 
        x.Map2((fun a b -> (a,b)), x1)

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    static member mix (x:DSet<'U>) (x1:DSet<'U1>) = 
        x.Mix x1

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix2 (x1:DSet<'U1>) = 
        x.Map2((fun a b -> (a, b)), x1)

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    static member mix2 (x:DSet<'U>) (x1:DSet<'U1>) = 
        x.Mix2 x1

    /// <summary> 
    /// Mixing three DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix3 (x1:DSet<'U1>, x2:DSet<'U2>) = 
        x.Map3 ((fun a b c -> (a,b,c) ), x1, x2)

    /// <summary> 
    /// Mixing three DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    static member mix3 (x:DSet<'U>) (x1:DSet<'U1>) (x2:DSet<'U2>)= 
        x.Mix3(x1, x2)

    /// <summary> 
    /// Mixing four DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix4(x1:DSet<'U1>, x2:DSet<'U2>, x3:DSet<'U3>) = 
        x.Map4 ((fun a b c d -> (a,b,c,d)), x1, x2, x3)

    /// <summary> 
    /// Mixing four DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    static member mix4 (x:DSet<'U>) (x1:DSet<'U1>) (x2:DSet<'U2>) (x3:DSet<'U3>) = 
        x.Mix4(x1, x2, x3)

    /// <summary> 
    /// Cross Join, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc:'U->'U1->'U2
    /// The resultant set is the product of Dset&lt;'U> and Dset&lt;'U1>
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoin (mapFunc:'U -> 'U1 ->'U2, x1:DSet<'U1>) =
        let multicastDSet = x1.Multicast().UseConcurrentQueueCache()
        let newDSet = x.Derived<'U2>( DSetMetadataCopyFlag.Passthrough, "_cjoin" )
        newDSet.EstablishCrossJoin x multicastDSet
        newDSet.Function <- Function.CrossJoin<'U,'U1,'U2>( mapFunc ) 
        newDSet

    /// <summary> 
    /// Cross Join, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc:'U->'U1->'U2
    /// The resultant set is the product of Dset&lt;'U> and Dset&lt;'U1>
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    static member crossJoin (mapFunc:'U -> 'U1 ->'U2 ) (x:DSet<'U>) (x1:DSet<'U1>) = 
        x.CrossJoin(mapFunc, x1)

    /// <summary> 
    /// Cross Join with Filtering, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc:'U->'U1->'U2 option. 
    /// Filter out all those result that returns None. 
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoinChoose (mapFunc:'U -> 'U1 ->'U2 option, x1:DSet<'U1>) =
        let multicastDSet = x1.Multicast().UseConcurrentQueueCache()
        let newDSet = x.Derived<'U2>( DSetMetadataCopyFlag.Passthrough, "_cjchoose" )
        newDSet.EstablishCrossJoin x multicastDSet
        newDSet.Function <- Function.CrossJoinChoose<'U,'U1,'U2>( mapFunc ) 
        newDSet

    /// <summary> 
    /// Cross Join with Filtering, for each entry in x:DSet&lt;'U> and x1:DSet&lt;'U1> apply mapFunc:'U->'U1->'U2 option. 
    /// Filter out all those result that returns None. 
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:DSet&lt;'U> and each entry in x1:DSet&lt;'U1>  </param>
    /// <param name="x1"> The DSet&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    static member crossJoinChoose (mapFunc:'U -> 'U1 ->'U2 option ) (x:DSet<'U>) (x1:DSet<'U1>) = 
        x.CrossJoinChoose(mapFunc, x1)


    /// <summary> 
    /// Cross Join with Fold, for each entry in x:DSet&lt;'U> and x1:DSet&lt;'U1> apply mapFunc:'U->'U1->'U2, and foldFunc: 'S->'U2->'S
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:DSet&lt;'U> and each entry in x1:DSet&lt;'U1>  </param>
    /// <param name="foldFunc"> The fold function that is executed for each of the map result </param>
    /// <param name="x1"> The DSet&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoinFold (mapFunc:'U -> 'U1 ->'U2, foldFunc:'S -> 'U2 ->'S, initialState, x1:DSet<'U1>) =
        let multicastDSet = x1.Multicast().UseConcurrentQueueCache()
        let newDSet = x.Derived<'S>( DSetMetadataCopyFlag.Passthrough, "_cjfold" )
        newDSet.EstablishCrossJoin x multicastDSet
        newDSet.Function <- Function.CrossJoinFold<'U,'U1,'U2,'S>( mapFunc, foldFunc, initialState ) 
        newDSet

    /// <summary> 
    /// Cross Join with Fold, for each entry in x:DSet&lt;'U> and x1:DSet&lt;'U1> apply mapFunc:'U->'U1->'U2, and foldFunc: 'S->'U2->'S
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:DSet&lt;'U> and each entry in x1:DSet&lt;'U1>  </param>
    /// <param name="foldFunc"> The fold function that is executed for each of the map result </param>
    /// <param name="x1"> The DSet&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    static member crossJoinFold (mapFunc:'U -> 'U1 ->'U2) (foldFunc:'S -> 'U2 ->'S) (initialState) (x:DSet<'U>) (x1:DSet<'U1>) =
        x.CrossJoinFold(mapFunc, foldFunc, initialState, x1)

    // ============================================================================================================
    // Setup push/pull dataflow
    // ============================================================================================================

    /// <summary>
    /// Split the DSet in n+1 ways. One of the DSet is pulled, the other n DSet will in push dataflow. 
    /// </summary> 
    member private y.Bypass( nWays ) = 
        let x = y.IfSourceIdentity() :?> DSet<_>
        let childrenDSet = Array.init (nWays+1) ( fun i -> let sDSet = x.Identity()
                                                           sDSet.Name <- x.Name + "_b" + i.ToString() 
                                                           sDSet )
        x.EstablishBypass( childrenDSet |> Seq.map ( DSet.toBase ) )         
        childrenDSet.[0], ( Array.sub childrenDSet 1 (childrenDSet.Length - 1) )
    
    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    member x.Bypass( ) = 
        let passthroughDSet, sidewayDSets = x.Bypass( 1 )
        passthroughDSet, sidewayDSets.[0]
    
    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    static member bypass (x:DSet<'U>) = x.Bypass()
       
    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    member x.Bypass2() = x.Bypass()

    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    static member bypass2 (x:DSet<'U>) = x.Bypass2()

    /// Bypass the Dset in 3 ways. One of the DSet is pulled, the other two DSets will be in push dataflow. 
    member x.Bypass3() = 
        let dset1, dsets = x.Bypass( 2 ) 
        let dset2 = dsets.[0]
        let dset3 = dsets.[1]
        dset1, dset2, dset3
    
    /// Bypass the Dset in 3 ways. One of the DSet is pulled, the other two DSets will be in push dataflow. 
    static member bypass3 (x:DSet<'U>) = x.Bypass3()
    
    /// Bypass the Dset in 4 ways. One of the DSet is pulled, the other three DSets will be in push dataflow. 
    member x.Bypass4() = 
        let dset1, dsets = x.Bypass( 3 ) 
        let dset2 = dsets.[0]
        let dset3 = dsets.[1]
        let dset4 = dsets.[2]
        dset1, dset2, dset3, dset4

    /// Bypass the Dset in 4 ways. One of the DSet is pulled, the other three DSets will be in push dataflow. 
    static member bypass4 (x:DSet<'U>) = x.Bypass4()

    /// Bypass the Dset in n ways. One of the DSet is pulled, the other n - 1 DSets will be in push dataflow. 
    member x.BypassN nWays = x.Bypass( nWays - 1 )

    /// Bypass the Dset in n ways. One of the DSet is pulled, the other n - 1 DSets will be in push dataflow. 
    static member bypassN nWays (x:DSet<'U>) = x.BypassN( nWays )

    /// <summary>
    /// Correlated split a dataset to two, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// </summary> 
    member x.Split2(fun0, fun1) = 
        let newDSet1 = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_s1" )
        newDSet1.Function <- Function.MappingFunction<'U, 'U1>( fun1 )
        let newDSet0 = x.Derived<'U0>( DSetMetadataCopyFlag.Passthrough, "_s0" )
        newDSet0.Function <- Function.MappingFunction<'U, 'U0>( fun0 )
        x.EstablishBypass( [ newDSet0; newDSet1 ] )
        newDSet0, newDSet1
        
    /// <summary>
    /// Correlated split a dataset to two, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// </summary> 
    static member split2 fun0 fun1 (x:DSet<'U>) = 
        x.Split2(fun0, fun1)

    /// <summary>
    /// Correlated split a dataset to three, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// </summary> 
    member x.Split3(fun0, fun1, fun2) = 
        let newDSet2 = x.Derived<'U2>( DSetMetadataCopyFlag.Passthrough, "_s2" )
        newDSet2.Function <- Function.MappingFunction<'U, 'U2>( fun2 ) 
        let newDSet1 = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_s1" )
        newDSet1.Function <- Function.MappingFunction<'U, 'U1>( fun1 )
        let newDSet0 = x.Derived<'U0>( DSetMetadataCopyFlag.Passthrough, "_s0" )
        newDSet0.Function <- Function.MappingFunction<'U, 'U0>( fun0 )
        x.EstablishBypass( [ newDSet0; newDSet1; newDSet2 ] )
        newDSet0, newDSet1, newDSet2
    
    /// <summary>
    /// Correlated split a dataset to three, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// </summary> 
    static member split3 fun0 fun1 fun2 (x:DSet<'U>) = 
        x.Split3(fun0, fun1, fun2)

    /// <summary>
    /// Correlated split a dataset to four, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// </summary> 
    member x.Split4(fun0, fun1, fun2, fun3) = 
        let newDSet3 = x.Derived<'U3>( DSetMetadataCopyFlag.Passthrough, "_s3" )
        newDSet3.Function <- Function.MappingFunction<'U, 'U3>( fun3 ) 
        let newDSet2 = x.Derived<'U2>( DSetMetadataCopyFlag.Passthrough, "_s2" )
        newDSet2.Function <- Function.MappingFunction<'U, 'U2>( fun2 ) 
        let newDSet1 = x.Derived<'U1>( DSetMetadataCopyFlag.Passthrough, "_s1" )
        newDSet1.Function <- Function.MappingFunction<'U, 'U1>( fun1 )
        let newDSet0 = x.Derived<'U0>( DSetMetadataCopyFlag.Passthrough, "_s0" )
        newDSet0.Function <- Function.MappingFunction<'U, 'U0>( fun0 )
        x.EstablishBypass( [ newDSet0; newDSet1; newDSet2; newDSet3 ] )
        newDSet0, newDSet1, newDSet2, newDSet3

    /// <summary>
    /// Correlated split a dataset to four, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// They can be combined later by Map4 transforms. 
    /// </summary> 
    static member split4 fun0 fun1 fun2 fun3 (x:DSet<'U>) = 
        x.Split4(fun0, fun1, fun2, fun3) 

    // ============================================================================================================
    // Try to find a DSet
    // ============================================================================================================

    /// <summary> 
    /// Find dataset with name that matches the search pattern.</summary>
    /// <param name="cluster"> Find the DSet that are on the cluster </param>
    /// <param name="searchPattern"> Find DSet with name that matches the search Patterns </param>
    /// <return> DSet[] </return>
    static member tryFind (cluster:Cluster) (searchPattern:string) = 
            let rootpath = DSet.RootMetadataPath()
            let dirs = Directory.GetDirectories( rootpath, searchPattern, SearchOption.AllDirectories )
            let dsetLst = List<_>( dirs.Length )
            for filename in dirs do 
                let idx = filename.IndexOf( rootpath, StringComparison.CurrentCultureIgnoreCase )
                if idx >= 0 then 
                    let name = filename.Substring( idx + rootpath.Length + 1 )
                    try 
                        let dset = DSet<'U>( Cluster = cluster, Name = name )
                        if dset.TryLoadDSetMetadata( false ) then 
                            dsetLst.Add( dset )    
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Fail to load DSet %s with exception %A" name e ))
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Fail to locate DSet name within folder %s, rootpath=%s" filename rootpath))
            dsetLst.ToArray()
