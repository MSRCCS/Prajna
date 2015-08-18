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
        DKV.fs
  
    Description: 
        DKV Extensions for DSet<'K*'V> 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2015
    
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

/// Functions for DSet<'U> when 'U represents a key-value pair with type 'K*'V
type DKV<'K, 'V when 'K : equality> =
    // Static functions
    // * Use currying form

    /// Apply a partition function, repartition elements by the key across nodes in the cluster according to the setting specified by "param".
    static member repartitionPByKey (param:DParam) (partFunc:'K -> int ) (x:DSet<'K*'V>) = 
        x.RepartitionToNodeParam param (fun (k, _) -> partFunc k)

    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    static member repartitionNByKey numPartitions (partFunc:'K -> int ) (x:DSet<'K*'V>) = 
        let param = DSet.DefaultParam()
        param.NumPartitions <- numPartitions
        x |> DKV.repartitionPByKey param partFunc

    /// Apply a partition function, repartition elements across nodes in the cluster. The number of partitions remains unchanged.
    static member repartitionByKey (partFunc:'K -> int ) (x:DSet<'K*'V>) = 
        x |> DKV.repartitionPByKey (DSet.DefaultParam()) partFunc

    /// Map element to one unique key, and repartition the DKV to ensure that each 
    /// unique key is mapped to a different partition 
    /// Note: This operation triggers the evaluation of all previous transformations up to this point.
    ///       It's different from other transformations/actions. 
    ///       Make it private for now, user can implement it by herself if that's what wanted.
    static member private repartitionByUniqueKey ( partFunc: 'K -> 'K1 ) (x:DSet<'K*'V>) =
        // Fold 'State doesnot involve in multithread operation in Prajna, use of Dictionary is OK. 
        let foldFunc (set: HashSet<_>) (k:'K) = 
            let key = partFunc( k )
            let s = if Utils.IsNull set then HashSet<_>() else set
            s.Add(key) |> ignore
            s
        let aggrFunc ( set1:HashSet<_>) (set2:HashSet<_>) =
            let first, second = if Utils.IsNotNull set1 then set1,set2 else set2, set1
            if Utils.IsNotNull first then
                if Utils.IsNotNull second then
                    first.UnionWith(second)
                first
            else
                null
        let d = x |> DSet.cacheInMemory |> DSet.map (fun (k, v) -> k )
        let set = d.FoldWithCommonStatePerNode(foldFunc, aggrFunc, null)
        let dic = Dictionary<_,_>()
        let npart = ref 0
        for k in set do 
            dic.[k] <- !npart
            npart := !npart + 1
        let wrapperPartFunc k = 
            dic.[partFunc k]
        x |> DKV.repartitionNByKey dic.Count wrapperPartFunc

    /// Group all values that correspond to a similar key in a partition to a List function
    static member private groupByValue (x:DSet<'K*'V>) = 
        let newDSet = x.Derived<'K*System.Collections.Generic.List<'V>>( DSetMetadataCopyFlag.Passthrough, "_gbv" )
        newDSet.Function <- Function.GroupByValueFunction<'K, 'V>( )
        newDSet

    /// <summary> Group all values of the same key to a List. 
    /// </summary> 
    /// <param name="serializationNum"> The collection size used before sending over network to improve efficiency </param>
    static member groupByKeyN serializationNum (x:DSet<'K*'V>) =
        let groupByDKV = 
            if serializationNum > 0 then 
                let reorgDKV = x.RowsReorg( serializationNum )
                reorgDKV.Name <- x.Name + "_gb0"
                x |> DKV.groupByValue
            else
                x |> DKV.groupByValue
        groupByDKV.Name <- x.Name + "_gb1"
        let repartDKV = DKV.repartitionByKey (Utils.GetHashCode) groupByDKV
        repartDKV.Name <- x.Name + "_gb2"
        let collectAllRows = repartDKV.RowsReorg( Int32.MaxValue ) 
        let groupByDst = collectAllRows.Derived<'K*System.Collections.Generic.List<'V>>( DSetMetadataCopyFlag.Passthrough, "_gbvs" )
        groupByDst.Function <- Function.GroupByValueCollectionFunction<'K, 'V>( )
        groupByDst.Name <- x.Name + "_mr5"
        groupByDst

    /// Group all values of the same key to a List. 
    static member groupByKey (x:DSet<'K*'V>) =
        x |> DKV.groupByKeyN DeploymentSettings.DefaultGroupBySerialization

    /// <summary>
    /// Aggregate all values of a unique key of the DKV togeter. Caution: as this function uses mapreduce, the network cost is not negligble. If the aggregated result is to be returned to the client, 
    /// rather than further used in the cluster, the .fold function should be considered instead for performance. 
    /// </summary>
    /// <param name="reduceFunc"> Reduce Function, see Seq.reduce for examples </param>
    static member reduceByKey reduceFunc (x:DSet<'K*'V>) = 
        let useMapFunc elem = Seq.singleton elem
        let useReduceFunc (key, valueList:System.Collections.Generic.List<'V>) = 
            let en = valueList |> Seq.reduce reduceFunc
            key, en
        x.MapReduce(useMapFunc, useReduceFunc)

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    static member binSortPByKey (param : DParam) (partFunc : 'K -> int) (comparer:IComparer<'K>) (x:DSet<'K*'V>) =
        param.IsPartitionByKey <- true
        let kvComparer() =
            { new IComparer<'K*'V> with
                    member this.Compare(x, y) =
                        comparer.Compare(fst x, fst y) }
        x.BinSortP(param, (fun (k, _) -> partFunc k), kvComparer())

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    static member binSortNByKey numPartitions (partFunc : 'K -> int) (comparer:IComparer<'K>) (x:DSet<'K*'V>) =
        let param = DSet.DefaultParam()
        param.NumPartitions <- numPartitions
        x |> DKV.binSortPByKey param partFunc comparer

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary>  
    static member binSortByKey (partFunc : 'K -> int) (comparer:IComparer<'K>) (x:DSet<'K*'V>) =
        x |> DKV.binSortPByKey (DSet.DefaultParam()) partFunc comparer

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate on key returns true.
    static member filterByKey func (x:DSet<'K*'V>) =
        let newDSet = x.Derived<'K*'V>( DSetMetadataCopyFlag.Passthrough, "_kFilter" )
        // Deterministic name, 
        newDSet.Function <- Function.FilteringKeyFunction<'K, 'V>( func )
        newDSet        

    /// Create a new dataset by transforming only the value of the original dataset
    static member mapByValue func (x:DSet<'K*'V>) =
        let newDSet = x.Derived<'K*'V1>( DSetMetadataCopyFlag.Passthrough, "_vMap" )
        newDSet.Function <- Function.MappingValueFunction<'K, 'V, 'V1>( func )
        newDSet

    /// async map DKV by value
    static member asyncMapByValue func (x:DSet<'K*'V>) =
        let newDSet = x.Derived<'K*'V1>( DSetMetadataCopyFlag.Passthrough, "_avMap" )
        newDSet.Function <- Function.AsyncMappingValueFunction<'K, 'V, 'V1>( func )
        newDSet

    /// <summary>
    /// Map DKV by value, in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    static member parallelMapByValue func (x:DSet<'K*'V>) =
        let newDSet = x.Derived<'K*'V1>( DSetMetadataCopyFlag.Passthrough, "_pvMap" )
        newDSet.Function <- Function.ParallelMappingValueFunction<'K, 'V, 'V1>( func )
        newDSet

    /// Inner join the two DKVs by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member innerJoinByMergeAfterBinSortByKey comp (func:'V->'V1->'V2) (x:DSet<'K*'V>) (x1:DSet<'K*'V1>) = 
        let newDSet = x.Derived<'K*'V2>( DSetMetadataCopyFlag.Passthrough, "_ijoin" )
        newDSet.EstablishCorrelatedMix( [| x; x1 |] )
        newDSet.Function <- Function.InnerSortedJoinKV<'V,'V1,'V2,'K>( comp, func )
        newDSet

    /// Left outer join DKV 'x' with DKV 'x1' by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member leftOuterJoinByMergeAfterBinSortByKey comp (func:'V->'V1 option->'V2) (x:DSet<'K*'V>) (x1:DSet<'K*'V1>) = 
        let newDSet = x.Derived<'K*'V2>( DSetMetadataCopyFlag.Passthrough, "_lojoin" )
        newDSet.EstablishCorrelatedMix( [| x; x1 |] )
        newDSet.Function <- Function.LeftOuterSortedJoinKV<'V,'V1,'V2,'K>( comp, func )
        newDSet      
        
    /// Right outer join the DKV 'x' with DKV 'x1' by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member rightOuterJoinByMergeAfterBinSortByKey comp (func:'V option->'V1->'V2) (x:DSet<'K*'V>) (x1:DSet<'K*'V1>) =           
        let newDSet = x.Derived<'K*'V2>( DSetMetadataCopyFlag.Passthrough, "_rojoin" )
        newDSet.EstablishCorrelatedMix( [| x; x1 |] )
        newDSet.Function <- Function.RightOuterSortedJoinKV<'V,'V1,'V2,'K>( comp, func )
        newDSet        

    /// Inner join the two DKVs by hash join.
    /// It assumes the 'probeDKV' is small and can completely fit into the memory of of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member private innerJoinByHash (joinFunc : 'V * 'V1 -> 'V2) (probeDKV : DSet<'K*'V>) (buildDKV : DSet<'K*'V1>) =
        raise (System.NotImplementedException())
        // let probeDKV = x.Multicast() 
        // let inMemProbDKV = probeDKV.CacheInRAM()
        // For each element in buildDKV, search against inMemProbDKV using the key to perform the join

    /// Left join the two DKVs by hash join.
    /// It assumes the 'probeDKV' is small and can completely fit into the memory of of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member private leftOuterJoinByHash (joinFunc : 'V * 'V1 option -> 'V2) (probeDKV : DSet<'K*'V>) (buildDKV : DSet<'K*'V1>) =
        raise (System.NotImplementedException())
        // let probeDKV = x.Multicast() 
        // let inMemProbDKV = probeDKV.CacheInRAM()
        // For each element in buildDKV, search against inMemProbDKV using the key to perform the join        

    /// Right join the two DKVs by hash join.
    /// It assumes the 'probeDKV' is small and can completely fit into the memory of of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    static member private rightOuterJoinByHash (joinFunc : 'V option * 'V1 -> 'V2) (probeDKV : DSet<'K*'V>) (buildDKV : DSet<'K*'V1>) = 
        raise (System.NotImplementedException())
        // let probeDKV = x.Multicast() 
        // let inMemProbDKV = probeDKV.CacheInRAM()
        // For each element in buildDKV, search against inMemProbDKV using the key to perform the join

/// Extension member functions for DSet<'K*'V>
[<Extension>]
type DKVExtensionsToDSet =
    // Extension member functions
    // * Use tuple form
    // * First parameter must be of type DSet<'K*'V>

    /// Apply a partition function, repartition elements by the key across nodes in the cluster according to the setting specified by "param".
    [<Extension>]
    static member RepartitionPByKey(x:DSet<'K*'V>, param:DParam, partFunc:'K -> int ) =
        x |> DKV.repartitionPByKey param partFunc

    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    [<Extension>]
    static member RepartitionNByKey(x:DSet<'K*'V>,  numPartitions, partFunc:'K -> int) =
        x |> DKV.repartitionNByKey numPartitions partFunc

    /// Apply a partition function, repartition elements across nodes in the cluster. The number of partitions remains unchanged.
    [<Extension>]
    static member RepartitionByKey(x:DSet<'K*'V>, partFunc:'K -> int ) =
        x |> DKV.repartitionByKey partFunc

//    [<Extesnion>]
//    static member RepartitionByUniqueKey(x:DSet<'K*'V>, partFunc: 'K -> 'K1 ) =
//        DKV.repartitionByUniqueKey partFunc x

    /// <summary> Group all values of the same key to a List. 
    /// </summary> 
    /// <param name="numSerialization"> The collection size used before sending over network to improve efficiency </param>
    [<Extension>]
    static member GroupByKeyN(x:DSet<'K*'V>, numSerialization) =
        x |> DKV.groupByKeyN numSerialization

    /// Group all values of the same key to a List. 
    [<Extension>]
    static member GroupByKey(x:DSet<'K*'V>) =
        x |> DKV.groupByKey

    /// <summary>
    /// Aggregate all values of a unique key of the DKV togeter. Caution: as this function uses mapreduce, the network cost is not negligble. If the aggregated result is to be returned to the client, 
    /// rather than further used in the cluster, the .fold function should be considered instead for performance. 
    /// </summary>
    /// <param name="reduceFunc"> Reduce Function, see Seq.reduce for examples </param>
    [<Extension>]
    static member ReduceByKey(x:DSet<'K*'V>, reduceFunc) = 
        x |> DKV.reduceByKey reduceFunc

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary>
    [<Extension>] 
    static member BinSortPByKey (x:DSet<'K*'V>, param:DParam, partFunc:'K->int, comparer:IComparer<'K>) =
        x |> DKV.binSortPByKey param partFunc comparer

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    [<Extension>] 
    static member BinSortNByKey (x:DSet<'K*'V>, numPartitions, partFunc, comparer) =
        x |> DKV.binSortNByKey numPartitions partFunc comparer

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    [<Extension>] 
    static member BinSortByKey (x:DSet<'K*'V>, partFunc, comparer) =
        x |> DKV.binSortByKey partFunc comparer

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate on key returns true.
    [<Extension>] 
    static member FilterByKey (x:DSet<'K*'V>, func: 'K -> bool ) = 
        x |> DKV.filterByKey func

    /// Create a new dataset by transforming only the value of the original dataset
    [<Extension>] 
    static member MapByValue (x:DSet<'K*'V>, func: 'V -> 'V1 ) = 
        x |> DKV.mapByValue func

    /// Map DKV by value, in which func is an async function so that all data in a serialization block will be executed with Async.Parallel 
    [<Extension>] 
    static member AsyncMapByValue (x:DSet<'K*'V>,  func: 'V -> Async<'V1> ) = 
        x |> DKV.asyncMapByValue func

    /// <summary>
    /// Map DKV by value, in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    [<Extension>] 
    static member ParallelMapByValue (x:DSet<'K*'V>,  func: 'V -> Task<'V1> ) = 
        x |> DKV.parallelMapByValue func

    /// Inner join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member InnerJoinByMergeAfterBinSortByKey (x:DSet<'K*'V>, x1:DSet<'K*'V1>, comp, func) = 
        DKV.innerJoinByMergeAfterBinSortByKey comp func x x1

    /// Left outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member LeftOuterJoinByMergeAfterBinSortByKey (x:DSet<'K*'V>, x1:DSet<'K*'V1>, comp, func) = 
        DKV.leftOuterJoinByMergeAfterBinSortByKey comp func x x1

    /// Right outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member RightOuterJoinByMergeAfterBinSortByKey (x:DSet<'K*'V>, x1:DSet<'K*'V1>, comp, func) = 
        DKV.rightOuterJoinByMergeAfterBinSortByKey comp func x x1

    /// Inner join the DKV with another DKV by hash join.
    /// It assumes this DKV is small and can completely fit into the memory of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member private InnerJoinByHash (probeDKV : DSet<'K*'V>, buildDKV : DSet<'K*'V1>, joinFunc : 'V * 'V1 -> 'V2) : DSet<'K*'V2> =
        raise (System.NotImplementedException())

    /// Left join the DKV with another DKV by hash join.
    /// It assumes this DKV is small and can completely fit into the memory of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member private LeftOuterJoinByHash (probeDKV : DSet<'K*'V>, buildDKV : DSet<'K*'V1>, joinFunc : 'V * 'V1 option -> 'V2) : DSet<'K*'V2> =
        raise (System.NotImplementedException())

    /// Right join the DKV with another DKV by hash join.
    /// It assumes this DKV is small and can completely fit into the memory of each single node of the cluster
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    member private x.RightOuterJoinByHash (probeDKV : DSet<'K*'V>, buildDKV : DSet<'K*'V1>, joinFunc : 'V option * 'V1 -> 'V2) : DSet<'K*'V2> =
        raise (System.NotImplementedException())
