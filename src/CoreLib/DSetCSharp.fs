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
        DSetCSharp.fs
  
    Description: 
        CSharp APIs for DSet<'U>

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Api.CSharp

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Tasks

open Prajna.Tools

/// Generic distributed dataset. If storing the content of DSet to a persisted store, it is highly advisable that 'U used is either of .Net system type, 
/// or using a customized serializer/deserializer to serialize ('U)[]. Otherwise, the stored data may not be retrievable as the BinaryFormatter may fail 
/// to deserialize a class if the associated code has changed.
type DSet<'U> () =

    internal new (o) as x = 
        new DSet<'U>()
        then
            x.DSet <- o

    member val internal DSet = Prajna.Api.FSharp.DSet<'U>() with get, set

    /// Get and Set Cluster 
    member x.Cluster with get() = x.DSet.Cluster
                     and  set(value) =  x.DSet.Cluster <- value

    /// Is the partition of DSet formed by a key function that maps a data item to an unique partition
    member x.IsPartitionByKey with get() = x.DSet.IsPartitionByKey
                              and  set(value) =  x.DSet.IsPartitionByKey <- value

    /// Name of the DSet
    member x.Name with get() = x.DSet.Name
                  and  set(value) =  x.DSet.Name <- value

    /// Number of partitions  
    member x.NumPartitions with get() = x.DSet.NumPartitions
                           and  set(value) =  x.DSet.NumPartitions <- value

    /// Required number of replications for durability
    member x.NumReplications with get() = x.DSet.NumReplications
                             and  set(value) =  x.DSet.NumReplications <- value

    /// Password that will be hashed and used for triple DES encryption and decryption of data. 
    member x.Password with get() = x.DSet.Password
                      and  set(value) =  x.DSet.Password <- value

    /// Flow control, limits the total bytes send out to PeerRcvdSpeedLimit
    /// If it is communicating with N peer, each peer, the sending queue limit is PeerRcvdSpeedLimit/N
    member x.PeerRcvdSpeedLimit with get() = x.DSet.PeerRcvdSpeedLimit
                                and  set(value) =  x.DSet.PeerRcvdSpeedLimit <- value
    
    /// In BinSort/MapReduce, indicate the collection size after the a collection of data is received from network  
    member x.PostGroupByReserialization with get() = x.DSet.PostGroupByReserialization
                                        and  set(value) =  x.DSet.PostGroupByReserialization <- value

    /// In BinSort/MapReduce, indicate whether need to regroup collection before sending a collection of data across network 
    member x.PreGroupByReserialization with get() = x.DSet.PreGroupByReserialization
                                       and  set(value) =  x.DSet.PreGroupByReserialization <- value

    /// Sender flow control, DSet/DStream limits the total sending queue to SendingQueueLimit
    /// If it is communicating with N peer, each peer, the sending queue limit is SendingQueueLimit/N
    member x.SendingQueueLimit with get() = x.DSet.SendingQueueLimit
                               and  set(value) =  x.DSet.SendingQueueLimit <- value

    /// Number of record in a collection during data analytical jobs. 
    /// This parameter will not change number of record in an existing collection of a DSet. To change the 
    /// number of record of an existing collection, please use RowsReorg(). 
    member x.SerializationLimit with get() = x.DSet.SerializationLimit
                                and  set(value) =  x.DSet.SerializationLimit <- value
    
    /// Storage Type, which include StorageMedia and IndexMethod
    member x.StorageType with get() = x.DSet.StorageType
                         and  set(value) =  x.DSet.StorageType <- value

    /// Get or Set Load Balancer
    /// Note that the change will affect Partitioner
    member x.TypeOfLoadBalancer with get() = x.DSet.TypeOfLoadBalancer
                                and  set(value) =  x.DSet.TypeOfLoadBalancer <- value
    
    /// Get and Set version 
    member x.Version with get() = x.DSet.Version
                     and  set(value) =  x.DSet.Version <- value

    /// Represent version in a string for display 
    member x.VersionString with get() = x.DSet.VersionString

    /// Set a content key for DSet that governs partition mapping, 
    /// For two DSets that have the same content key, a single key will be mapped uniquely to a partition
    member x.ContentKey with get() = x.DSet.ContentKey
                        and  set(value) =  x.DSet.ContentKey <- value

    /// Maximum number of parallel threads that will execute the data analytic jobs in a remote container. 
    /// If 0, the remote container will determine the number of parallel threads used according to its computation and memory resource
    /// available. 
    member x.NumParallelExecution with get() = x.DSet.NumParallelExecution
                                  and  set(value) =  x.DSet.NumParallelExecution <- value

    /// Get the size of all key-values or blobs in DSet
    member x.SizeInBytes with get() = x.DSet.SizeInBytes
    
    // ============================================================================================================
    // Store a sequence to a persisted DSet
    // ============================================================================================================

    /// Store an IEnumerable to a persisted DSet
    member x.Store (o : IEnumerable<'U>) = 
        x.DSet.Store(o)

    // ============================================================================================================
    // Load persisted DSet
    // ============================================================================================================

    /// <summary>
    /// Trigger to load metadata. 
    /// </summary>
    /// <returns> Dataset with loaded metadata. </returns>
    member x.LoadSource() = 
        DSet<_>(x.DSet.LoadSource())

    // ============================================================================================================
    // Actions for DSet
    // ============================================================================================================

    /// Convert DSet to an IEnumerable<'U>
    member x.ToIEnumerable() = 
        x.DSet.ToSeq()

    /// <summary>
    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is broadcasted to each node, 
    /// and shared across partition within the node.  
    /// Within each partition, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    /// </summary>
    /// <param name="folder"> update the state given the input elements </param>
    /// <param name="aggrFunc"> aggregate the state from different partitions to a single state variable.</param>
    /// <param name="state"> initial state for each partition </param>
    member private x.FoldWithCommonStatePerNode (folder : Func<'State, 'U, 'State>, aggrFunc : Func<'State, 'State, 'State>, state : 'State) = 
        x.DSet.FoldWithCommonStatePerNode(folder.ToFSharpFunc(), aggrFunc.ToFSharpFunc(), state)

    /// <summary>
    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is deserialized (separately) for each partition. 
    /// Within each partition, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    /// </summary>
    /// <param name="folder"> update the state given the input elements </param>
    /// <param name="aggrFunc"> aggregate the state from different partitions to a single state variable.</param>
    /// <param name="state"> initial state for each partition </param>
    member x.Fold (folder : Func<'State, 'U, 'State>, aggrFunc : Func<'State, 'State, 'State>, state : 'State) = 
        x.DSet.Fold(folder.ToFSharpFunc(), aggrFunc.ToFSharpFunc(), state)

    /// <summary>
    /// Reduces the elements using the specified 'reducer' function
    /// </summary>
    member x.Reduce (reducer : Func<'U, 'U, 'U>) = 
        x.DSet.Reduce(reducer.ToFSharpFunc())

    /// Applies a function 'iterFunc' to each element
    member x.Iter (iterFunc : Action<'U> ) = 
        x.DSet.Iter (iterFunc.ToFSharpFunc())

    /// Count the number of elements(rows) in the DSet 
    member x.Count() = 
        x.DSet.Count()

    /// Read DSet back to local machine and apply a function on each value. Caution: the iter function will be executed locally as the entire DSet is read back. 
    member x.LocalIter (iterFunc : Action<'U>) = 
        x.DSet.LocalIter (iterFunc.ToFSharpFunc())

   /// Save the DSet to the storage specifed by storageKind with name "name". This is an action.
    /// Note: make it public when we support more than one storage kind
    member private x.Save(storageKind, name) =
        raise (NotImplementedException())

    /// Save the DSet to HDD using "name". This is an action.
    member x.SaveToHDDByName name =
        x.DSet.SaveToHDDByName name

    /// Save the DSet to HDD. This is an action.
    member x.SaveToHDD () =
        x.DSet.SaveToHDD()

    /// Lazily save the DSet to the storage specifed by storageKind with name "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    /// Note: make it public when we support more than one storage kind
    member private x.LazySave(storageKind, name) =
        raise (NotImplementedException())

    /// Lazily save the DSet to HDD using "name". 
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    member x.LazySaveToHDDByName name =
        x.DSet.LazySaveToHDDByName name

    /// Lazily save the DSet to HDD.
    /// This is a lazy evaluation. This DSet must be a branch generated by Bypass or a child of such branch.
    /// The save action will be triggered when a pull happens on other branches generated by Bypass.
    member x.LazySaveToHDD () =
        x.DSet.LazySaveToHDD()

    /// Save the DSet to the storage specifed by storageKind with name "name". 
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    /// Note: make it public when we support more than one storage kind
    member private x.SaveWithMonitor(monitorFunc : Func<'U, bool>, localIterFunc:  Action<'U>, storageKind, name) =
        raise (NotImplementedException())

    /// Save the DSet to HDD using "name". This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    member x.SaveToHDDByNameWithMonitor(monitorFunc : Func<'U, bool>, localIterFunc:  Action<'U>, name) =
        x.DSet.SaveToHDDByNameWithMonitor(monitorFunc.ToFSharpFunc(), localIterFunc.ToFSharpFunc(), name)

    /// Save the DSet to HDD. This is an action.
    /// Attach a monitor function that select elements that will be fetched to local machine to be iterated by 'localIterFunc'
    member x.SaveToHDDWithMonitor(monitorFunc : Func<'U, bool>, localIterFunc:  Action<'U>) =
        x.DSet.SaveToHDDWithMonitor(monitorFunc.ToFSharpFunc(), localIterFunc.ToFSharpFunc())

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
    member x.Init(initFunc : Func<int*int, 'U>, partitionSizeFunc : Func<int, int>) =
        DSet<_>(x.DSet.Init(initFunc.ToFSharpFunc(), partitionSizeFunc.ToFSharpFunc()))

    /// <summary>
    /// Create a distributed dataset on the distributed cluster, with each element created by a functional delegate.
    /// </summary> 
    /// <param name="initFunc"> The functional delegate that create each element in the dataset, the integer index passed to the function 
    /// indicates the partition, and the second integer passed to the function index (from 0) element within the partition. 
    /// </param> 
    /// <param name="partitionSize"> Size of each partition </param> 
    member x.InitS(initFunc : Func<int*int, 'U>, partitionSize) =
        DSet<_>(x.DSet.InitS(initFunc.ToFSharpFunc(), partitionSize))

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
    member x.InitN(initFunc : Func<int*int, 'U>, partitionSizeFunc : Func<int, int, int>) =
        DSet<_>(x.DSet.InitN(initFunc.ToFSharpFunc(), partitionSizeFunc.ToFSharpFunc()))

    /// <summary> 
    /// Generate a distributed dataset through a customerized seq functional delegate running on each of the machine. 
    /// Each node runs a local instance of the functional delegate, which generates a seq('U) that forms one partition of DSet('U).
    /// The number of partitions of DSet('U) is N, which is the number of the nodes in the cluster. The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegate in that node. </summary>
    /// <param name="sourceSeqFun"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated DSet('U) </return>
    member x.Source (sourceSeqFunc : Func<IEnumerable<'U>>) =
        DSet<_>(x.DSet.Source(sourceSeqFunc.ToFSharpFunc()))

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// Each node runs num local instance of the functional delegates, each of which forms one partition of the dataset.
    /// The number of partitions of dataset is N * num, where N is the number of the nodes in the cluster, and num is the number of partitions per node. 
    /// The NumReplications is 1. 
    /// If any node in the cluster is not responding, the dataset does not contain the data resultant from the functional delegates in that node. </summary>
    /// <param name="sourceNSeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set. </param>
    /// <remarks> See code example DistributedLogAnalysis </remarks>
    /// <return> generated dataset </return>
    member x.SourceN(num, sourceNSeqFunc : Func<int, IEnumerable<'U>>) = 
        DSet<_>(x.DSet.SourceN(num, sourceNSeqFunc.ToFSharpFunc()))

    /// <summary> 
    /// Generate a distributed dataset through customerized seq functional delegates running on each of the machine. 
    /// The number of partitions of dataset is numPartitions. 
    /// If any node in the cluster is not responding, the partition may be rerun at a different node. </summary>
    /// <param name="sourceISeqFunc"> Customerized functional delegate that runs on each node, it generates one partition of the distributed data set, the input parameter is the index (0 based)of the partition.  </param>
    /// <return> generated dataset </return>
    member x.SourceI(numPartitions, sourceISeqFunc : Func<int, IEnumerable<'U>>) = 
        DSet<_>(x.DSet.SourceI(numPartitions, sourceISeqFunc.ToFSharpFunc()))

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
        DSet<_>(x.DSet.Import(serversInfo, importName))

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
    member x.ImportN(serversInfo, importNames) = 
        DSet<_>(x.DSet.ImportN(serversInfo, importNames))

    /// <summary> 
    /// Span an IEnumerable (dataset) to a distributed dataset by splitting the sequence to N partitions, with N being the number of nodes in the cluster. 
    /// Each node get one partition of the dataset. The number of partitions of the distributed dataset is N, which is the number of the active nodes in the cluster. 
    /// </summary>
    /// <param name="sourceEnumerable"> Source dataset </param>
    /// <return> Generated dataset. </return>
    member x.Distribute (sourceEnumerable : IEnumerable<'U>) = 
        DSet<_>(x.DSet.Distribute(sourceEnumerable))

    /// <summary> 
    /// Span an IEnumerable (dataset) to a distributed dataset by splitting the sequence to NUM * N partitions, with NUM being the number of partitions on each node, 
    /// and N being the number of nodes in the cluster. Each node get NUM partition of the dataset. 
    /// </summary>
    /// <param name="num"> Number of partitions in each node. </param>
    /// <param name="sourceEnumerable"> Source dataset. </param>
    /// <remarks> See code example DistributedKMeans. </remarks>
    /// <return> Generated dataset. </return>
    member x.DistributeN(num, sourceEnumerable  : IEnumerable<'U>) = 
        DSet<_>(x.DSet.DistributeN(num, sourceEnumerable))

    // ============================================================================================================
    // Execute a delegate remotely
    // ============================================================================================================

    /// <summary> 
    /// Execute a distributed customerized functional delegate on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegate does not run on that node. </summary>
    /// <param name="func"> The customerized functional delegate that runs on each remote container. </param>
    member x.Execute (func : Action) = 
        x.DSet.Execute(func.ToFSharpFunc())

    /// <summary> 
    /// Execute num distributed customerized functional delegates on each of the machine. 
    /// The functional delegate is running remotely on each node. If any node in the cluster is not responding, the functional delegates do not run on that node. </summary>
    /// <param name="num"> The number of instances run on each of the node. </param>
    /// <param name="funcN"> The customerized functional delegate that runs on each remote container, the input integer index (from 0) the instance of the functional delegate runs. </param>
    member x.ExecuteN(num, funcN : Action<int>) = 
        x.DSet.ExecuteN(num, funcN.ToFSharpFunc())

    // ============================================================================================================
    // Transformations for DSet
    // ============================================================================================================

    /// Identity Mapping, the new DSet is the same as the old DSet, with an encode function installed.
    member x.Identity( ) = 
        DSet<_>(x.DSet.Identity())

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate returns true.
    member x.Filter ( func : Func<'U, bool>) =
        DSet<_>(x.DSet.Filter(func.ToFSharpFunc()))

    /// Applies the given function to each element of the dataset. 
    /// The new dataset comprised of the results for each element where the function returns some value or null.
    member x.Choose ( func : Func<'U, 'U1>) =
        DSet<_>(x.DSet.Choose(fun u ->
            match (func.Invoke(u)) with
            | null -> None
            | u1 -> u1 |> Some
        ))

    /// Applies the given function to each element of the dataset. 
    /// The new dataset comprised of the results for each element where the function returns some value or null.
    member x.Choose ( func : Func<'U, Nullable<'U1>>) =
        DSet<_>(x.DSet.Choose(fun u ->
            match (func.Invoke(u)) with
            | u1 when u1.HasValue -> u1.Value |> Some
            | _ -> None
        ))

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The given function will be applied per collection as the dataset is being distributedly iterated. 
    /// The entire dataset never materialize entirely. 
    member x.Map ( func : Func<'U, 'U1> ) = 
        DSet<_>(x.DSet.Map(func.ToFSharpFunc()))

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition
    member x.Mapi ( func : Func<int, int64, 'U, 'U1>) = 
        DSet<_>(x.DSet.Mapi(func.ToFSharpFunc()))

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    member x.ParallelMap ( func : Func<'U, Task<'U1>> ) = 
        DSet<_>(x.DSet.ParallelMap(func.ToFSharpFunc()))

    /// <summary>
    /// Map DSet , in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    member x.ParallelMapi ( func : Func<int, int64, 'U, Task<'U1>> ) = 
        DSet<_>(x.DSet.ParallelMapi(func.ToFSharpFunc()))

    /// Creates a new dataset whose elements are the results of applying the given function to each collection of the elements 
    /// of the dataset. In the input DSet, a parttition can have multiple collections, 
    /// the size of the which is determined by the SerializationLimit of the cluster.
    member x.MapByCollection ( func : Func<'U[], 'U1[]> ) = 
        DSet<_>(x.DSet.MapByCollection(func.ToFSharpFunc()))

    /// Reorganization collection in a dataset to accommodate a certain parallel execution degree. 
    member x.ReorgWDegree numParallelJobs = 
        DSet<_>(x.DSet.ReorgWDegree numParallelJobs)

    /// Reorganization collection of a dataset. 
    /// If numRows = 1, the dataset is split into one row per collection. 
    /// If numRows = Int32.MaxValue, the data set is merged so that all rows in a partition is grouped into a single collection. 
    member x.RowsReorg numRows = 
        DSet<_>(x.DSet.RowsReorg numRows)

    /// Reorganization collection of a dataset so that each collection has a single row. 
    member x.RowsSplit() = 
        DSet<_>(x.DSet.RowsSplit())

    /// merge all rows of a partition into a single collection object
    member x.RowsMergeAll() = 
        DSet<_>(x.DSet.RowsMergeAll())

    /// Cache DSet content in memory, in raw object form. 
    member x.CacheInMemory() = 
        DSet<_>(x.DSet.CacheInMemory())

    /// Applies the given function to each element of the dataset and concatenates all the results. 
    member x.Collect ( func : Func<'U, IEnumerable<'U1>>) = 
        DSet<_>(x.DSet.Collect(func.ToFSharpFunc()))

    /// Multicast a DSet over network, replicate its content to all peers in the cluster. 
    member x.Multicast()  = 
        DSet<_>(x.DSet.Multicast())

   /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    member x.RepartitionP (param, partFunc: Func<'U, int> ) = 
        DSet<_>(x.DSet.RepartitionP(param, partFunc.ToFSharpFunc()))

    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    member x.RepartitionN(numPartitions, partFunc : Func<'U, int> ) = 
        DSet<_>(x.DSet.RepartitionN(numPartitions, partFunc.ToFSharpFunc()))

    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    member x.Repartition (partFunc : Func<'U, int> ) = 
        DSet<_>(x.DSet.Repartition(partFunc.ToFSharpFunc()))

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="partFunc"> After map, used for user defined repartition </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param> 
    member x.MapReducePWithPartitionFunction (param, mapFunc : Func<'U, IEnumerable<'K1*'V1>>, partFunc : Func<'K1, int>, reduceFunc : Func<'K1*System.Collections.Generic.List<'V1>, 'U2> ) =
        DSet<_>(x.DSet.MapReducePWithPartitionFunction(param, mapFunc.ToFSharpFunc(), partFunc.ToFSharpFunc(), reduceFunc.ToFSharpFunc()))

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="param"> Controls the settings for the map reduce </param>
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param>  
    member x.MapReduceP (param, mapFunc : Func<'U, IEnumerable<'K1*'V1>>, reduceFunc : Func<'K1*System.Collections.Generic.List<'V1>, 'U2> ) =
        DSet<_>(x.DSet.MapReduceP(param, mapFunc.ToFSharpFunc(), reduceFunc.ToFSharpFunc()))

    /// <summary> MapReduce, see http://en.wikipedia.org/wiki/MapReduce </summary> 
    /// <param name="mapFunc"> Mapping functional delegate that performs filtering and sorting </param>
    /// <param name="reduceFunc"> Reduce functional delegate that performs a summary operation </param>  
    member x.MapReduce (mapFunc : Func<'U, IEnumerable<'K1*'V1>>, reduceFunc : Func<'K1*System.Collections.Generic.List<'V1>, 'U2> ) =
        DSet<_>(x.DSet.MapReduce(mapFunc.ToFSharpFunc(), reduceFunc.ToFSharpFunc()))

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSortP (param, partFunc : Func<'U, int>, comparer) =
        DSet<_>(x.DSet.BinSortP(param, partFunc.ToFSharpFunc(), comparer))

    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSortN(numPartitions, partFunc : Func<'U, int>, comparer) =
        DSet<_>(x.DSet.BinSortN(numPartitions, partFunc.ToFSharpFunc(), comparer))
        
    /// <summary> 
    /// Bin sort the DSet.
    /// Apply a partition function, repartition elements of dataset across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    member x.BinSort(partFunc : Func<'U, int>, comparer) =
        DSet<_>(x.DSet.BinSort(partFunc.ToFSharpFunc(), comparer))

    /// <summary> 
    /// Merge the content of multiple dataset into a single dataset. The original dataset become partitions of the resultant dataset. 
    /// This can be considered as merge dataset by rows, and all dataset have the same column structure. </summary>
    /// <param name="source"> Source dataset </param>
    /// <return> Merged dataset </return>
    member x.Merge (source : IEnumerable<DSet<'U>>) = 
        DSet<_>(x.DSet.Merge(source |> Seq.map(fun s -> s.DSet)))

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the two DSets pairwise
    /// The two DSet must have the same partition mapping structure and same number of element (e.g.), established via Split.
    member x.Map2 (func : Func<'U, 'U1, 'V>, x1 : DSet<'U1>) = 
        DSet<_>(x.DSet.Map2(func.ToFSharpFunc(), x1.DSet))

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the three DSets pairwise
    member x.Map3 (func: Func<'U, 'U1, 'U2, 'V>, x1 : DSet<'U1>, x2 : DSet<'U2>) = 
        DSet<_>(x.DSet.Map3(func.ToFSharpFunc(), x1.DSet, x2.DSet))

    /// Create a new DSet whose elements are the results of applying the given function to the corresponding elements of the four DSets pairwise
    member x.Map4 (func: Func<'U, 'U1, 'U2, 'U3, 'V>, x1 : DSet<'U1>, x2 : DSet<'U2>, x3 : DSet<'U3>) =
        DSet<_>(x.DSet.Map4(func.ToFSharpFunc(), x1.DSet, x2.DSet, x3.DSet))

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix (x1 : DSet<'U1>) = 
        DSet<_>(x.DSet.Mix(x1.DSet))

    /// <summary> 
    /// Mixing two DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix2 (x1 : DSet<'U1>) = 
        DSet<_>(x.DSet.Mix2(x1.DSet))

    /// <summary> 
    /// Mixing three DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix3 (x1 : DSet<'U1>, x2 : DSet<'U2>) = 
        DSet<_>(x.DSet.Mix3(x1.DSet, x2.DSet))

    /// <summary> 
    /// Mixing four DSets that have the same size and partition layout into a single DSet by operating a function on the individual data (row).
    /// </summary>
    member x.Mix4(x1 : DSet<'U1>, x2 : DSet<'U2>, x3 : DSet<'U3>) = 
        DSet<_>(x.DSet.Mix4(x1.DSet, x2.DSet, x3.DSet))

    /// <summary> 
    /// Cross Join, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc:'U->'U1->'U2
    /// The resultant set is the product of Dset&lt;'U> and Dset&lt;'U1>
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoin (mapFunc : Func<'U, 'U1, 'U2>, x1 : DSet<'U1>) =
        DSet<_>(x.DSet.CrossJoin(mapFunc.ToFSharpFunc(), x1.DSet))

    /// <summary> 
    /// Cross Join with Filtering, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc
    /// Filter out all those result that returns null. 
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoinChoose (mapFunc : Func<'U, 'U1, 'U2>, x1 : DSet<'U1>) =
        DSet<_>(x.DSet.CrossJoinChoose( (fun u u1 ->
            match (mapFunc.Invoke(u, u1)) with
            | null -> None
            | u2 -> u2 |> Some
        ), x1.DSet))

    /// <summary> 
    /// Cross Join with Filtering, for each entry in x:Dset&lt;'U> and x1:Dset&lt;'U1> apply mapFunc. 
    /// Filter out all those result that returns null. 
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:Dset&lt;'U> and each entry in x1:Dset&lt;'U1>  </param>
    /// <param name="x1"> The Dset&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoinChoose (mapFunc : Func<'U, 'U1, Nullable<'U2>>, x1 : DSet<'U1>) =
        DSet<_>(x.DSet.CrossJoinChoose( (fun u u1 ->
            match (mapFunc.Invoke(u, u1)) with
            | u2 when u2.HasValue -> u2.Value |> Some
            | _ -> None
        ), x1.DSet))

    /// <summary> 
    /// Cross Join with Fold, for each entry in x:DSet&lt;'U> and x1:DSet&lt;'U1> apply mapFunc:'U->'U1->'U2, and foldFunc: 'S->'U2->'S
    /// </summary>
    /// <param name="mapFunc"> The map function that is executed for the product of each entry in x:DSet&lt;'U> and each entry in x1:DSet&lt;'U1>  </param>
    /// <param name="foldFunc"> The fold function that is executed for each of the map result </param>
    /// <param name="x1"> The DSet&lt;'U1>. For performance purpose, it is advisable to assign the smaller DSet as x1. </param>
    /// <return> DSet </return>
    member x.CrossJoinFold (mapFunc : Func<'U, 'U1, 'U2>, foldFunc : Func<'S, 'U2, 'S>, initialState, x1 : DSet<'U1>) =
        DSet<_>(x.DSet.CrossJoinFold(mapFunc.ToFSharpFunc(), foldFunc.ToFSharpFunc(), initialState, x1.DSet))

    // ============================================================================================================
    // Setup push/pull dataflow
    // ============================================================================================================
    
    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    member x.Bypass( ) = 
        let x1, x2 = x.DSet.Bypass()
        DSet<_>(x1), DSet<_>(x2)

    /// Bypass the Dset in 2 ways. One of the DSet is pulled, the other DSet will be in push dataflow. 
    member x.Bypass2() = 
        let x1, x2 = x.DSet.Bypass2()
        DSet<_>(x1), DSet<_>(x2)

    /// Bypass the Dset in 3 ways. One of the DSet is pulled, the other two DSets will be in push dataflow. 
    member x.Bypass3() = 
        let x1, x2, x3 = x.DSet.Bypass3()
        DSet<_>(x1), DSet<_>(x2), DSet<_>(x3)

    /// Bypass the Dset in 4 ways. One of the DSet is pulled, the other three DSets will be in push dataflow. 
    member x.Bypass4() = 
        let x1, x2, x3, x4 = x.DSet.Bypass4()
        DSet<_>(x1), DSet<_>(x2), DSet<_>(x3), DSet<_>(x4)

    /// Bypass the Dset in n ways. One of the DSet is pulled, the other n - 1 DSets will be in push dataflow. 
    member x.BypassN nWays = 
        let x1, xs = x.DSet.BypassN nWays
        DSet<_>(x1), xs |> Array.map(fun y -> DSet<_>(y))

    /// <summary>
    /// Correlated split a dataset to two, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// They can be combined later by Map2 transforms. 
    /// </summary> 
    member x.Split2(fun0 : Func<'U, 'U0>, fun1 : Func<'U, 'U1>) = 
        let a, b = x.DSet.Split2(fun0.ToFSharpFunc(), fun1.ToFSharpFunc())
        DSet<_>(a), DSet<_>(b)

    /// <summary>
    /// Correlated split a dataset to three, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// They can be combined later by Map3 transforms. 
    /// </summary> 
    member x.Split3(fun0 : Func<'U, 'U0>, fun1 : Func<'U, 'U1>, fun2 : Func<'U, 'U2>) = 
        let a, b, c = x.DSet.Split3(fun0.ToFSharpFunc(), fun1.ToFSharpFunc(), fun2.ToFSharpFunc())
        DSet<_>(a), DSet<_>(b), DSet<_>(c)

    /// <summary>
    /// Correlated split a dataset to four, each of which is created by running a functional delegate that maps the element of the original dataset.
    /// The resultant datasets all have the same partition and collection structure of the original dataset. 
    /// They can be combined later by Map4 transforms. 
    /// </summary> 
    member x.Split4(fun0 : Func<'U, 'U0>, fun1 : Func<'U, 'U1>, fun2 : Func<'U, 'U2>, fun3 : Func<'U, 'U3>) = 
        let a, b, c, d = x.DSet.Split4(fun0.ToFSharpFunc(), fun1.ToFSharpFunc(), fun2.ToFSharpFunc(), fun3.ToFSharpFunc())
        DSet<_>(a), DSet<_>(b), DSet<_>(c), DSet<_>(d)

    // ============================================================================================================
    // Try to find a DSet
    // ============================================================================================================
    
    static member TryFind (cluster : Prajna.Core.Cluster, searchPattern : string) =
        (Prajna.Api.FSharp.DSet.tryFind cluster searchPattern)
        |> Array.map(fun d -> DSet<_>(d))

type private FDKV<'K,'V  when 'K : equality> = Prajna.Api.FSharp.DKV<'K,'V>

/// Extension member functions for DSet<Tuple<'K,'V>>
[<Extension>]
type DKVMemberExtensionsToDSet =
    
    /// Apply a partition function, repartition elements by the key across nodes in the cluster according to the setting specified by "param".
    [<Extension>]
    static member RepartitionPByKey(x : DSet<'K*'V>, param : Prajna.Core.DParam, partFunc : Func<'K, int> ) =
        DSet<_>(x.DSet |> FDKV.repartitionPByKey param (partFunc.ToFSharpFunc()))


    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    [<Extension>]
    static member RepartitionNByKey(x : DSet<'K*'V>,  numPartitions, partFunc: Func<'K, int>) =
        DSet<_>(x.DSet |> FDKV.repartitionNByKey numPartitions (partFunc.ToFSharpFunc()))

    /// Apply a partition function, repartition elements across nodes in the cluster. The number of partitions remains unchanged.
    [<Extension>]
    static member RepartitionByKey(x : DSet<'K*'V>, partFunc : Func<'K, int> ) =
        DSet<_>(x.DSet |> FDKV.repartitionByKey (partFunc.ToFSharpFunc()))

    /// <summary> Group all values of the same key to a List. 
    /// </summary> 
    /// <param name="numSerialization"> The collection size used before sending over network to improve efficiency </param>
    [<Extension>]
    static member GroupByKeyN(x : DSet<'K*'V>, numSerialization) =
        DSet<_>(x.DSet |> FDKV.groupByKeyN numSerialization)

    /// Group all values of the same key to a List. 
    [<Extension>]
    static member GroupByKey(x : DSet<'K*'V>) =
        DSet<_>(x.DSet |> FDKV.groupByKey)

    /// <summary>
    /// Aggregate all values of a unique key of the DKV togeter. Caution: as this function uses mapreduce, the network cost is not negligble. If the aggregated result is to be returned to the client, 
    /// rather than further used in the cluster, the .fold function should be considered instead for performance. 
    /// </summary>
    /// <param name="reduceFunc"> Reduce Function, see Seq.reduce for examples </param>
    [<Extension>]
    static member ReduceByKey(x : DSet<'K*'V>, reduceFunc : Func<'V, 'V, 'V>) = 
        DSet<_>(x.DSet |> FDKV.reduceByKey (reduceFunc.ToFSharpFunc()))

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster according to the setting specified by "param".
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary>
    [<Extension>] 
    static member BinSortPByKey (x : DSet<'K*'V>, param : Prajna.Core.DParam, partFunc : Func<'K, int>, comparer) =
        DSet<_>(x.DSet |> FDKV.binSortPByKey param (partFunc.ToFSharpFunc()) comparer)

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key into 'numPartitions" partitions across nodes in the cluster.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    [<Extension>] 
    static member BinSortNByKey (x : DSet<'K*'V>, numPartitions, partFunc : Func<'K, int>, comparer) =
        DSet<_>(x.DSet |> FDKV.binSortNByKey numPartitions (partFunc.ToFSharpFunc()) comparer)

    /// <summary> 
    /// Bin sort the DKV by key.
    /// Apply a partition function, repartition elements by key across nodes in the cluster. The number of partitions remains unchanged.
    /// Elements within each partition/bin are sorted using the 'comparer'.
    /// </summary> 
    [<Extension>] 
    static member BinSortByKey (x : DSet<'K*'V>, partFunc : Func<'K, int>, comparer) =
        DSet<_>(x.DSet |> FDKV.binSortByKey (partFunc.ToFSharpFunc()) comparer)

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate on key returns true.
    [<Extension>] 
    static member FilterByKey (x : DSet<'K*'V>, func : Func<'K, bool> ) = 
        DSet<_>(x.DSet |> FDKV.filterByKey (func.ToFSharpFunc()))

    /// Create a new dataset by transforming only the value of the original dataset
    [<Extension>] 
    static member MapByValue (x : DSet<'K*'V>, func : Func<'V, 'V1> ) = 
        DSet<_>(x.DSet |> FDKV.mapByValue (func.ToFSharpFunc()))

    /// <summary>
    /// Map DKV by value, in which func is an Task&lt;_> function that may contains asynchronous operation. 
    /// You will need to start the 1st task in the mapping function. Prajna will not be able to start the task for you as the returned
    /// task may not be the a Task in the creation state. see: http://blogs.msdn.com/b/pfxteam/archive/2012/01/14/10256832.aspx 
    /// </summary>
    [<Extension>] 
    static member ParallelMapByValue (x : DSet<'K*'V>,  func : Func<'V, Task<'V1>> ) = 
        DSet<_>(x.DSet |> FDKV.parallelMapByValue (func.ToFSharpFunc()))

    /// Inner join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member InnerJoinByMergeAfterBinSortByKey (x : DSet<'K*'V>, x1 : DSet<'K*'V1>, comp, func : Func<'V, 'V1, 'V2> ) = 
        DSet<_>(FDKV.innerJoinByMergeAfterBinSortByKey comp (func.ToFSharpFunc()) x.DSet x1.DSet)

    /// Left outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member LeftOuterJoinByMergeAfterBinSortByKey (x : DSet<'K*'V>, x1 : DSet<'K*'V1>, comp, func : Func<'V, 'V1, 'V2>) = 
        let f = 
            fun a b -> let b1 = match b with
                                | Some v -> v
                                | None -> null
                       func.Invoke(a, b1)

        DSet<_>(FDKV.leftOuterJoinByMergeAfterBinSortByKey comp f x.DSet x1.DSet)

    /// Left outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member LeftOuterJoinByMergeAfterBinSortByKey (x : DSet<'K*'V>, x1 : DSet<'K*'V1>, comp, func : Func<'V, Nullable<'V1>, 'V2>) = 
        let f = 
            fun a b -> let b1 = match b with
                                | Some v -> Nullable<'V1>(v)
                                | None -> Nullable<'V1>()
                       func.Invoke(a, b1)

        DSet<_>(FDKV.leftOuterJoinByMergeAfterBinSortByKey comp f x.DSet x1.DSet)

    /// Right outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member RightOuterJoinByMergeAfterBinSortByKey (x : DSet<'K*'V>, x1 : DSet<'K*'V1>, comp, func : Func<'V, 'V1, 'V2>) = 
        let f = 
            fun a b -> let a1 = match a with
                                | Some v -> v
                                | None -> null
                       func.Invoke(a1, b)
        DSet<_>(FDKV.rightOuterJoinByMergeAfterBinSortByKey comp f x.DSet x1.DSet)

    /// Right outer join the DKV with another DKV by merge join at each partition.
    /// It assumes that both DKVs have already been bin sorted by key (using one of the BinSortByKey methods). 
    /// The bin sorts should have partitioned the two DKVs into the same number of paritions. For elements with the 
    /// same key, they should have been placed into the same parition.
    /// Please refer to http://en.wikipedia.org/wiki/Join_(SQL) on the join operators.
    [<Extension>] 
    static member RightOuterJoinByMergeAfterBinSortByKey (x : DSet<'K*'V>, x1 : DSet<'K*'V1>, comp, func : Func<Nullable<'V>, 'V1, 'V2>) = 
        let f = 
            fun a b -> let a1 = match a with
                                | Some v -> Nullable<'V>(v)
                                | None -> Nullable<'V>()
                       func.Invoke(a1, b)
        DSet<_>(FDKV.rightOuterJoinByMergeAfterBinSortByKey comp f x.DSet x1.DSet)
