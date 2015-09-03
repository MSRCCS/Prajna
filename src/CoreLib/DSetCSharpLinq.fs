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
        DSetCSharpLinq.fs
  
    Description: 
        CSharp Linq APIs for DSet<'U>

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Api.CSharp.Linq

open System
open System.Collections.Generic
open System.Runtime.CompilerServices

open Prajna.Api.CSharp

[<Extension>]
type DSetLinqExtensions =

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. 
    [<Extension>]
    static member Select (dset : DSet<'U>, func : Func<'U, 'U1>) =
        dset.Map(func)

    /// Creates a new dataset whose elements are the results of applying the given function to each of the elements 
    /// of the dataset. The first integer index passed to the function indicates the partition, and the second integer
    /// passed to the function index (from 0) element within the partition
    [<Extension>]
    static member Select (dset : DSet<'U>, func : Func<'U, int, int64, 'U1>) =
        dset.Mapi(fun partitionIndex indexInPartition u -> func.Invoke(u, partitionIndex, indexInPartition))

    /// Applies the given function to each element of the dataset and concatenates all the results. 
    [<Extension>]
    static member SelectMany(dset : DSet<'U>, func : Func<'U, IEnumerable<'U1>>) =
        dset.Collect(func)

    /// Creates a new dataset containing only the elements of the dataset for which the given predicate returns true.
    [<Extension>]
    static member Where(dset : DSet<'U>, func : Func<'U, bool>) =
        dset.Filter(func)

    /// Fold the entire DSet with a fold function, an aggregation function, and an initial state. The initial state is deserialized (separately) for each partition. 
    /// Within each partition, the elements are folded into the state variable using 'folder' function. Then 'aggrFunc' is used to aggregate the resulting
    /// state variables from all partitions to a single state.
    [<Extension>]
    static member Aggregate(dset : DSet<'U>, state : 'State, folder : Func<'State, 'U, 'State>, aggrFunc : Func<'State, 'State, 'State>) =
        dset.Fold(folder, aggrFunc, state)

     /// Reduces the elements using the specified 'reducer' function
    [<Extension>]
    static member Aggregate(dset : DSet<'U>, reducer : Func<'U, 'U, 'U>) =
        dset.Reduce(reducer)

    /// Groups the elements of a dataset according to a specified key selector function.
    [<Extension>]
    static member GroupBy(dset : DSet<'U>, keySelector : Func<'U, 'K>) =
        dset.Map(fun u -> (keySelector.Invoke(u), u)).GroupByKey()
        
    /// Groups the elements of a dataset according to a specified key selector function and projects the elements for each group by using a specified function.
    [<Extension>]
    static member GroupBy(dset : DSet<'U>, keySelector : Func<'U, 'K>, elementSelector : Func<'U, 'U1>) =
        DSetLinqExtensions.GroupBy(dset, keySelector).Map(fun (_, grp) -> grp |> Seq.map(fun v -> elementSelector.Invoke(v)))
        
    /// Groups the elements of a dataset according to a specified key selector function and creates a result value from each group and its key.
    [<Extension>]
    static member GroupBy(dset : DSet<'U>, keySelector : Func<'U, 'K>, resultSelector : Func<'K, IEnumerable<'U>, 'U1>) =
        DSetLinqExtensions.GroupBy(dset, keySelector).Map(fun (k, grp) -> resultSelector.Invoke(k, grp))

    /// Groups the elements of a dataset according to a specified key selector function and creates a result value from each group and its key. The elements of each group are projected by using a specified function.
    [<Extension>]
    static member GroupBy(dset : DSet<'U>, keySelector : Func<'U, 'K>, elementSelector : Func<'U, 'U1>, resultSelector : Func<'K, IEnumerable<'U1>, 'U2>) =
        DSetLinqExtensions.GroupBy(dset, keySelector).Map(fun (k, grp) -> k, grp |> Seq.map(fun v -> elementSelector.Invoke(v))).Map(fun (k, grp) -> resultSelector.Invoke(k, grp) )

    /// Returns the dataset as an IEnumerable<'U>
    [<Extension>]
    static member AsEnumerable(dset : DSet<'U>) = 
        dset.ToIEnumerable()
