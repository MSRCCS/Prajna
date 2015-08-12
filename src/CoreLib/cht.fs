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
        cht.fs
  
    Description: 
        Consistent Hash Table

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core
open System
open System.Collections.Generic
open Prajna.Tools

/// Enumeration class of constant of type of Load balancing algorithm supported 
type LoadBalanceAlgorithm = 
    /// RoundRobin
    | RoundRobin = 0 
    /// A second RoundRobin algorithm 
    | RoundRobin2 = 1
    /// Consistent Hash Table 
    | CHT = 2
    /// A second implementation of consistent hash table 
    | CHT2 = 3
    /// Consistent hash ring
    | CHR = 4
    /// Default load balancing algorithm 
    | Default = 1

/// A Galois Field with datatype 'D and primitive Polynomial p and keysize s
type internal GF2wInt( p, s ) = 
    let nPrimPoly = p
    let nFieldSize = 1 <<< s
    let nFieldSize1 = nFieldSize - 1
    let pnLogTbl = Array.zeroCreate<_> nFieldSize
    let pniLogTbl = Array.zeroCreate<_> nFieldSize
    do 
        let mutable b = 1
        for log = 0 to nFieldSize1-1 do 
            pnLogTbl.[b] <- log
            pniLogTbl.[log] <- b
            b <- b <<< 1
            if b>= nFieldSize then 
                b <- b ^^^ nPrimPoly
        pnLogTbl.[0] <- 0
    member val nKeySize = s with get
    member val GetFieldSize = nFieldSize with get
    member val GetFieldMask = nFieldSize1 with get
    member x.Log( v ) = pnLogTbl.[v] 
    member x.Exp( v ) = pniLogTbl.[v]
    member x.Pow( v, w ) = x.Exp( (w * x.Log( v ) )% nFieldSize1 ) 
    member x.Mul( v, w ) = 
        if v=0 || w=0 then 
            0
        else
            let s = ( x.Log( v ) + x.Log( w ) ) % nFieldSize1
            x.Exp( s )
    member x.Div( v, w ) = 
        if v = 0 then 
            0 
        else
            let s = ( nFieldSize1 + x.Log( v ) - x.Log(w) ) % nFieldSize1
            x.Exp( s )

type internal LoadBalanceAlgBase( t )  = 
    member val TypeOf : LoadBalanceAlgorithm = t with get, set

[<AbstractClass>]
type internal LoadBlanceAlg( t ) = 
    inherit LoadBalanceAlgBase( t )
    let mutable ids : uint64[] = null
    let mutable curResource : int[] = null
    member val FastFailureRecovery = false with get, set
    /// NormalizeResource: convert resource to a set of normalized resource (1..n) for resource allocation purpose. 
    /// Mapping:
    /// Any resource <=0 -> 0
    /// Any resource r >0 -> round( r / base_resource) with cap
    /// We want the base to be as large as possible to reduce the number of bins needed in resource reallocation. 
    static member NormalizeResource( resource:int[], base_resource, cap ) = 
        let base2_resource = base_resource/2
        let nResource = resource |> Array.map ( fun n -> if n>0 then Math.Min( (n+base2_resource)/base_resource, cap) else 0 )
        nResource
    /// NormalizeResource: Internal function, convert resource to a set of normalized resource (1..n) for resource allocation purpose. 
    /// Mapping:
    /// We want the base to be as large as possible to reduce the number of bins needed in resource reallocation. 
    static member NormalizeResource (resource:int[]) =
        let sortedResource = Array.sort resource
        // Find 25-percentile resource value
        let mutable idx = ( resource.Length-1 ) / 4
        let base_resource = 
            while ( sortedResource.[idx] <=0 && idx < resource.Length-1 ) do
                idx <- idx+1
            sortedResource.[idx]
        // Use 95 percentile 
        idx <- Math.Max( idx, ( resource.Length-1) * 95 / 100 )
        let cap_resource = sortedResource.[idx]
        let cap = ( cap_resource + base_resource-1) / base_resource 
        LoadBlanceAlg.NormalizeResource( resource, base_resource, cap )
    member x.IDs with get() = ids
                  and set( lst ) = ids <- lst
    member x.Resources with get() = curResource
                        and set(r) = curResource <- LoadBlanceAlg.NormalizeResource( r )
    member x.Set( idlist, resource ) = 
        ids <- idlist
        curResource <- LoadBlanceAlg.NormalizeResource( resource )                        
        assert ( ids.Length = curResource.Length )
    new ( t, idlist, resource ) as x = 
        LoadBlanceAlg(t) 
        then 
            x.Set( idlist, resource ) 
    abstract member GetNumberOfPartitions: Cluster -> int
    abstract member GetPartitionMapping: uint64 * int * int -> int[] []
    default x.GetNumberOfPartitions( cluster ) = 
        let np = x.Resources |> Array.fold ( fun sum x -> sum+x) 0 
        if x.FastFailureRecovery then np*Math.Min(np,30) else np

/// Load balancing by round robin assignment
type internal RoundRobin = 
    inherit LoadBlanceAlg

    new () = { inherit LoadBlanceAlg(LoadBalanceAlgorithm.RoundRobin ) }
    new ( ids, resource ) = 
        { inherit LoadBlanceAlg( LoadBalanceAlgorithm.RoundRobin, ids, resource ) }
    override x.GetPartitionMapping( contentKey, numPartitions, numReplications ) =
        let isPrime x = 
            if x <= 3 then 
                true
            elif x % 2 = 0 then 
                false
            else
                let mutable i = 3 
                while i*i < x && (x%i)<>0 do 
                    i <- i + 2
                (x%i)<>0
        let curResource = x.Resources
        let nbins = curResource |> Array.fold ( fun sum x -> sum+x) 0 
        let assign_bins = Array.zeroCreate<int> nbins
        let mutable pos = 0
        for i = 0 to curResource.Length-1 do
            for j = 0 to curResource.[i]-1 do 
                assign_bins.[pos] <- i
                pos <- pos + 1
        let PartitionMapping = Array.zeroCreate<int[]> numPartitions
        let nHash = contentKey
        let mutable nRound = ( numPartitions + curResource.Length - 1 ) / curResource.Length 
        while not (isPrime nRound) || (curResource.Length % nRound) =0 do 
            nRound <- nRound + 1
        for par = 0 to numPartitions-1 do              
            let idx = int ( (nHash+uint64 par) % uint64 nbins)
            let node = assign_bins.[idx]
            let mutable round = par / curResource.Length + 1
            let arr = Array.zeroCreate<_> numReplications
            let rep = Dictionary<_,_>()
            while rep.Count<>numReplications do 
                rep.Clear()
                for i = 0 to numReplications - 1 do 
                    let nd = ( node + i * round) % curResource.Length 
                    arr.[i] <- nd
                    rep.Item(nd) <- true
                round <- round + nRound 
            PartitionMapping.[par] <- arr
        PartitionMapping

type internal CHT = 
    inherit LoadBlanceAlg
    new () = { inherit LoadBlanceAlg( LoadBalanceAlgorithm.CHT ) }
    new ( ids, resource ) = 
        { inherit LoadBlanceAlg( LoadBalanceAlgorithm.CHT, ids, resource ) }
    /// Get a partition mapping using Consistent Hash Table (CHT).
    /// contentKey is a hash value that identifies a certain content in Prajna, which randomize partition assignment for different content
    override x.GetPartitionMapping( contentKey, numPartitions, numReplications ) =
        let PartitionMapping = Array.zeroCreate<int[]> numPartitions
        for par = 0 to numPartitions-1 do
            let mapping = Array.zeroCreate<int> numReplications
            let hashing = Array.zeroCreate<uint64> numReplications
            for node = 0 to x.IDs.Length-1 do
                // Get the maximum hash value of the current node. 
                // If the node has N resources, it will get a proportionally large N share of nodes
                let mutable maxHash = 0UL
                let nodeID = x.IDs.[node]
                let nHash = MurmurHash.MurmurHash64OneStep( contentKey, nodeID )
                for resource = 1 to x.Resources.[node] do
                    maxHash <- Math.Max( maxHash, MurmurHash.MurmurHash64OneStep( nHash, (uint64) (( par <<< 8 ) + resource ) ) )
                // Insert the node the sorted Hash value list. 
                let mutable maxNode = 0
                // We use > so that any hash < 0 is false, so that the first node always gets inserted before all zero entries of 
                // hashing array
                while maxNode < numReplications && hashing.[maxNode] > maxHash do
                    maxNode <- maxNode + 1
                // Insert (node, maxHash) to maxNode position
                if maxNode < numReplications then 
                    if maxNode < numReplications-1 then 
                        Array.Copy( mapping, maxNode, mapping, maxNode+1, numReplications-1-maxNode )
                        Array.Copy( hashing, maxNode, hashing, maxNode+1, numReplications-1-maxNode )
                    mapping.[ maxNode ] <- node
                    hashing.[ maxNode ] <- maxHash
            PartitionMapping.[par] <- mapping
        PartitionMapping

type LoadBlanceAlg with
    static member CreateLoadBalanceAlg( typeof, ids, resource ) : LoadBlanceAlg = 
        match typeof with 
        | LoadBalanceAlgorithm.RoundRobin ->
            new RoundRobin( ids, resource ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.RoundRobin2 ->
            new RoundRobin( ids, resource, FastFailureRecovery = true ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.CHT -> 
            new CHT( ids, resource ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.CHT2 -> 
            new CHT( ids, resource, FastFailureRecovery = true ) :> LoadBlanceAlg
        | _ -> 
            failwith (sprintf "The load balancing algorithm %A is undefined" typeof )
        
    static member DefaultLoadBlanceAlg( ids, resource ) = 
        LoadBlanceAlg.CreateLoadBalanceAlg( LoadBalanceAlgorithm.Default, ids, resource )

    static member CreateLoadBalanceAlg( typeof ) : LoadBlanceAlg = 
        match typeof with 
        | LoadBalanceAlgorithm.RoundRobin ->
            new RoundRobin( ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.RoundRobin2 ->
            new RoundRobin( FastFailureRecovery = true ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.CHT -> 
            new CHT( ) :> LoadBlanceAlg
        | LoadBalanceAlgorithm.CHT2 -> 
            new CHT( FastFailureRecovery = true ) :> LoadBlanceAlg
        | _ -> 
            failwith (sprintf "The load balancing algorithm %A is undefined" typeof )
        
    static member DefaultLoadBlanceAlg( ) = 
        LoadBlanceAlg.CreateLoadBalanceAlg( LoadBalanceAlgorithm.Default )

