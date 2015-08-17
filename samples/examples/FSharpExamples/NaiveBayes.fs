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
        NaiveBayes.fs
  
    Description: 
        Compute histograms for a Naive Bayes classifier
 ---------------------------------------------------------------------------*)
namespace Prajna.Examples.FSharp

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO

open Prajna.Core
open Prajna.Api.FSharp

open Prajna.Examples.Common

type internal Counts = Dictionary<string, int>

type NaiveBayes() =

    let data = Path.Combine(Utility.GetExecutingDir(), "20news-featurized2-tiny.txt")

    let split (c: char) (str:string) = str.Split([|c|], StringSplitOptions.RemoveEmptyEntries)

    let add word n (counts: Counts) = 
        match counts.TryGetValue word with
        | true, c -> counts.[word] <- c + n
        | _ -> counts.[word] <- n

    let numLabels = 20

    let chooseLine (line: string) =
        match split '\t' line with
        | [|_; label; text|] -> 
            let splitLine = split ' ' text
            Some(HashSet<string>(splitLine), Int32.Parse label)
        | _ -> 
            None

    let addWords (numLabels: int) (countsOrNull: Counts[]) (words: HashSet<string>, label: int) = 
        let counts = 
            if countsOrNull = null 
            then Array.init numLabels (fun _ -> new Counts()) 
            else countsOrNull
        words |> Seq.iter (fun w -> counts.[label] |> add w 1)
        counts

    let addCounts (counts1: Counts[]) (counts2: Counts[]) =
        let ret = 
            Array.map2 (fun (lc1: Counts) (lc2: Counts) ->
                for wc in lc2 do 
                    if wc.Key <> null then 
                        lc1 |> add wc.Key wc.Value
                lc1)
                counts1 counts2 
        ret

    let naiveBayesMapReduce (name: string) (cluster: Cluster) (trainSet: string seq) =
        let sparseCounts = 
            DSet<string>(Name = name, Cluster = cluster)
            |> DSet.distributeN 8 trainSet
            |> DSet.choose chooseLine
            |> DSet.mapReduce 
                (fun (words,label) -> seq { for w in words -> w,label } )
                (fun (word, labels) -> 
                    let histogram : int[] = Array.zeroCreate numLabels
                    for l in labels do
                        histogram.[l] <- histogram.[l] + 1
                    word, histogram)
            |> DSet.mapReduce 
                (fun (word,hist) -> 
                    seq {for i = 0 to hist.Length-1 do 
                            if hist.[i] <> 0 then
                                yield i,(word,hist.[i]) } )
                (fun (label,wordCounts) -> 
                    let cs = new Counts()
                    for w,c in wordCounts do 
                        if c <> 0 then
                            cs |> add w c
                    label,cs)
            |> DSet.toSeq
        let ret : Counts[] = Array.zeroCreate numLabels
        for i,cs in sparseCounts do
            ret.[i] <- cs
        ret |> Array.iteri (fun i cs -> if cs = null then ret.[i] <- Counts())
        ret

    let run (cluster: Cluster) = 

        let trainSet, testSet = 
            let all = 
                data 
                |> File.ReadLines 
                |> Seq.toArray
            let numTrain = 200
            all |> Seq.take numTrain |> Seq.toArray, all |> Seq.skip numTrain |> Seq.toArray

        let sw = Stopwatch.StartNew()
        let seqCounts = 
            trainSet
            |> Seq.choose chooseLine
            |> Seq.fold (addWords numLabels) null
        printfn "Seq train took: %A" (sw.Stop(); sw.Elapsed)

        let name = "20News-TinyTest-" + Guid.NewGuid().ToString("D")
        sw.Restart()
        let dsetCounts = 
            DSet<string>(Name = name, Cluster = cluster)
            |> DSet.distributeN 8 trainSet
            |> DSet.choose chooseLine
            |> DSet.fold (addWords numLabels) addCounts null
        printfn "DSet train took: %A" (sw.Stop(); sw.Elapsed)

        sw.Restart()
        let mapReduceCounts = naiveBayesMapReduce name cluster trainSet
        printfn "MapReduce train took: %A" (sw.Stop(); sw.Elapsed)

        let areEqual = 
            let dictToMap (dict: Dictionary<_,_>) = seq {for kvPair in dict -> kvPair.Key, kvPair.Value} |> Map.ofSeq
            Seq.zip3 seqCounts dsetCounts mapReduceCounts
            |> Seq.map (fun (seqLabelCounts, dsetLabelCounts, mapReduceLabelCounts) -> 
                let seqMap = dictToMap seqLabelCounts
                let dsetMap = dictToMap dsetLabelCounts
                let mrMap = dictToMap mapReduceLabelCounts
                seqMap = dsetMap && dsetMap = mrMap)
            |> Seq.forall id
        printfn "Model comparison result: %s" (if areEqual then "Equal" else "Different")

        // Call this to test prediction accuracy on large dataset, but not during unit test
        let evaluate() =
//        do
            printfn "Testing..."
            let priors: float[] = 
                let labelCounts = 
                    trainSet
                    |> Seq.choose chooseLine 
                    |> Seq.map snd
                    |> Seq.countBy id
                    |> Seq.sortBy fst
                    |> Seq.map snd
                    |> Seq.toArray
                let sum = labelCounts |> Array.sum |> float
                labelCounts |> Array.map (fun x -> float x / sum)

            let preds : (int * int * float[])[] = 
                [|for words,label in testSet |> Seq.choose chooseLine do
                    let logProbs : float[] = Array.zeroCreate numLabels
                    for w in words do
                        let wCounts : int[] = 
                            seqCounts 
                            |> Array.map (fun labelCounts -> 
                                match labelCounts.TryGetValue w with
                                | true, c -> c
                                | _ -> 0)
                        let sum = Array.sum wCounts |> float
                        wCounts |> Seq.iteri (fun i c -> logProbs.[i] <- logProbs.[i] + Math.Log(float (c + 1) / (sum + 1.0)))
                    let normProbs = 
                        let probs = Array.map2 (fun logProb prior -> Math.Exp logProb * prior) logProbs priors 
                        let probSum = probs |> Seq.sum
                        probs |> Array.map (fun p -> p / probSum)
                    let prediction = Array.IndexOf(normProbs, Array.max normProbs)
                    yield label, prediction, normProbs |]

            let hits = preds |> Seq.where (fun (l,p,_) -> l = p) |> Seq.length
            let accuracy = float hits / float (preds.Length)
            printfn "Accuracy: %f" accuracy

        areEqual
            
    interface IExample with
        member this.Description = 
            "Create Naive Bayes model"
        member this.Run(cluster) =
            run cluster        
        

    
