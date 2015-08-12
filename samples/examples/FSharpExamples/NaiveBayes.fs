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
        | _ -> None

    let addWords (counts: Counts[]) (words: HashSet<string>, label: int) = 
        words |> Seq.iter (fun w -> counts.[label] |> add w 1)
        counts

    let addCounts (counts1: Counts[]) (counts2: Counts[]) =
        Seq.zip counts1 counts2
        |> Seq.iter (fun (lc1, lc2) ->
            for wc in lc2 do 
                lc1 |> add wc.Key wc.Value)
        counts1

    let createCounts() = Array.init numLabels (fun _ -> new Counts())

    let run (cluster: Cluster) = 

        let name = "20News-TinyTest-" + Guid.NewGuid().ToString("D")
        let trainSet, testSet = 
            let all = data |> File.ReadLines |> Seq.toArray
            all |> Seq.take 200, all |> Seq.skip 200

        let sw = Stopwatch.StartNew()
        let seqCounts = 
            trainSet
            |> Seq.choose chooseLine
            |> Seq.fold addWords (createCounts())
        printfn "Seq train took: %A" (sw.Stop(); sw.Elapsed)

        sw.Restart()
        let dsetCounts = 
            DSet<string>(Name = name, Cluster = cluster)
            |> DSet.distribute trainSet
            |> DSet.identity
            |> DSet.choose chooseLine
            |> DSet.fold addWords addCounts (createCounts())
        printfn "DSet train took: %A" (sw.Stop(); sw.Elapsed)

        let areEqual = 
            Seq.zip seqCounts dsetCounts
            |> Seq.map (fun (sc, dsc) -> 
                let dictToMap (dict: Dictionary<_,_>) = seq {for kvPair in dict -> kvPair.Key, kvPair.Value} |> Map.ofSeq
                dictToMap sc = dictToMap dsc)
            |> Seq.forall id

        printfn "Model comparison result: %s" (if areEqual then "Equal" else "Different")

        // Call this to test accuracy, but not during test
        let evaluate() =
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
        

    
