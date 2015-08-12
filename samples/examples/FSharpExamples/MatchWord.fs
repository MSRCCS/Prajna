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
        MatchWord.fs
  
    Description: 
        Match a word from a text file    
 ---------------------------------------------------------------------------*)
namespace Prajna.Examples.FSharp

open System
open System.IO

open Prajna.Core
open Prajna.Api.FSharp

open Prajna.Examples.Common

type MatchWord ()=
    
    static let book = Path.Combine(Utility.GetExecutingDir(), "pg1661.txt")

    static let splitWords (str:string) = str.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries) 

    static let matchWordInDSet dset matchWord = 
        dset
        |> DSet.collect (fun s -> splitWords s |> Array.toSeq)
        |> DSet.filter ((=) matchWord)
        |> DSet.count                    
        
    static let Match cluster matchWord =        
        let name = "Sherlock-Holmes-" + Guid.NewGuid().ToString("D")
        let corpus = File.ReadAllLines(book)

        // Store the book into the cluster
        let dset = DSet<string>(Name = name, Cluster = cluster)        

        corpus |> Seq.ofArray|> dset.Store
        let corpusLines = dset |> DSet.loadSource
                       
        let count1 = matchWordInDSet corpusLines matchWord
        printfn "Counted with DSet: The word %s occurs %d times" matchWord count1
        
        let count2 = corpus |> Array.collect splitWords |> Array.filter((=) matchWord) |> Array.length
        printfn "Counted locally: The word %s occurs %d times" matchWord count2

        count1 = (int64 count2)

    interface IExample with
        member this.Description = 
            "Count the occurrences of a word (case-sensitive) in an book"
        member this.Run(cluster) =
            Match cluster "Holmes"
