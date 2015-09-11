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
        WordPopulation.fs
  
    Description: 
        Get the total population of the world
 ---------------------------------------------------------------------------*)
namespace Prajna.Examples.FSharp

open System
open System.IO

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Tools.FSharp

open Prajna.Examples.Common

open FSharp.Data

// Depend on availability of external resource -- "World Bank", not suitable for unit test
[<SkipUnitTest>]
type WorldPopulation ()=

    let numChars = 26

    let getPopulation cluster year =

        let total =      
            DSet<_>(Name = Guid.NewGuid().ToString("D"), Cluster = cluster)    
            |> DSet.sourceI numChars (
                   fun i ->  // Get the countries from world bank
                             // Each partition deals with contries, the name of which starts with a specified character
                             let countries = WorldBankData.GetDataContext().Countries
                             seq {
                                for c in countries do
                                    if c.Name.ToLowerInvariant().[0] = (char i) + 'a' then
                                        yield c
                             }
               )
            |> DSet.map (                     
                    fun c -> // Get the population of the country
                             let total = c.Indicators.``Total Population (in number of people)``.[year]
                             if Double.IsNaN(total) then
                                0.0
                             else
                                total
               )
            |> DSet.reduce ( fun t1 t2 -> t1 + t2)

        printfn "By year %i, world population reached %s" year (total.ToString("N"))
        total

    let get2010Population cluster = 
        let total = getPopulation cluster 2010
        // Assume 2010's population is close to 7 billion (with an error less than 200 million)
        abs (total - 7e9) < 2. * 1e8

    interface IExample with
        member this.Description = 
            "Get the population of the world"
        member this.Run(cluster) =
            get2010Population cluster
