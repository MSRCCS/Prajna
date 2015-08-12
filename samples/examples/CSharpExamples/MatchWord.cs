//---------------------------------------------------------------------------
//    Copyright 2013 Microsoft
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.                                                      
//
//    File: 
//        MatchWord.cs
//
//    Description: 
//        Match a word from a text file       
// ---------------------------------------------------------------------------

namespace Prajna.Examples.CSharp
{
    using System;
    using System.IO;
    using System.Linq;

    using Prajna.Core;
    using Prajna.Api.CSharp;

    using Prajna.Examples.Common;

    /// <summary>
    /// Example on matching a word from a document
    /// </summary>
    public class MatchWord : IExample
    {
        private string Book = Path.Combine(Utility.GetExecutingDir(), "pg1661.txt");

        private static string[] SplitWords(string line)
        {
            return line.Split(new char[] {' '}, StringSplitOptions.RemoveEmptyEntries);
        }

        private bool Match(Cluster cluster, string matchWord)
        {
            var name = "Sherlock-Holmes-" + Guid.NewGuid().ToString("D");
            var corpus = File.ReadAllLines(Book);

            // Store the file to the cluster
            var dset = new DSet<string> { Name = name, Cluster = cluster };
            dset.Store(corpus);

            var corpusLines = dset.LoadSource();
            var count1 = 
                dset.Collect(SplitWords)
                    .Filter(w => w == matchWord)
                    .Count();
            Console.WriteLine("Counted with DSet: The word {0} occurs {1} times", matchWord, count1);

            var count2 =
                corpus.SelectMany(SplitWords)
                  .Where(w => w == matchWord)
                  .Count();
            Console.WriteLine("Counted locally: The word {0} occurs {1} times", matchWord, count2);
            return count1 == count2;
        }

        /// <summary>
        ///  Description of the example
        /// </summary>
        public string Description
        {
            get
            {
                return "Count the occurrences of a word (case-sensitive) in an book";
            }
        }

        /// <summary>
        ///  Run the example
        /// </summary>
        public bool Run(Cluster cluster)
        {
            return Match(cluster, "Sherlock");
        }
    }
}
