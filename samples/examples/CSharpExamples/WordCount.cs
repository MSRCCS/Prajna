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
//        WordCount.cs
//
//    Description: 
//        Count words of a text file           
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
    /// Count the number of words in a document
    /// </summary>
    public class WordCount : IExample
    {
        private string Book = Path.Combine(Utility.GetExecutingDir(), "pg1661.txt");

        private static string[] SplitWords(string line)
        {
            return line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        }

        private bool Count(Cluster cluster)
        {
            var name = "Sherlock-Holmes-" + Guid.NewGuid().ToString("D");
            var corpus = File.ReadAllLines(Book);

            var count1 =
                (new DSet<string> { Name = name, Cluster = cluster })
                .Distribute(corpus)
                .Collect(SplitWords)
                .Count();

            Console.WriteLine("Counted with DSet: there are {0} words", count1);

            var count2 = corpus.SelectMany(SplitWords).Count();

            Console.WriteLine("Counted locally: there are {0} words", count2);
            return count1 == count2;
        }

        /// <summary>
        ///  Description of the example
        /// </summary>
        public string Description
        {
            get
            {
                return "Count the number of words in an book";
            }
        }

        /// <summary>
        ///  Run the example
        /// </summary>
        public bool Run(Cluster cluster)
        {
            return Count(cluster);
        }
    }
}
