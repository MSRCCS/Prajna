namespace Prajna.Examples.Tests

open System
open System.Reflection
open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.FSharp

open Prajna.Test.Common
open Prajna.Examples.Common
open Prajna.Tools.FSharp

[<TestFixture(Description = "Tests for Examples")>]
type ExamplesTests() = 
    inherit Prajna.Test.Common.Tester()

    let sharedCluster = TestEnvironment.Environment.Value.LocalCluster
    
    // specifiy one of the types that is defined in the assembly to help find all the IExample test cases in the assembly
    static let findTestCases t =
        [|
            let examples = 
                // specifiy one of the types that is defined in the assembly to help find the assembly
                let asm = Assembly.GetAssembly(t)               
                Utility.GetExamplesFromAssembly(asm)
            for e in examples do
                yield [| e |]
        |]

    // This is the source of test cases for Test x.FSharpExamples.Test
    // It example the FSharpExamples assembly that find all types implements IExample
    static member val FSharpExamples = findTestCases (typeof<Prajna.Examples.FSharp.MatchWord>)

    // This is the source of test cases for Test x.CSharpExamples.Test
    // It example the FSharpExamples assembly that find all types implements IExample
    static member val CSharpExamples = findTestCases (typeof<Prajna.Examples.CSharp.MatchWord>)

    // A data driven tests that will run all the examples in Prajna.Examples.FSharp
    [<Test(Description = "Test for FSharp Examples")>]
    [<TestCaseSource("FSharpExamples")>]
    member x.FSharpExamplesTest(example : IExample) =
        Logger.LogF( LogLevel.MildVerbose, fun _ -> "Start F# samples"  )
        let ret = example.Run(sharedCluster)
        Assert.IsTrue(ret)

    // A data driven tests that will run all the examples in Prajna.Examples.CSharp
    [<Test(Description = "Test for CSharp Examples")>]
    [<TestCaseSource("CSharpExamples")>]
    member x.CSharpExamplesTest(example : IExample) =
        let ret = example.Run(sharedCluster)
        Assert.IsTrue(ret)
