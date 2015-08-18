namespace Prajna.Examples.Tests

open System
open System.Reflection
open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.FSharp

open Prajna.Test.Common
open Prajna.Examples.Common

[<TestFixture(Description = "Tests for Examples")>]
type ExamplesTests() = 
    let sharedCluster = TestEnvironment.Environment.Value.LocalCluster
    let sw = Diagnostics.Stopwatch()

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

    // To be called before each test
    [<SetUp>] 
    member x.InitTest () =
        sw.Start()
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Test %s starts (%s) #####" TestContext.CurrentContext.Test.FullName (StringTools.UtcNowToString())))

    // To be called right after each test
    [<TearDown>] 
    member x.CleanUpTest () =
        sw.Stop()
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Test %s ends (%s): %s (%i ms) #####" TestContext.CurrentContext.Test.FullName (StringTools.UtcNowToString()) (TestContext.CurrentContext.Result.Status.ToString()) sw.ElapsedMilliseconds))


    // A data driven tests that will run all the examples in Prajna.Examples.FSharp
    [<Test(Description = "Test for FSharp Examples")>]
    [<TestCaseSource("FSharpExamples")>]
    member x.FSharpExamplesTest(example : IExample) =
        let ret = example.Run(sharedCluster)
        Assert.IsTrue(ret)

    // A data driven tests that will run all the examples in Prajna.Examples.CSharp
    [<Test(Description = "Test for CSharp Examples")>]
    [<TestCaseSource("CSharpExamples")>]
    member x.CSharpExamplesTest(example : IExample) =
        let ret = example.Run(sharedCluster)
        Assert.IsTrue(ret)
