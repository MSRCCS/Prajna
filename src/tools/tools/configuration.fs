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
        configuration.fs
  
    Description: 
        Utilities for handling Application Configurations

 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System.IO

// Record types used to record the assembly binding informations (we only care about redirects for now)
type internal BindingRedirect = { OldVersion : string;  NewVersion : string}
type internal AssemblyIdentity = { Name : string; PublicKeyToken : string option; Culture : string option; ProcessorArchitecture : string option}
type internal DependentAssembly = { AssemblyIdentity : AssemblyIdentity; BindingRedirects : BindingRedirect[] }
type internal AssemblyBinding = { DependentAssemblies : DependentAssembly[] }

// Utils for managing configurations
module internal ConfigurationUtils =
    open System
    open System.Configuration
    open System.Xml.Linq
    open System.Linq

    open Prajna.Tools.FSharp

    // xname with namespace
    let private xnn n = XName.Get(n, "urn:schemas-microsoft-com:asm.v1")
    // xname without namespace
    let private xn n = XName.Get(n)
    // get required attribute value
    let private attrValue (elem : XElement) (attrName: string) =
        elem.Attribute(xn attrName).Value
    // get optional attribute's value
    let private optionalAttrValue (elem : XElement) (attrName: string) =
        let attr = elem.Attribute(xn attrName)
        match attr with
        | null -> None
        | v -> Some v.Value

    // Parse the "assemblyBinding" element from the "runtime"
    // Note: it might be easier/less error-prone to maintain if use XmlSerializer/DataContractSerializer
    //       However, XmlSerializer requires the type used for serialization to be public.
    //       DataContractSerializer does not seem to be able to handle multiple same elements for the 
    //       same parent without being grouped into an XML collection.
    //       So, for now, use Linq.Xml to parse/write the xml segment
    let private getAssemblyBindingsFromConfig (config : Configuration) =
        let getAssemblyBindingsFromRuntime (runtime : XElement) =
            let asmBinding = runtime.Element(xnn "assemblyBinding")
            if Utils.IsNotNull(asmBinding) then
                {
                    DependentAssemblies = 
                        asmBinding.Elements(xnn "dependentAssembly")
                        |> Seq.map(fun da ->
                              {
                                  AssemblyIdentity = 
                                      let ai = da.Element(xnn "assemblyIdentity")
                                      { Name = "name" |> attrValue ai 
                                        PublicKeyToken = "publicKeyToken" |> optionalAttrValue ai 
                                        Culture = "culture" |> optionalAttrValue ai 
                                        ProcessorArchitecture = "processorArchitecture" |> optionalAttrValue ai 
                                      }
                                  BindingRedirects = 
                                      da.Elements(xnn "bindingRedirect") 
                                      |> Seq.map(fun br -> { OldVersion = "oldVersion" |> attrValue br
                                                             NewVersion = "newVersion" |> attrValue br })
                                      |> Array.ofSeq
                              }
                           )
                         |> Array.ofSeq
                } |> Some
            else
                None

        let runtimeSection = config.Sections.Get("runtime")
        /// Always have this section
        let runtimeXml = runtimeSection.SectionInformation.GetRawXml()
        if Utils.IsNotNull runtimeXml then
            let xDoc = XDocument.Parse(runtimeXml)
            let runtime = xDoc.Element(xn "runtime")
            if Utils.IsNotNull runtime then
                getAssemblyBindingsFromRuntime(runtime)
            else 
                None
        else
            None

    let private mergeBindingRedirects (br1 : BindingRedirect[]) (br2 : BindingRedirect[]) =
        br1.Union(br2) |> Array.ofSeq

    // merge two assembly bindings
    // Note: currently the merge is naive: 
    //  * based on that the identity is completely the same, which may not be the case
    //  * also it does not try to see if there's any conflicts between the binding redirects
    // This is probably OK for our current usage, since the target is the container exe which doe not really contain many assemblyBinding's.
    let private mergeAssemblyBindings (a1 : AssemblyBinding option) (a2 : AssemblyBinding option) =
        if Option.isNone a1 then a2
        elif Option.isNone a2 then a1
        else
            let das1 = a1.Value.DependentAssemblies;
            let das2 = a2.Value.DependentAssemblies;
        
            let das = 
                Seq.concat ( seq { yield das1; yield das2})
                |> Seq.groupBy (fun das -> das.AssemblyIdentity)
                |> Seq.map( fun (id, grp) -> (id, (grp |> Seq.reduce(fun v1 v2 -> { AssemblyIdentity = v1.AssemblyIdentity
                                                                                    BindingRedirects = mergeBindingRedirects v1.BindingRedirects v2.BindingRedirects
                                                                                  } ))))
                |> Seq.map ( fun (id, x) -> { AssemblyIdentity = id;  BindingRedirects = x.BindingRedirects })
                |> Array.ofSeq
            { DependentAssemblies = das } |> Some

    // serialize the assembly binding to xml
    let private assemblyBindingToXml (a : AssemblyBinding) =
        let rootXml = XElement(xnn "assemblyBinding")
        a.DependentAssemblies 
        |> Array.iter( fun da ->
              let daXml = XElement(xnn "dependentAssembly")

              let id = da.AssemblyIdentity
              let idXml = XElement(xnn "assemblyIdentity",  XAttribute(xn "name", id.Name))
              match id.PublicKeyToken with
              | Some v -> idXml.Add(XAttribute(xn "publicKeyToken", v))
              | _ -> ()
              match id.Culture with
              | Some v -> idXml.Add(XAttribute(xn "culture", v))
              | _ -> ()
              match id.ProcessorArchitecture with
              | Some v -> idXml.Add(XAttribute(xn "processorArchitecture", v))
              | _ -> ()
              daXml.Add(idXml)

              da.BindingRedirects
              |> Array.iter (fun br ->  daXml.Add(XElement(xnn "bindingRedirect", XAttribute(xn "oldVersion", br.OldVersion), XAttribute(xn "newVersion", br.NewVersion))))

              rootXml.Add(daXml))
        rootXml

    /// Get the content of assemblyBinding (if any) from an executable's configuration
    let private getAssemblyBindingsForExe (path : string) = 
        try
            let config = ConfigurationManager.OpenExeConfiguration(path)
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Get the configuration for '%s'" path)
            (config, getAssemblyBindingsFromConfig(config)) |> Some
        with
        | :? ConfigurationErrorsException as ex -> Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Fail to load the configuration for '%s' due to an ConfigurationErrorsException : %A" path ex)
                                                   None
        | ex -> Logger.LogF(LogLevel.Warning, fun _ -> sprintf "Fail to get/parse the configuration for '%s' due to an unexpected exception: %A" path ex)
                None

    /// Replace "assemblyBinding for an exe"
    let private replaceAssemblyBindingsForExe (config : Configuration) (a : AssemblyBinding) =
         let asmXml = assemblyBindingToXml a
         let runtimeSection = config.Sections.Get("runtime")
         // There is always a runtime section
         let runtimeXml = runtimeSection.SectionInformation.GetRawXml()
         let runtimeElem =
             if Utils.IsNotNull runtimeXml then
                 let xDoc = XDocument.Parse(runtimeXml)
                 let runtime = xDoc.Element(xn "runtime")
                 if Utils.IsNotNull runtime then
                     let oldAsmBinding = runtime.Element(xnn "assemblyBinding")
                     if Utils.IsNotNull oldAsmBinding then oldAsmBinding.Remove()
                     runtime
                 else
                     XElement(xn "runtime")
             else
                 XElement(xn "runtime")
         runtimeElem.Add(asmXml)
         runtimeSection.SectionInformation.SetRawXml(runtimeElem.ToString())
         config.Save()

    /// Get the content of assemblyBinding (if any) from the current app's configuration
    let GetAssemblyBindingsForCurrentExe () = 
        try
            let config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None)
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Get the configuration for the current exe : %s" config.FilePath)
            getAssemblyBindingsFromConfig(config)
        with
        | :? ConfigurationErrorsException as ex -> Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Fail to load the configuration for current exe due to an ConfigurationErrorsException : %A" ex)
                                                   None
        | ex -> Logger.LogF(LogLevel.Warning, fun _ -> sprintf "Fail to get/parse the configuration for current exe due to an unexpected exception: %A" ex)
                None

    let ReplaceAssemblyBindingsForExeIfNeeded (exePath : string) (appAsmBinding : AssemblyBinding option) =
        try
            match appAsmBinding with
            | Some ab1 ->
                match getAssemblyBindingsForExe(exePath) with
                | Some (config, ab2) ->
                     let ab = mergeAssemblyBindings (ab1 |> Some) ab2
                     ab.Value |> replaceAssemblyBindingsForExe config
                | None -> ()
            | None -> ()
        with
        | :? ConfigurationErrorsException as ex -> Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Fail to replace the configuration for '%s' due to an ConfigurationErrorsException : %A" exePath ex)
        | ex -> Logger.LogF(LogLevel.Warning, fun _ -> sprintf "Fail to get/parse the configuration for '%s' due to an unexpected exception: %A" exePath ex)

    // Pack asm binding
    let PackAsmBinding (ms : Stream) (a: AssemblyBinding option) =
        match a with 
        | None ->  ms.WriteInt32(0)
        | Some v ->
            let das = v.DependentAssemblies
            if Array.isEmpty das then
                ms.WriteInt32(0)
            else
                ms.WriteInt32(das.Length)
                das |> Array.iter (
                    fun da -> let id = da.AssemblyIdentity
                              ms.WriteString(id.Name)
                              match id.PublicKeyToken with
                              | Some v -> ms.WriteBoolean(true)
                                          ms.WriteString(v)
                              | None -> ms.WriteBoolean(false)
                              match id.Culture with
                              | Some v -> ms.WriteBoolean(true)
                                          ms.WriteString(v)
                              | None -> ms.WriteBoolean(false)
                              match id.ProcessorArchitecture with
                              | Some v -> ms.WriteBoolean(true)
                                          ms.WriteString(v)
                              | None -> ms.WriteBoolean(false)

                              let brs = da.BindingRedirects
                              ms.WriteInt32(brs.Length)
                              brs |> Array.iter(
                                  fun br -> ms.WriteString(br.OldVersion)
                                            ms.WriteString(br.NewVersion)
                              )
                )

    // Unpack asm binding information
    let UnpackAsmBinding (ms : Stream) =
        let daCount = ms.ReadInt32()
        if daCount = 0 then
            None
        else
            {
                DependentAssemblies = 
                [|
                    for i in 1..daCount do
                        yield {
                                  AssemblyIdentity = 
                                      { Name = ms.ReadString()
                                        PublicKeyToken = if ms.ReadBoolean() then ms.ReadString() |> Some else None
                                        Culture = if ms.ReadBoolean() then ms.ReadString() |> Some else None
                                        ProcessorArchitecture = if ms.ReadBoolean() then ms.ReadString() |> Some else None
                                      }
                                  BindingRedirects = 
                                      let brCount = ms.ReadInt32()
                                      [|
                                          for j in 1..brCount do
                                              yield { OldVersion = ms.ReadString(); NewVersion = ms.ReadString() }
                                      |]
                              }
                |]
            } |> Some
