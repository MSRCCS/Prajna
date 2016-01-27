(*---------------------------------------------------------------------------
    Copyright 2014 Microsoft

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
        VHub.Instance.fs
  
    Description: 
        Classes for Image/Video Recognition Classifier

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Nov. 2014
    
 ---------------------------------------------------------------------------*)
namespace VMHub.ServiceEndpoint

open System
open System.Web
open System.Text
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Net
open System.Threading
open System.Threading.Tasks
open System.Diagnostics
open System.Runtime.Serialization

open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.HttpBuilderHelper
open Prajna.Tools.Network

open Prajna.Core
open Prajna.Service.FSharp
open Prajna.Service.ServiceEndpoint

open VMHub.Data


type VHubExecutionMode = QueryExecutionMode<RecogRequest, RecogReply>

[<AllowNullLiteral>]
type VHubServiceInstance() =
    inherit ServiceInstance<RecogRequest, RecogReply>()
    member val RecogDomain = "" with get, set
    member val EngineName = "" with get, set
    member val SampleImageGuid = Guid.Empty with get, set
    member val LeastSmallestDimension = 100 with get, set
    static member ConstructServiceID( domain: string, engine: string ) = 
        let fullname = if StringTools.IsNullOrEmpty engine then domain else domain + "@" + engine
        let serviceID = HashStringToGuid( fullname )
        serviceID
    static member EncodeCollections ( collection:seq< ServiceInstanceBasic>, ms:Stream ) = 
        if Utils.IsNull collection then
            ms.WriteVInt32(0) 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "No Service Collection, VHub will not be able to operate" ))
        else
            let filtered = collection |> Seq.choose( fun obj -> match obj with 
                                                                | :? VHubServiceInstance as service -> 
                                                                    Some service 
                                                                | _ -> 
                                                                    None
                                                    ) |> Seq.toArray
            let len = filtered.Length
            ms.WriteVInt32( len ) 
            for i = 0 to len - 1 do 
                let service = filtered.[i] 
                ms.WriteBytes( service.ServiceID.ToByteArray() )
                ms.WriteVInt32( service.MaxConcurrentItems ) 
                ms.WriteStringV( service.RecogDomain ) 
                ms.WriteStringV( service.EngineName ) 
                ms.WriteBytes( service.SampleImageGuid.ToByteArray() ) 
                ms.WriteVInt32( service.LeastSmallestDimension )
                ms.WriteGuid( service.InputSchemaID )
                ms.WriteGuid( service.OutputSchemaID )
    static member DecodeCollections( ms:Stream ) = 
        let len = ms.ReadVInt32() 
        let arr = Array.zeroCreate<_> len 
        let buf = Array.zeroCreate<_> 16
        for i = 0 to len - 1 do 
            ms.ReadBytes( buf ) |> ignore
            let serviceID = Guid(buf) 
            let maxItems = ms.ReadVInt32() 
            let recogDomain = ms.ReadStringV() 
            let engineName = ms.ReadStringV()
            ms.ReadBytes( buf ) |> ignore
            let sampleImageGuid = Guid(buf)
            let leastSmallestDimension = ms.ReadVInt32()
            let inputSchemaID = ms.ReadGuid() 
            let outputSchemaID = ms.ReadGuid()
            arr.[i] <- VHubServiceInstance( ServiceID = serviceID, 
                                            MaxConcurrentItems = maxItems,  
                                            RecogDomain=recogDomain, 
                                            EngineName=engineName, 
                                            SampleImageGuid=sampleImageGuid, 
                                            LeastSmallestDimension=leastSmallestDimension,
                                            InputSchemaID = inputSchemaID, 
                                            OutputSchemaID = outputSchemaID )
        arr |> Seq.map( fun service -> service :> ServiceInstanceBasic )

/// <summary>
/// VHubBackendStartParam: this will be used to launch a VHubInstance at remote
/// </summary>
/// <param name = "localInfo"> Local for comparison. Used by System.Globalization.CultureInfo() </param>
/// <param name = "displayName"> Display Name of the classifier server </param>
/// <param name = "providerName"> Provider Name of the classifier server </param>
/// <param name = "versionString"> Version of the classifier server </param>
/// <example>
/// See SampleRecogServer on how the classifier is instantiated. 
/// </example>
[<AllowNullLiteral>]
type VHubBackendStartParam(  ) as x = 
    inherit BackEndServiceParam()
    do 
        x.EncodeServiceCollectionAction <- EncodeCollectionAction( VHubServiceInstance.EncodeCollections )
/// <summary>
/// This class represent a backend query instance. The developer needs to extend BackEndInstance class, to implement the missing functions.
/// The following command are reserved:
///     List, Buffer : in the beginning, to list Guids that are used by the current backend instance. 
///     Read, Buffer : request to send a list of missing Guids. 
///     Write, Buffer: send a list of missing guids. 
///     Echo, QueryReply : Keep alive, send by front end periodically
///     EchoReturn, QueryReply: keep alive, send to front end periodically
///     Set, QueryReply: Pack serviceInstance information. 
///     Get, QueryReply: Unpack serviceInstance information 
///     Request, QueryReply : FrontEnd send in a request (reqID, serviceID, payload )
///     Reply, QueryReply : BackEnd send in a reply
///     TimeOut, QueryReply : BackEnd is too heavily loaded, and is unable to serve the request. 
///     NonExist, QueryReply : requested BackEnd service ID doesn't exist. 
/// Programmer will need to extend BackEndInstance class to fill in OnStartBackEnd. The jobs of OnStartBackEnd are: 
/// 1. fill in ServiceCollection entries. Note that N parallel thread will be running the Run() operation. However, OnStartBackEnd are called only once.  
/// 2. fill in BufferCache.Current on CacheableBuffer (constant) that we will expect to store at the server side. 
/// 3. fill in MoreParseFunc, if you need to extend beyond standard message exchanged between BackEnd/FrontEnd
///         Please make sure not to use reserved command (see list in the description of the class BackEndInstance )
///         Network health and message integrity check will be enforced. So when you send a new message to the FrontEnd, please use:
///             health.WriteHeader (ms)
///             ... your own message ...
///             health.WriteEndMark (ms )
/// 4. Setting TimeOutRequestInMilliSecond, if need
/// 5. Function in EncodeServiceCollectionAction will be called to pack all service collection into a stream to be sent to the front end. 
/// </summary>
[<AllowNullLiteral; AbstractClass >]
type VHubBackEndInstance<'StartParam when 'StartParam :> VHubBackendStartParam >(engineName) as x = 
    inherit BackEndInstance< 'StartParam >() 
    do 
        x.OnStartBackEnd.Add( new BackEndOnStartFunction<'StartParam>( x.InstallCodec) )
        x.OnInitialMsgToFrontEnd.Add( new FormMsgToFrontEndFunction( x.EncodeAppInfo ) )
        ContractStore.ExportSeqFunction (VHubBackEndInstance<_>.ContractNameGetServiceCollection, x.GetServiceCollection, -1, true )
    static member val ContractNameGetServiceCollection = "GetServiceCollection" with get
    member x.InstallCodec param = 
        JobDependencies.InstallDeserializer<RecogRequest>( VHubRecogRequestHelper.RecogRequestCodecGuid, VHubRecogRequestHelper.Unpack, "Customized:RecogRequest")
        JobDependencies.InstallSerializer<RecogReply>( VHubRecogResultHelper.RecogReplyCodecGuid, VHubRecogResultHelper.Pack, "Customized:RecogReply" )
        true
    member val AppInfo = null with get, set
    member x.RegisterAppInfo( providerID: Guid, versionString: string ) =
        x.AppInfo <- VHubAppInfo( ProviderID = providerID, VersionString = versionString, HostName = RemoteExecutionEnvironment.MachineName + ":" + RemoteExecutionEnvironment.ContainerName  )
    member x.EncodeAppInfo() = 
        if Utils.IsNull x.AppInfo then 
            failwith "Need to call RegisterAppInfo to initialize the recognition instance"
        else
            let msApp = new MemStream( 1024 )
            Seq.singleton ( ControllerCommand( ControllerVerb.Open, ControllerNoun.QueryReply), VHubAppInfo.Pack( x.AppInfo, msApp ) :?> MemStream )
    static member ServiceIDForDomain( recognitionDomainInput:string ) = 
        let recognitionDomain = recognitionDomainInput.Trim().ToLower()
        let megaID = VHubServiceInstance.ConstructServiceID( recognitionDomain, null )
        megaID
    /// Register a service function call 
    member x.RegisterService<'U,'V>(domain:string, serviceImageName) =
        let unifiedDomain = domain.Trim().ToLower()
        let domainID = VHubServiceInstance.ConstructServiceID( unifiedDomain, engineName )
        let megaID = VHubServiceInstance.ConstructServiceID( unifiedDomain, null )
        if domainID = Guid.Empty || megaID = Guid.Empty then 
            /// This should never happen. 
            failwith ( sprintf "Recog domain %s hash to Guid.Empty, try to use a different domain name" unifiedDomain )
        else
            let serviceInstance = VHubServiceInstance( ServiceID = domainID, RecogDomain = domain, EngineName = engineName, LeastSmallestDimension = 0 )
            if not (StringTools.IsNullOrEmpty( serviceImageName )) then 
                let imgBuf = FileTools.ReadBytesFromFile serviceImageName
                let imgTypeWDot = Path.GetExtension( serviceImageName ).Trim().ToLower()
                let imgType = if imgTypeWDot.Length > 0 then imgTypeWDot.Substring(1) else imgTypeWDot
                let imgTypeBuf = System.Text.Encoding.UTF8.GetBytes( imgType )
                let imgID = BufferCache.Current.AddCacheableBuffer( CacheableBuffer( imgBuf, imgTypeBuf ) )
                serviceInstance.SampleImageGuid <- imgID
            x.AddServiceInstance( serviceInstance )
            serviceInstance.InputSchemaID <- SchemaJSonHelper<'U>.SchemaID()
            serviceInstance.OutputSchemaID <- SchemaJSonHelper<'V>.SchemaID()
            serviceInstance
    /// Helper function to decode generic service 
    static member DecodeInput<'U>( reqdata : byte[])= 
        let inDecoder = SchemaJSonHelper<'U>.Decoder
        let msInput = new MemoryStream( reqdata ) 
        let input = inDecoder( msInput )
        input
    /// Helper function to encode generic output
    static member EncodeOutput<'V>(output: 'V) = 
        let outEncoder = SchemaJSonHelper<'V>.Encoder
        let msOutput = new MemoryStream()
        outEncoder( output, msOutput)
        let msBuffer = msOutput.GetBuffer()
        let msLength = int msOutput.Length
        let outputBuf = Array.zeroCreate<byte> msLength
        Buffer.BlockCopy( msBuffer, 0, outputBuf, 0, msLength )
        let reply = RecogReply( Confidence = 1.0, Result = outputBuf )
        reply
    /// Helper function to decode generic service 
    static member WrappedServiceFunc<'U,'V>( serviceFunc: 'U -> 'V ) (jobID: Guid, timeBudget: int, req: RecogRequest)= 
        let input = VHubBackEndInstance<_>.DecodeInput( req.Data )
        let output = serviceFunc( input )
        VHubBackEndInstance<_>.EncodeOutput<'V>( output )
    /// Helper function to decode generic service 
    static member WrappedServiceFuncAsync<'U,'V>( serviceFunc: 'U -> Task<'V> ) (jobID: Guid, timeBudget: int, req: RecogRequest)= 
        let input = VHubBackEndInstance<_>.DecodeInput( req.Data )
        serviceFunc( input ).ContinueWith( fun (outputTask:Task<'V>) -> VHubBackEndInstance<_>.EncodeOutput<'V>( outputTask.Result ) )


    /// <summary>
    /// Register a service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "leastSmallestDimension"> The smallest dimension of an image </param> 
    /// <param name = "classifierFunc"> A classifier function that will be run once the recognition service is called. </param>
    /// </summary>
    member x.RegisterService<'U,'V>( domain, serviceImageName, serviceFunc: 'U -> 'V ) = 
        let serviceInstance = x.RegisterService<'U,'V>( domain, serviceImageName )
        serviceInstance.ExecutionMode <- VHubExecutionMode.QueryExecutionByFunc (VHubBackEndInstance<_>.WrappedServiceFunc<'U,'V>(serviceFunc))
        serviceInstance
    /// <summary>
    /// Register a service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "leastSmallestDimension"> The smallest dimension of an image </param> 
    /// <param name = "classifierFunc"> A classifier function that will be run once the recognition service is called. </param>
    /// </summary>
    member x.RegisterServiceAsync<'U,'V>( domain, serviceImageName, serviceFunc: 'U -> Task<'V> ) = 
        let serviceInstance = x.RegisterService<'U,'V>( domain, serviceImageName )
        serviceInstance.ExecutionMode <- VHubExecutionMode.QueryByTask (VHubBackEndInstance<_>.WrappedServiceFuncAsync<'U,'V>(serviceFunc))
        serviceInstance


    /// <summary>
    /// Register a classifier service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "classifierFunc"> A classifier function that will be run once the recognition service is called. </param>
    /// This function should be called during part of OnStartBackEnd. 
    /// The serviceID is the hashed lowercase of the recognitionDomains
    /// </summary>
    member x.RegisterClassifier( recognitionDomainInput:string, sampleImageNames, leastSmallestDimension ) = 
        let recognitionDomain = recognitionDomainInput.Trim().ToLower()
        let serviceID = VHubServiceInstance.ConstructServiceID( recognitionDomain, engineName )
        let megaID = VHubServiceInstance.ConstructServiceID( recognitionDomain, null )
        if serviceID = Guid.Empty || megaID = Guid.Empty then 
            /// This should never happen. 
            failwith ( sprintf "Recog domain %s hash to Guid.Empty, try to use a different domain name" recognitionDomain )
        else
            let classifierInstance = VHubServiceInstance( ServiceID = serviceID, RecogDomain = recognitionDomain, EngineName = engineName, LeastSmallestDimension = leastSmallestDimension )
            if not (StringTools.IsNullOrEmpty( sampleImageNames )) then 
                let imgBuf = FileTools.ReadBytesFromFile sampleImageNames
                let imgTypeWDot = Path.GetExtension( sampleImageNames ).Trim().ToLower()
                let imgType = if imgTypeWDot.Length > 0 then imgTypeWDot.Substring(1) else imgTypeWDot
                let imgTypeBuf = System.Text.Encoding.UTF8.GetBytes( imgType )
                let imgID = BufferCache.Current.AddCacheableBuffer( CacheableBuffer( imgBuf, imgTypeBuf ) )
                classifierInstance.SampleImageGuid <- imgID
            x.AddServiceInstance( classifierInstance )
            classifierInstance
    /// <summary>
    /// Register a classifier service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "leastSmallestDimension"> The smallest dimension of an image </param> 
    /// <param name = "classifierFunc"> A classifier function that will be run once the recognition service is called. </param>
    /// </summary>
    member x.RegisterClassifier( recognitionDomain, sampleImageNames, leastSmallestDimension, classifierFunc ) = 
        let classifierInstance = x.RegisterClassifier( recognitionDomain, sampleImageNames, leastSmallestDimension )
        classifierInstance.ExecutionMode <- VHubExecutionMode.QueryExecutionByFunc classifierFunc
        classifierInstance
    /// <summary>
    /// Register a classifier service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "leastSmallestDimension"> The smallest dimension of an image </param> 
    /// <param name = "maxConcurrentItems"> Maximum number of objects that can be feeded into the classifier </param> 
    /// <param name = "classifierFunc"> A classifier function that will be run a Task, which completes once the recognition service completes. </param>
    /// </summary>
    member x.RegisterClassifierTask( recognitionDomain, sampleImageNames, leastSmallestDimension, maxConcurrentItems, classifierFuncTask ) = 
        let classifierInstance = x.RegisterClassifier( recognitionDomain, sampleImageNames, leastSmallestDimension )
        classifierInstance.MaxConcurrentItems <- maxConcurrentItems
        classifierInstance.ExecutionMode <- VHubExecutionMode.QueryByTask classifierFuncTask
        classifierInstance
    /// <summary>
    /// Register a classifier service, with supported domain, a local filename that contains a sample image, and a recognition interface. 
    /// This interface is designed for each of use by C# and other .Net developers. 
    /// The operation of classifier service registration should be performed before registering an endpoint. 
    /// <param name = "recognitionDomains"> A set of domains that the current classifier will recognize </param>
    /// <param name = "sampleImageNames"> A local file that contains a sample image for the recognizer </param>
    /// <param name = "leastSmallestDimension"> The smallest dimension of an image </param> 
    /// <param name = "classifierFunc"> A classifier function that will be run once the recognition service is called. </param>
    /// </summary>
    member x.RegisterClassifierCS( recognitionDomain, sampleImageNames, leastSmallestDimension, classifierFunc:Func<Guid,int,RecogRequest,RecogReply> ) = 
        x.RegisterClassifier( recognitionDomain, sampleImageNames, leastSmallestDimension, fun tuple -> let id, timeBudget, req = tuple
                                                                                                        classifierFunc.Invoke( id, timeBudget, req ) )
    /// <summary> 
    /// Return query performance in the following forms:
    ///     frontend server, serviceID, service msg, msInQueue, msInProcessing
    /// service_msg: null (not served yet).
    ///              "Timeout", the request is timed out. 
    ///              "Reply", the request is succesfully served. msInQueue
    /// </summary>
    member x.GetServiceCollection() = 
        let lst = List<_>()
        for pair in x.ServiceCollection do 
            let serviceID = pair.Key
            let bag = pair.Value
            let inst = Seq.find ( fun _ -> true ) bag
            match inst with 
            | :? VHubServiceInstance as vHubInstance -> 
                let engineName = vHubInstance.EngineName
                let domainName = vHubInstance.RecogDomain
                let bExist, capacity = x.ServiceCapacity.TryGetValue( serviceID ) 
                if bExist then  
                    lst.Add( serviceID, RemoteExecutionEnvironment.MachineName, RemoteExecutionEnvironment.ContainerName, engineName, domainName, !capacity.TotalItems, !capacity.CurrentItems, capacity.Capacity )
            | _ -> 
                ()
        lst :> seq<_>

                

