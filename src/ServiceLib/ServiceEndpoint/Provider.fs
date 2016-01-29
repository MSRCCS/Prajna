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
		VHub.Provider.fs
  
	Description: 
		Image/Video Recognition Provider (per companies/institution)

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
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Net
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.HttpBuilderHelper

open Prajna.Core
open Prajna.Service.ServiceEndpoint
open VMHub.Data

type VHubDomainFlag = 
    | VHubSpecificDomain = '#'
    | VHubAllDomain = '*'

type VHubRecogResultHelper() =
    static member FixedClassificationResultItem( info: string ) = 
        RecogReply ( Confidence = 1.0,
                     Description = info, 
                     PerfInformation = "<null>",
                     Result = null, 
                     AuxData = [| VHubSetting.ImageNotFoundGuid |] )
    static member val UnidentifiedObjectClassificationResultItem = VHubRecogResultHelper.FixedClassificationResultItem( "Unidentified Object" ) with get
    static member val ServiceNotAvailableClassificationResultItem = VHubRecogResultHelper.FixedClassificationResultItem( "No Service" ) with get
    static member val ServiceBusyClassificationResultItem = VHubRecogResultHelper.FixedClassificationResultItem( "All servers are busy" ) with get
    static member FixedClassificationResult( info: string, subinfo: string ) = 
        RecogReply (  Confidence = 1.0, 
                      Description = info, 
                      PerfInformation = subinfo,
                      Result = null, // subinfo is not carried over. Text.Encoding.UTF8.GetBytes( subinfo ), 
                      AuxData = [| VHubSetting.ImageNotFoundGuid |]  )
    static member val NotImplementedClassificationResult = VHubRecogResultHelper.FixedClassificationResult( "Not implemented", "Not implemented" ) with get
    static member val UnidentifiedObjectClassificationResult = VHubRecogResultHelper.FixedClassificationResult( "Unidentified Object", "Unidentified Object" ) with get
    static member val ServiceNotAvailableClassificationResult = VHubRecogResultHelper.FixedClassificationResult( "No Service", "No Service" ) with get
    static member val ServiceBusyClassificationResult = VHubRecogResultHelper.FixedClassificationResult( "All servers are busy", "All servers are busy" ) with get
    static member val TimeoutClassificationResult = VHubRecogResultHelper.FixedClassificationResult( "Query Timeout", "Query Timeout" ) with get
    // Constant that uniquely identifies the codec format of RecogReply
    static member val RecogReplyCodecGuid = Guid( "358C8C8B-AFA1-48D2-8910-695B7F78397D" ) with get
    // Encode a reply, with the associate id back to server. 
    static member Pack( x: RecogReply, ms: Stream ) = 
        ms.WriteDouble( x.Confidence ) 
        ms.WriteString( x.Description )
        ms.WriteBytesWLen( x.Result ) 
        let lenAuxData = if Utils.IsNull x.AuxData then 0 else x.AuxData.Length
        ms.WriteVInt32( lenAuxData ) 
        for i = 0 to lenAuxData - 1 do
            ms.WriteBytes( x.AuxData.[i].ToByteArray() ) 
    // Decode a reply            
    static member Unpack( ms: Stream ) = 
        try
            let confidence = ms.ReadDouble() 
            let description = ms.ReadString()
            let result = ms.ReadBytesWLen() 
            let lenAuxData = ms.ReadVInt32()
            let auxData = 
                if lenAuxData <= 0 then 
                    null 
                else
                    let buf = Array.zeroCreate<_> 16
                    let guidArray = Array.zeroCreate<_> lenAuxData
                    for i = 0 to lenAuxData - 1 do
                        ms.ReadBytes( buf ) |> ignore
                        guidArray.[i] <- Guid(buf)
                    guidArray
            let reply = RecogReply ( Confidence = confidence, 
                                     Description = description, 
                                     Result = result, 
                                     AuxData = auxData )
            reply
        with 
        | e ->
            VHubRecogResultHelper.FixedClassificationResult( "Failed to Deserialize Classification Result", "Exception in Reading" )

type VHubRecogRequestHelper() =  
    // Constant that uniquely identifies the codec format of RecogReply
    static member val RecogRequestCodecGuid = Guid( "49FEF817-52C5-4892-ADBF-90435A07568C" ) with get
    static member Pack( x:RecogRequest, ms: Stream ) = 
        ms.WriteBytesWVLen( x.Data ) 
        ms.WriteBytesWVLen( x.AuxData ) 

    static member Unpack( ms: Stream ) = 
        let data = ms.ReadBytesWVLen()  
        let auxdata = ms.ReadBytesWVLen()  
        RecogRequest( Data=data, AuxData=auxdata ) 

/// <summary> 
/// Information about the current provider and version
/// </summary>
[<AllowNullLiteral>]
type VHubAppInfo() = 
    /// <summary>
    /// Provider version, in a.b.c.d. Each of a, b, c, d should be from 0..255, and a should be from 0 to 255 the provider version is represented as an unsigned integer. 
    /// </summary>
    member val ProviderVersion = 0u with get, set
    member x.VersionString with get() = let arr = System.BitConverter.GetBytes(x.ProviderVersion) |> Array.rev
                                        let addr = arr |> Array.map ( fun b -> b.ToString() ) |> String.concat "."
                                        addr.ToString()
                            and set( str: string ) = let arr = str.Split([|'.'|]) |> Array.map( System.Byte.Parse ) |> Array.rev
                                                     x.ProviderVersion <- System.BitConverter.ToUInt32( arr, 0 )
    member val HostName = "" with get, set
    member val ProviderID = Guid.Empty with get, set
    static member Pack( x:VHubAppInfo, ms: Stream ) = 
        ms.WriteBytes( x.ProviderID.ToByteArray() )
        ms.WriteUInt32( x.ProviderVersion ) 
        ms.WriteStringV( x.HostName ) 
        ms
    static member Unpack( ms: Stream ) = 
        let buf = Array.zeroCreate<_> 16
        ms.ReadBytes( buf ) |> ignore
        let providerID = Guid( buf ) 
        let providerVersion = ms.ReadUInt32()
        let hostName = ms.ReadStringV()
        VHubAppInfo( ProviderID = providerID, ProviderVersion=providerVersion, HostName = hostName )




