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
		WCFTools.fs
  
	Description: 
		A setup of helper routines for WCF Services. 

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Nov. 2014
	
 ---------------------------------------------------------------------------*)
namespace Prajna.WCFTools

open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.HttpBuilderHelper

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent

open Microsoft.FSharp.Control.CommonExtensions
open System
open System.Text
open System.Net
open System.Web
open System.Threading.Tasks
open System.Diagnostics
open System.ServiceModel
open System.ServiceModel.Web

/// Helper class for string to be used in browser. 
module SafeHttpBuilderHelper =
    /// Encodes a string to be displayed in a browser.  
    let inline WrapText (txt: string ) (sb: StringBuilder) =
        if Utils.IsNull txt then 
            sb
        else
            sb.Append( HttpUtility.HtmlEncode( txt ) )

type internal HttpTemplateSlot =
    | CopyHttpTextSlot of int * int
    | StringFuncSlot of int * int * char

type internal WCFToolsSetting() = 
    static member val ContentSlot  = @"<!--TOBE_FILLED_CONTENT" with get
    /// Need to be exact
    static member val ContentSlotTotal = @"<!--TOBE_FILLED_CONTENT?-->" with get

[<AllowNullLiteral>]
type internal HttpTemplate( pageTemplate: byte[] ) = 
    member val PageBuffer = pageTemplate with get
    member val PageTemplate = System.Text.Encoding.UTF8.GetString( pageTemplate ) with get
    member val PlaceHolderPosition = ConcurrentDictionary<_,_>() with get
    member val internal Slots = List<_>(4) with get
    member x.Analyze() = 
        let slotPos = WCFToolsSetting.ContentSlot.Length
        let slotLen = WCFToolsSetting.ContentSlotTotal.Length
        let templateLen = x.PageTemplate.Length
        let mutable bDone = false
        let mutable lastPos = 0
        while not bDone do 
            let idx = x.PageTemplate.IndexOf( WCFToolsSetting.ContentSlot, lastPos, StringComparison.OrdinalIgnoreCase )
            if idx < 0 || idx + slotLen >= templateLen then 
                bDone <- true
            else
                x.Slots.Add ( CopyHttpTextSlot (lastPos, idx - lastPos ) )
                x.Slots.Add ( StringFuncSlot (idx, slotLen, x.PageTemplate.[ idx + slotPos ] ) )
                lastPos <- idx + slotLen
        x.Slots.Add( CopyHttpTextSlot (lastPos, templateLen - lastPos ) )

type internal HttpTemplateStore() = 
    member val internal Store = ConcurrentBag<HttpTemplate>() with get
    member internal x.FindTemplate( pageTemplate: byte[] ) = 
        let mutable returnStore = null
        let mutable bFind = false
        let en = x.Store.GetEnumerator()
        while not bFind && en.MoveNext() do
            let page = en.Current
            if Object.ReferenceEquals( page.PageBuffer, pageTemplate ) then 
                bFind <- true
                returnStore <- page
        returnStore
    member internal x.GetOrAdd( pageTemplate: byte[] ) = 
        let returnStore = x.FindTemplate( pageTemplate ) 
        if Utils.IsNull returnStore then 
            lock ( pageTemplate ) ( fun _ -> 
                let returnStore = x.FindTemplate( pageTemplate ) 
                if Utils.IsNull returnStore then 
                    let newStore = HttpTemplate( pageTemplate ) 
                    newStore.Analyze() 
                    x.Store.Add( newStore ) 
                    newStore
                else
                    returnStore )
        else
            returnStore


/// Helper class to form a HTML page 
type HttpTools internal () = 
    /// Convert a plain string to a HTML page represented by stream, ready to be served in WCF  
    static member PlainTextToHttpStream (content:string ) = 
        let webText = sprintf """<!DOCTYPE html><html lang="en-US"><header></header><body>%s</body></html>""" (HttpUtility.HtmlEncode(content).Replace( Environment.NewLine, "<br>" +  Environment.NewLine ))
        let st = new MemoryStream( System.Text.Encoding.UTF8.GetBytes(webText) ) :> Stream
        let ctxOutgoing = WebOperationContext.Current.OutgoingResponse
        ctxOutgoing.ContentType <- "text/html"
        ctxOutgoing.StatusCode <- System.Net.HttpStatusCode.OK
        ctxOutgoing.Headers.Add( "Cache-Control", "no-cache" )
        ctxOutgoing.Headers.Add( "Pragma", "no-cache" )
        st
    
    /// Convert a plain string to a HTML page represented by stream, ready to be served in WCF 
    static member PlainTextToHttpStream ( ctx: WebOperationContext, content:string ) = 
        let webText = sprintf """<!DOCTYPE html><html lang="en-US"><header></header><body>%s</body></html>""" (HttpUtility.HtmlEncode(content).Replace( Environment.NewLine, "<br>" +  Environment.NewLine ))
        let st = new MemoryStream( System.Text.Encoding.UTF8.GetBytes( webText) ) :> Stream    
        ctx.OutgoingResponse.ContentType <- "text/html"
        ctx.OutgoingResponse.StatusCode <- System.Net.HttpStatusCode.OK
        ctx.OutgoingResponse.Headers.Add( "Cache-Control", "no-cache" )
        ctx.OutgoingResponse.Headers.Add( "Pragma", "no-cache" )
        st

    /// Convert content of specific metadata type to a HTML page represented by stream, ready to be served in WCF 
    static member ContentToHttpStreamWithMetadata ( ctx: WebOperationContext, content:byte[], metaName: string ) = 
        if Utils.IsNull content then 
            HttpTools.PlainTextToHttpStream( ctx, (sprintf "<h1> The requested content does not exist </h1>") )
        else
            let st = new MemoryStream( content, 0, content.Length, false, true ) :> Stream    
            ctx.OutgoingResponse.ContentType <- metaName
            ctx.OutgoingResponse.StatusCode <- System.Net.HttpStatusCode.OK
            st

    /// Convert content of specific metadata type (via extension of the content name) to a HTML page represented by stream, ready to be served in WCF 
    static member ContentToHttpStreamWithContentName ( ctx: WebOperationContext, content:byte[], fname: string ) = 
        if Utils.IsNull content then 
            HttpTools.PlainTextToHttpStream( ctx, (sprintf "<h1> The requested content %s does not exist </h1>" fname) )
        else
            let bImage, meta = WebCache.GetFileType fname
            let st = new MemoryStream( content, 0, content.Length, false, true ) :> Stream    
            ctx.OutgoingResponse.ContentType <- meta
            ctx.OutgoingResponse.StatusCode <- System.Net.HttpStatusCode.OK
            st

    /// Convert content returned by an async task of specific metadata type (via extension of the content name) 
    /// to a HTML page represented by Task<Stream>, ready to be served in WCF 
    static member AsyncContentToHttpStream ( ctx: WebOperationContext, contentTask:Task<byte[]>, fname: string ) = 
        let retrunTask ctx (inpTask:Task<byte[]>) = 
            HttpTools.ContentToHttpStreamWithContentName( ctx, inpTask.Result, fname )    
        contentTask.ContinueWith( retrunTask ctx )
    
    /// Convert file to a HTML page represented by Stream, ready to be served in WCF 
    static member FileToWebStreamSync (fname:string) = 
        let bytRead = 
            if Utils.IsNotNull fname then 
                try 
                    use fstream = new FileStream( fname, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                    let flen = int fstream.Length
                    let content = Array.zeroCreate<_> flen
                    let lenRead = fstream.Read( content, 0, flen ) 
                    fstream.Close()
                    content
                with 
                | e -> 
                    null
            else
                null
        HttpTools.ContentToHttpStreamWithContentName( WebOperationContext.Current, bytRead, fname ) 

    /// Convert a plaintext Task<string> to a HTML page represented by Task<Stream>, ready to be served in WCF 
    static member PlainTextToWebPage (ctx: WebOperationContext, logTask: Task<string> ) =
        let logReturnTask (logTask: Task<string> ) = 
            HttpTools.PlainTextToHttpStream( ctx, logTask.Result ) 
        let task2 = logTask.ContinueWith( logReturnTask ) 
        task2

    /// Using async API, Convert a file to a HTML page represented by Task<Stream>, ready to be served in WCF 
    static member FileToWebStreamAsync ( ctx: WebOperationContext, fname:string) = 
        let content = WebCache.TaskReadFileToBytes( fname )
        HttpTools.AsyncContentToHttpStream( ctx, content, fname )

    /// Using a plaintext string to a HTML page represented by Task<Stream>, ready to be served in WCF 
    static member PlainTextToWebPageTask ( s: string ) =
        new Task<_>( fun _ -> HttpTools.PlainTextToHttpStream( s ) )


(*
    Non working experiments
    
    static member StringToHttpResponse (content:string ) = 
        let contentArr = System.Text.Encoding.UTF8.GetBytes(content )
        let buf = Array.zeroCreate<_> ( contentArr.Length + 1000 )
        let ms = new MemoryStream(buf, 0, buf.Length, true, true) 
        use tw = new StreamWriter( ms ) 
        let resp = new HttpResponse( tw ) 
        resp.BinaryWrite( contentArr ) 
        tw.Close()
        // ms.Seek( 0L, SeekOrigin.Begin ) |> ignore
        let outputstream = new MemoryStream( buf, 0, int ms.Length, false, true )
        outputstream :> Stream
        let ctx = HttpContext.Current.Response
        ctx.BinaryWrite( System.Text.Encoding.UTF8.GetBytes(content ) )
        ctx.OutputStream
//        let ms = new MemoryStream()
//        let tw = new StreamWriter( ms ) 
//        let resp = new HttpResponse( tw, BufferOutput = true ) 
//        resp.BinaryWrite( System.Text.Encoding.UTF8.GetBytes(content ) ) 
//        resp.OutputStream 
    static member FileToHttpResponse (fname:string) = 
        let strRead = 
            if Utils.IsNotNull fname then 
                try 
                    use logF = new FileStream( fname, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                    use readStream = new StreamReader( logF ) 
                    let recentReadStr = readStream.ReadToEnd()
                    readStream.Close()
                    logF.Close()
                    recentReadStr
                with 
                | e -> 
                    let msg = sprintf "Error in reading file %s: %A" fname e
                    msg
            else
                "No Input Filename"
        HttpTools.StringToHttpResponse( strRead )
*)

/// Helper class to build HTML page based on template for WCF services. 
type HttpTemplateBuilder internal () = 
    static member val internal Current = HttpTemplateStore() with get
    /// Generate HTML page based on a page template, 
    /// and converts tags via functional delegate installed.         
    static member GenerateHttpPage( ctx: WebOperationContext, pageTemplate: byte[], funcStore: Dictionary<_,StringBuilder->StringBuilder > ) = 
        let pg = HttpTemplateBuilder.Current.GetOrAdd( pageTemplate ) 
        let sb = StringBuilder()
        for slot in pg.Slots do 
            match slot with 
            | CopyHttpTextSlot ( pos, len ) -> 
                sb.Append( pg.PageTemplate, pos, len ) |> ignore
            | StringFuncSlot (pos, len, c ) -> 
                let funcRef = ref Unchecked.defaultof<_>
                if funcStore.TryGetValue( c, funcRef ) then 
                    (!funcRef)(sb) |> ignore
                else
                    sb.Append( pg.PageTemplate, pos, len ) |> ignore
        HttpTools.ContentToHttpStreamWithMetadata( ctx, System.Text.Encoding.UTF8.GetBytes( sb.ToString() ), "text/html" )

    /// <summary>
    /// Form a HTML table, where content is the template. 
    /// We will replace caption and tableinfo. 
    /// Please note that both caption and tableinfo has not passed through HttpUtility.HtmlEncode. 
    /// </summary>
    static member ServeTable( ctx, content: byte[], captionPlaceHolder: char, captionFunc: StringBuilder->StringBuilder, tablePlaceHolder: char, tableSeqFunc: seq<seq<StringBuilder -> StringBuilder>> ) = 
        if Utils.IsNull content then 
            HttpTools.PlainTextToHttpStream( ctx, (sprintf "Content template does not exist" ) )
        else
            let tableElem (sb: StringBuilder ) ( elemFunc: StringBuilder -> StringBuilder ) =
                WrapTableItem (elemFunc) sb
            let tableRow (sb: StringBuilder ) (innerSeq:seq<StringBuilder->StringBuilder>)  = 
                WrapTableRow ( fun sb1 -> innerSeq |> Seq.fold ( tableElem ) sb1 ) sb
//                innerSeq  |> Seq.map HttpTools.WrapTableItem |> String.concat "" )
            let tableBuildingFunc (sb:StringBuilder) = 
                tableSeqFunc |> Seq.fold( tableRow  ) sb
            let dic = Dictionary<_,_>(2)
            dic.Add( captionPlaceHolder, captionFunc )
            dic.Add( tablePlaceHolder, tableBuildingFunc ) 
            HttpTemplateBuilder.GenerateHttpPage( ctx, content, dic )
    /// <summary>
    /// Serve HTML dynamic content through a template
    /// Please note that the content will not through HttpUtility.HtmlEncode. 
    /// </summary>
    static member ServeContent( ctx, content: byte[], placeHolder: char, info:StringBuilder -> StringBuilder ) = 
        if Utils.IsNull content then 
            HttpTools.PlainTextToHttpStream( ctx, (sprintf "Content template does not exist" ) )
        else    
            let dic = Dictionary<_,_>(1)
            dic.Add( placeHolder, info ) 
            HttpTemplateBuilder.GenerateHttpPage( ctx, content, dic )
