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
		HttpBuilder.fs
  
	Description: 
		HttpBuilder helper function

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Nov. 2014
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.Text
open System.Web

/// Helper function to form a properly formatted HTTP page 
module HttpBuilderHelper =
    /// Append a properly formatted HTML string to a HTML page
    let inline WrapRaw (txt: string ) (sb: StringBuilder)  =
        sb.Append( txt )
    /// Append a table row to Http page, where function s(sb) generates a HTML table row
    let inline WrapTableRow ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder) = 
        let b1 = sb.Append( "<tr>" ).Append( Environment.NewLine ) 
        (s b1 ).Append( "</tr>" ).Append( Environment.NewLine ) 
    /// Append a table column to Http page, where function s(sb) generates a HTML table column
    let inline WrapTableItem ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder) = 
        let b1 = sb.Append("  <td>")
        ( s b1 ).Append( "</td>" ).Append( Environment.NewLine )
    /// Append a properly formatted HTML link to a HTML page. Function link(sb) appends href link, and function s(sb) appends link text. 
    let inline FormLink( link: StringBuilder -> StringBuilder ) ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder ) = 
        let b1 = sb.Append( "<a href=\"")
        let b2 = ( link b1).Append( "\"> ")
        ( s b2).Append( "</a>" )
    /// Append a properly formatted HTML paragraph with ID, function id(sb) appends ID string, and function info(sb) appends paragraph text. 
    let inline WrapParagraphWithID( id: StringBuilder -> StringBuilder ) ( info: StringBuilder -> StringBuilder ) ( sb:StringBuilder) = 
            let b1 = sb.Append( "<p id=\"")
            let b2 = ( id b1).Append("\"> ")
            ( info b2).Append(" </p>").Append( Environment.NewLine )
    /// Append a properly formatted HTML paragraph, with function info(sb) appends paragraph text. 
    let inline WrapParagraph ( info: StringBuilder -> StringBuilder ) ( sb:StringBuilder) = 
            let b1 = sb.Append( @"<p>" )
            ( info b1).Append("</p>").Append( Environment.NewLine )
    /// Append an image with URL to HTML paragraph, with width and height defined the size of included image, uri locates the image link, and al_text is the alternative text of the image. 
    let inline WrapImageURI (width:int) (height:int) (uri:string) (alt_text:string ) (sb:StringBuilder) = 
        let sb1 = sb.Append( "<img src=\"" ).Append( uri ) 
        let sb2 = sb1.Append( "\" alt=\"").Append( alt_text ).Append("\" " )
        let sb3 = if width > 0 then sb2.Append( "width=" ).Append( width.ToString() ) else sb2
        let sb4 = if height > 0 then sb3.Append( "height=" ).Append( height.ToString() ) else sb3
        sb4.Append( ">" )


//    static member inline WrapText (txt: string ) (sb: StringBuilder)  =
//        sb.Append( HttpUtility.HtmlEncode( txt ) )
//    static member inline WrapRaw (txt: string ) (sb: StringBuilder)  =
//        sb.Append( txt )
//    static member inline WrapTableRow ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder) = 
//        let b1 = sb.Append( "<tr>" ).Append( Environment.NewLine ) 
//        (s b1 ).Append( "</tr>" ).Append( Environment.NewLine ) 
//    static member inline WrapTableItem ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder) = 
//        let b1 = sb.Append("  <td>")
//        ( s b1 ).Append( "</td>" ).Append( Environment.NewLine )
//    static member inline FormLink( link: StringBuilder -> StringBuilder ) ( s: StringBuilder -> StringBuilder ) ( sb: StringBuilder ) = 
//        let b1 = sb.Append( "<a href=\"")
//        let b2 = ( link b1).Append( "\"> ")
//        ( s b2).Append( "</a>" )
//    static member inline WrapParagraph( id: StringBuilder -> StringBuilder ) ( info: StringBuilder -> StringBuilder ) ( sb:StringBuilder) = 
//        if Utils.IsNull id then 
//            let b1 = sb.Append( @"<p>" )
//            ( info b1).Append("</p>").Append( Environment.NewLine )
//        else
//            let b1 = sb.Append( "<p id=\"")
//            let b2 = ( id b1).Append("\"> ")
//            ( info b2).Append(" </p>").Append( Environment.NewLine )
