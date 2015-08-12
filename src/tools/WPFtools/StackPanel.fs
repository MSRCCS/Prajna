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
		StackPanel.fs
  
	Description: 
		Support Stack Panel (usually for Menu)

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

    Date:
        Feb. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.WPFTools

open System
open System.Threading
open System.Linq
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp

open System.ComponentModel
open System.IO
open System.Windows
open System.Windows.Data
open System.Windows.Media
open System.Windows.Controls
open System.Windows.Threading

/// A window UI with a menu
type WindowWithMenu( menu, content, sizex, sizey ) as x = 
//    inherit DockPanel( Width = sizex, Height = sizey, LastChildFill=true ) 
//    do 
//        x.Children.Add( menu ) |> ignore
//        DockPanel.SetDock( menu, Dock.Top ) 
//        x.Children.Add( content ) |> ignore
    inherit StackPanel( Width = sizex, Height = sizey, Orientation=Orientation.Vertical ) 
    do 
        x.Children.Add( menu ) |> ignore
        x.Children.Add( content ) |> ignore
    /// Change the size of the window
    member x.ChangeSize( width, height ) = 
        x.Width <- width
        x.Height <- height
