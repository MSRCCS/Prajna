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
		TabWithLog.fs
  
	Description: 
		A multi-tab window with OneTab being the current log file. 

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

/// A WPF window that display a large text file 
type ScrollFileWindow( getfilename, width, height) as x = 
    inherit StackPanel( Orientation = Orientation.Horizontal ) 
    let ScrollBarWidth = 30. 
    let fontSize = 14.
    let TextWindow = new TextBox( Background = new SolidColorBrush(Colors.White) ) 
    let mutable bVerticalScrollBarVisible = false
    let mutable bHorizontalScrollBarVisible = false
    let mutable bFirstTime = true
    let VerticalScrollValue = ref 0.
    let VerticalScrollBar = SingleCreation<_>()
    let inMonitoring = ref 0 
    let lines = List<_>()
    let sb = System.Text.StringBuilder()
    let mutable bException = false
    let endPosCollection = ConcurrentDictionary<string, _>(StringComparer.OrdinalIgnoreCase)
    let timerUpdate = SingleCreation<_>()
    let timerCleanup = SingleCreation<_>()
    let mutable ticksLastVisible = (PerfADateTime.UtcNowTicks())
    let mutable numLines = height / fontSize / 1.35
    do
        x.Width <- width
        x.Height <- height
        TextWindow.Text <- ""
        TextWindow.TextAlignment <- TextAlignment.Left
        TextWindow.TextWrapping <- TextWrapping.NoWrap
        x.IsVisibleChanged.Add( x.VisibleChangedCallback ) 
    member internal x.DoChangeSize( sizex, sizey ) = 
        x.Width <- sizex
        x.Height <- sizey
        numLines <- sizey / fontSize / 1.35
        x.DoScrollBarVisibility()
    member internal x.BeginRender() = 
        x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( x.DoScrollBarVisibility ) ) |> ignore
    member internal  x.NumLines with get() = numLines
    member internal  x.DoScrollBarVisibility() = 
        let width, height = x.Width, x.Height
        x.DoSetup()
    member internal x.DoSetupFirst() = 
        x.Children.Clear()
        TextWindow.Width <- if bVerticalScrollBarVisible then x.Width - GridWithScrollBar.ScrollBarWidth else x.Width
        TextWindow.Height <- if bHorizontalScrollBarVisible then x.Height - GridWithScrollBar.ScrollBarWidth else x.Height
        TextWindow.FontSize <- fontSize
        TextWindow.FontWeight <- FontWeights.Normal
        TextWindow.Foreground <- new SolidColorBrush( Colors.Black) 
        x.Children.Add( TextWindow ) |> ignore
        bVerticalScrollBarVisible <- x.NumLines < float lines.Count
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Setup Window for Monitor file %s Vertical visible is %A" (getfilename()) bVerticalScrollBarVisible  ))
        if bVerticalScrollBarVisible then 
            let vBar = VerticalScrollBar.Create( fun _ ->   let bar = new Primitives.ScrollBar ( Width = ScrollBarWidth, Height = x.Height - ScrollBarWidth , Orientation = Orientation.Vertical  )
                                                            bar.Height <- x.Height 
                                                            bar.Maximum <- Math.Max( float lines.Count - x.NumLines, 0. )
                                                            bar.LargeChange <- x.NumLines
                                                            bar.SmallChange <- 1.0
                                                            bar.Value <- !VerticalScrollValue
                                                            bar.Scroll.Add( x.ScrollVerticalBar )
                                                            bar.ValueChanged.Add( x.ValueChangedVerticalBar )
                                                            x.Children.Add( bar ) |> ignore
                                                            bar )
            let th = vBar.Track
            if Utils.IsNotNull th then 
                th.Thumb.Background <- Brushes.Blue
                th.Thumb.Height <- x.NumLines / vBar.Maximum * x.Height
                th.Thumb.Width <- ScrollBarWidth
                th.ViewportSize <- x.Height
            ()

        x.UpdateContent() 
    member internal x.DoSetup() = 
        let bVerticalVisible = x.NumLines < float lines.Count
        let bChange = ( bVerticalVisible <> bVerticalScrollBarVisible ) || bFirstTime
        bFirstTime <- false
        bVerticalScrollBarVisible <- bVerticalVisible
        if bChange then 
            x.DoSetupFirst() 
        else
            if bVerticalScrollBarVisible then 
                let vBar = VerticalScrollBar.Object() 
                vBar.Height <- x.Height - ScrollBarWidth
                vBar.Maximum <- Math.Max( float lines.Count - x.NumLines , 0. )
                vBar.LargeChange <- x.NumLines
                vBar.SmallChange <- 1.0
                vBar.Value <- !VerticalScrollValue
                let th = vBar.Track
                if Utils.IsNotNull th then 
                    th.Thumb.Background <- Brushes.Blue
                    th.Thumb.Height <- x.NumLines / vBar.Maximum * x.Height
                    th.Thumb.Width <- ScrollBarWidth
                    th.ViewportSize <- x.Height
        TextWindow.Width <- if bVerticalScrollBarVisible then x.Width - GridWithScrollBar.ScrollBarWidth else x.Width
        TextWindow.Height <- if bHorizontalScrollBarVisible then x.Height - GridWithScrollBar.ScrollBarWidth else x.Height
        x.UpdateContent() 

    member internal x.ScrollVerticalBar e = 
        x.VerticalScrollTo e.NewValue
    member internal x.ValueChangedVerticalBar e = 
        x.VerticalScrollTo e.NewValue
    member internal x.VerticalScrollTo value = 
        let oldvalue = !VerticalScrollValue
        if value <> oldvalue && Interlocked.CompareExchange( VerticalScrollValue, value, oldvalue )=oldvalue then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Vertical scroll to %f" value ))
            x.UpdateContent() 
    member internal x.UpdateContent() = 
        if lines.Count > 0 then 
            let numLines = int x.NumLines
            let s0 = int !VerticalScrollValue
            let startLines = 
                if s0<=0 then 
                    0 
                elif s0 + numLines > lines.Count then 
                    lines.Count - numLines 
                else 
                    s0
            let renderLines = Math.Min( lines.Count - startLines, numLines ) 
            let pos, _ = lines.[startLines]
            let endpos = 
                if startLines + renderLines = lines.Count then
                    sb.Length
                else
                    fst lines.[startLines + renderLines]
            TextWindow.Text <- sb.ToString(pos, endpos - pos )
        else
            TextWindow.Text <- ""
        TextWindow.InvalidateVisual() 
    static member val internal UpdateIntervalInMilliseconds = 500 with get, set
    static member val internal ClearUpIntervalInMilliseconds = 10000 with get, set
    member internal x.VisibleChangedCallback e = 
        if x.IsVisible then 
            x.StartUpdate() 
        else
            x.StopUpdate() 
    member internal x.StartUpdate() = 
        if x.IsVisible then 
            /// Garantee only one timer is created. 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start Update file %s ..... " (getfilename()) ) )
            if not bException then 
                timerUpdate.Create ( fun _ -> ThreadPoolTimer.TimerWait (fun _ -> sprintf "Timer to update monitor file %s" (getfilename()) ) (x.UpdateFile) 0 ScrollFileWindow.UpdateIntervalInMilliseconds ) |> ignore 
            timerCleanup.Destroy( fun (timer:ThreadPoolTimer) -> timer.Cancel() )
        else
            x.StopUpdate() 
    member internal x.StopUpdate() = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Stop Update %s ..... " (getfilename()) ) )
        timerUpdate.Destroy( fun timer -> timer.Cancel() )
        if not bException then 
            timerCleanup.Create( fun _ -> ThreadPoolTimer.TimerWait (fun _ -> sprintf "Timer to clean up monitor file %s" (getfilename()) ) (x.Cleanup) ScrollFileWindow.ClearUpIntervalInMilliseconds -1 ) |> ignore 
        else
            x.Cleanup() 
    member internal x.Cleanup() = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Cleanup monitor file %s called" (getfilename()) ) )
        sb.Clear() |> ignore 
        endPosCollection.Clear()
    member internal x.UpdateFile() = 
        if x.IsVisible then 
            if Interlocked.CompareExchange( inMonitoring, 1, 0 ) = 0 then 
                ticksLastVisible <- (PerfADateTime.UtcNowTicks())
                let bAtEnd = !VerticalScrollValue + x.NumLines + 10. >= float lines.Count
                /// Monitoring
                try
                    let filename = (getfilename())
                    let endPosRef = endPosCollection.GetOrAdd( filename, ref 0L)
                    if !endPosRef = 0L then 
                        // new file being monitored
                        lines.Clear() |> ignore
                        sb.Clear() |> ignore 
                        ()
                    use logF = new FileStream( filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                    let oldEnd = !endPosRef 
                    let posEnd = logF.Seek( 0L, SeekOrigin.End ) 
                    if posEnd > oldEnd then 
                        /// Read in additional content 
                        if Interlocked.CompareExchange( endPosRef, posEnd, oldEnd) = oldEnd then 
                            logF.Seek( oldEnd, SeekOrigin.Begin ) |> ignore 
                            let arr = Array.zeroCreate<_> (int ( posEnd - oldEnd ))
                            let readLen = logF.Read( arr, 0, arr.Length ) 
                            if readLen < arr.Length then 
                                endPosRef := posEnd + int64 readLen    
                            let readStr = System.Text.Encoding.UTF8.GetString( arr, 0, readLen ) 
                            let posSB = sb.Length
                            sb.Append( readStr ) |> ignore 
                            let mutable lastLine = 0
                            while lastLine < readStr.Length do 
                                let nextLine = readStr.IndexOfAny( Environment.NewLine.ToCharArray(), lastLine ) 
                                if nextLine = lastLine then 
                                    // first char is a new line
                                    lastLine <- lastLine + 1 // Skip
                                elif nextLine > lastLine then 
                                    lines.Add( posSB + lastLine, nextLine - lastLine )
                                    lastLine <- nextLine + 1
                                else 
                                    // reaching end 
                                    lines.Add( posSB + lastLine, readStr.Length - lastLine )
                                    lastLine <- readStr.Length
                            if bAtEnd then 
                                VerticalScrollValue := Math.Max( 0.0, float lines.Count - x.NumLines )
                        x.BeginRender()
                with 
                | e -> 
                    bException <- true 
                    x.Cleanup()
                    sb.Append( sprintf "ScrollFileWindow error in monitoring file %s with exception %A" (getfilename()) e ) |> ignore                 
                inMonitoring := 0 
        else
            x.StopUpdate() 

/// The collection of UI include a set of tabbed window and a trace log window 
type TabWindowWithLog( sizex, sizey ) as x = 
    inherit Canvas( ) 
    let mutable activeTab = 0
    let tabs = new GeneralTab( Width=sizex, Height=sizey - 10. )
    // timer for updating frame, 1 frame every 25ms
//    let timerUpdate = new Threading.DispatcherTimer(Interval = new TimeSpan(0,0,0,0,1000))
    let scrollTrace = ScrollFileWindow( Logger.GetLogFile, sizex - 50., sizey - 70. )
    do 
        x.Width <- sizex
        x.Height <- sizey
        tabs.AddTab( "Log", scrollTrace )
        x.Children.Add( tabs ) |> ignore
//        timerUpdate.Tick.Add(fun _ -> x.InvalidateVisual() )       
        tabs.SelectionChanged.Add( x.TabSelectionChanged )
    /// Change the size of the UI
    member x.ChangeSize( width, height ) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Window size changes to %f X %f ... " width height ))
        x.Width <- width
        x.Height <- height
        tabs.Width <- width
        tabs.Height <- height - 10.
        scrollTrace.DoChangeSize( width - 50., height - 70. )
        tabs.SetActive( activeTab, x.Width, x.Height ) 
    member internal x.TabSelectionChanged( e ) = 
        let tabs = e.Source :?> GeneralTab
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Tab %d is selected" tabs.SelectedIndex ))
        // let cv = tabs.Parent :?> ImageBrowserWindow
        e.Handled <- true
        x.ChangeSelection(tabs.SelectedIndex)
        ()     
    /// Add a tab with header.
    member x.AddTab( header, c ) =
        tabs.AddTab( header, c ) 
    /// Retrieve a certain tab
    member x.Tab( index ) = 
        tabs.Tab( index ) 
    member internal x.ChangeSelection( idx ) =                
        match idx with
        | 0 ->
            activeTab <- 0 
            scrollTrace.InvalidateVisual()
            scrollTrace.StartUpdate()
            Logger.Log( LogLevel.MildVerbose, ("Start showing log."))
        | _ -> 
            activeTab <- idx
            tabs.SetActive( idx, x.Width, x.Height ) 
            let activeTab = x.Tab( idx )
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start showing %A." (activeTab.Header) ))
        tabs.InvalidateVisual()
        ()


