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
        interops.fs
  
    Description: 
        Unmanaged DLL interops

    Author:																	
        Jin Li, Partner Research Manager
        Sanjeev Mehrotra, Princial Architect
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.Runtime.InteropServices
open System.Diagnostics
open Microsoft.FSharp.NativeInterop

#nowarn "9"

/// Hold Interop Win32 system call 
module internal InteropWithKernel32 =
    /// Creates a symbolic link.
    /// see https://msdn.microsoft.com/en-us/library/windows/desktop/aa363866(v=vs.85).aspx
    [<DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError=true)>]
    extern bool CreateSymbolicLink(
        string lpSymlinkFileName,
        string lpTargetFileName,
        uint32 dwFlags
    )

    /// Establishes a hard link between an existing file and a new file. This function is only supported on the NTFS file system, and only for files, not directories.
    /// The two files will be considered the same once linked. 
    /// see https://msdn.microsoft.com/en-us/library/windows/desktop/aa363860(v=vs.85).aspx
    [<DllImport("Kernel32.dll", CharSet = CharSet.Unicode, SetLastError=true)>]
    extern bool CreateHardLink(
        string lpFileName,
        string lpExistingFileName,
        IntPtr lpSecurityAttributes
    )


/// Hold Interop Win32 system call related to JobObject
module internal InteropWithJobObject =
    [<type:StructLayout(LayoutKind.Sequential)>]
    type SECURITY_ATTRIBUTES =
        struct
            val mutable nLength: uint32
            val mutable pSecurityDescriptor : IntPtr
            val mutable bInheritHandle : int32
        end

    [<type:StructLayout(LayoutKind.Sequential)>]
    type JOBOBJECT_BASIC_LIMIT_INFORMATION =
        struct 
            val mutable PerProcessUserTimeLimit : Int64
            val mutable PerJobUserTimeLimit : Int64
            val mutable LimitFlags : uint32
            val mutable MinimumWorkingSetSize : IntPtr
            val mutable MaximumWorkingSetSize : IntPtr
            val mutable ActiveProcessLimit : uint32
            val mutable Affinity : IntPtr
            val mutable PriorityClass : uint32
            val mutable SchedulingClass : uint32
        end

    [<type:StructLayout(LayoutKind.Sequential)>]
    type IO_COUNTERS =
        struct
            val mutable ReadOperationCount : UInt64
            val mutable WriteOperationCount : UInt64
            val mutable OtherOperationCount : UInt64
            val mutable ReadTransferCount : UInt64
            val mutable WriteTransferCount : UInt64
            val mutable OtherTransferCount : UInt64
        end

    [<type:StructLayout(LayoutKind.Sequential)>]
    type JOBOBJECT_EXTENDED_LIMIT_INFORMATION =
        struct
            val mutable BasicLimitInformation : JOBOBJECT_BASIC_LIMIT_INFORMATION
            val mutable IoInfo : IO_COUNTERS
            val mutable ProcessMemoryLimit : IntPtr
            val mutable JobMemoryLimit : UIntPtr
            val mutable PeakProcessMemoryUsed : UIntPtr
            val mutable PeakJobMemoryUsed : UIntPtr
        end

    [<DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)>]
    extern IntPtr CreateJobObject(
        SECURITY_ATTRIBUTES *lpJobAttributes,
        string lpName
    )

    type JOBOBJECTINFOCLASS =
        | AssociateCompletionPortInformation = 7
        | BasicLimitInformation = 2
        | BasicUIRestrictions = 4
        | EndOfJobTimeInformation = 6
        | ExtendedLimitInformation = 9
        | SecurityLimitInformation = 5
        | GroupInformation = 11

    [<DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)>]
    extern bool SetInformationJobObject(
        IntPtr hJob,
        JOBOBJECTINFOCLASS JobObjectInfoClass,
        IntPtr lpJobObjectInfo,
        uint32 cbObjectInfoLength
    )

    [<DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)>]
    extern bool AssignProcessToJobObject(
        IntPtr job,
        IntPtr proc
    )

    [<DllImport("msvcrt.dll", CharSet=CharSet.Unicode, SetLastError=false)>]
    extern IntPtr memcpy(IntPtr dst, IntPtr src, IntPtr num)

    type LimitFlags =
         | JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008u
         | JOB_OBJECT_LIMIT_AFFINITY = 0x00000010u
         | JOB_OBJECT_LIMIT_BREAKAWAY_OK = 0x00000800u
         | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION = 0x00000400u
         | JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200u
         | JOB_OBJECT_LIMIT_JOB_TIME = 0x00000004u
         | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000u
         | JOB_OBJECT_LIMIT_PRESERVE_JOB_TIME = 0x00000040u
         | JOB_OBJECT_LIMIT_PRIORITY_CLASS = 0x00000020u
         | JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100u
         | JOB_OBJECT_LIMIT_PROCESS_TIME = 0x00000002u
         | JOB_OBJECT_LIMIT_SCHEDULING_CLASS = 0x00000080u
         | JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK = 0x00001000u
         | JOB_OBJECT_LIMIT_WORKINGSET = 0x00000001u

module internal InterProcess =
    open InteropWithKernel32
    open InteropWithJobObject

    let CreateJobObject() =
        let job = CreateJobObject(NativePtr.ofNativeInt(IntPtr.Zero), null)
        let mutable jobli = Unchecked.defaultof<JOBOBJECT_BASIC_LIMIT_INFORMATION>
        // see flags at: https://msdn.microsoft.com/en-us/library/windows/desktop/ms684147%28v=vs.85%29.aspx?f=255&MSPPError=-2147217396
        jobli.LimitFlags <- uint32(LimitFlags.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
        let mutable jobliex = Unchecked.defaultof<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>
        jobliex.BasicLimitInformation <- jobli
        let handle = GCHandle.Alloc(jobliex, GCHandleType.Pinned) // prevent memory movement
        let mutable res = SetInformationJobObject(job, JOBOBJECTINFOCLASS.ExtendedLimitInformation, handle.AddrOfPinnedObject(), uint32(sizeof<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>))
        handle.Free()
        if (res) then
            job
        else
            failwith (sprintf "SetInformationJobObject Fails with code %A" (Marshal.GetLastWin32Error()))
            IntPtr.Zero

    let CommonJob = if not Runtime.RunningOnMono then CreateJobObject() else IntPtr.Zero

    let StartChild(startInfo : ProcessStartInfo) =
        if (CommonJob <> IntPtr.Zero) then
            let proc = Process.Start(startInfo)
            let res = AssignProcessToJobObject(CommonJob, proc.Handle)
            if (not res) then
                proc.Kill()
                failwith (sprintf "AssignProcessToJobObject Fails with code %A" (Marshal.GetLastWin32Error()))
                null
            else
                proc
        else
            failwith (sprintf "No CommonJob object exists")
            null

/// <summary>
/// PerfADateTime is to used anyplace when you need to time the performance of code. DateTime.UtcNow and DateTime.UtcNow.Ticks doesn't have
/// good resolution, and sometime may cause a measurement error.  PerfADateTime use Stopwatch to provide accurate ticks and DateTime.UtcNow
/// for measurement. 
/// </summary>
module internal PerfADateTime = 
    let private startTicks = DateTime.UtcNow.Ticks
    let private toTicks = float TimeSpan.TicksPerSecond / float System.Diagnostics.Stopwatch.Frequency
    let private stopWatch = 
        let wat = System.Diagnostics.Stopwatch.StartNew()
        wat.Start()
        wat

    // UtcNow can not be inlined, as it may bind startTick and stopWatch, which caused wrong behavior when the function is remoted. 
    /// Equivalent to DateTime.UtcNow, but with a higher resolution clock. 
    let UtcNow() = 
        DateTime( (int64) (float stopWatch.ElapsedTicks * toTicks) + startTicks )

    // UtcNowTicks can not be inlined, as it may bind startTick and stopWatch, which caused wrong behavior when the function is remoted. 
    /// Equivalent to DateTime.UtcNow.Ticks, but with a higher resolution clock. 
    let UtcNowTicks() = 
        (int64) (float stopWatch.ElapsedTicks * toTicks) + startTicks

/// <summary>
/// PerfADateTime maps to DateTime (for UtcNow and UtcNowTicks). 
/// </summary>
module internal PerfDateTime =        
    /// <summary> DateTime.UtcNow, used for purpose of possible change to a higher resolution clock if need.  </summary>
    let UtcNow() = 
        DateTime.UtcNow
    /// <summary> DateTime.UtcNow.Ticks, used for purpose of possible change to a higher resolution clock if need.  </summary>
    let UtcNowTicks() = 
        DateTime.UtcNow.Ticks

(*
module internal PerfNativeDateTime = 
    [<DllImport("Kernel32.dll", CallingConvention = CallingConvention.Cdecl)>]
    extern bool QueryPerformanceCounter( int64 *lpCounter)

    [<DllImport("Kernel32.dll", CallingConvention = CallingConvention.Cdecl)>]
    extern bool QueryPerformanceFrequency( int64 *lpCounter)

    let private frequency = 
        let freqPtr = NativePtr.stackalloc<int64>( 1 )
        QueryPerformanceFrequency( freqPtr ) |> ignore
        NativePtr.get freqPtr 0

    let private startTicks = DateTime.UtcNow.Ticks
    let private startCount = 
        let counterPtr = NativePtr.stackalloc<int64>( 1 )
        QueryPerformanceCounter( counterPtr ) |> ignore
        NativePtr.get counterPtr 0

    let inline private ElapseTicks() = 
        let counterPtr = NativePtr.stackalloc<int64>( 1 )
        QueryPerformanceCounter( counterPtr ) |> ignore
        let curCount = NativePtr.get counterPtr 0
        ( curCount - startCount ) * System.TimeSpan.TicksPerSecond / frequency
        
    // UtcNow can not be inlined, as it may bind frequency and startCount, which caused wrong behavior when the function is remoted. 
    let UtcNow() = 
        System.DateTime( ElapseTicks() + startTicks )
    // UtcNowTicks can not be inlined, as it may bind frequency and startCount, which caused wrong behavior when the function is remoted. 
    let UtcNowTicks() = 
        ElapseTicks() + startTicks
*)


