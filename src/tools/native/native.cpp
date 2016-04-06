// This is the main DLL file.

#include "stdafx.h"

#include "native.h"

// ====================================================

#pragma managed

using namespace System;
using namespace System::IO;
using namespace System::Reflection;
using namespace System::Threading;
using namespace System::Collections::Generic;
using namespace System::Collections::Concurrent;
using namespace System::Runtime::InteropServices;
using namespace Microsoft::Win32::SafeHandles;

namespace Prajna 
{
    namespace Tools 
    {
        namespace Test 
        {
            // In case class does not have destructor, and it does not derive from base class which is IDisposable, then it is not IDisposable
            // In case class has destructor or it derives from base class which is IDisposable, then it is IDisposable
            // 1. In case it has dtor and does not derive from base class which is IDisposable, then
            //    it implements IDisposable::Dispose automatically, and implements "virtual Dispose(bool disposing) method"
            // 2. In case it has dtor and derives from base class which is IDisposable, then
            //    it does not implement IDisposable::Dispose, it only implements "override Dispose(bool disposing) method" which automatically calls base.Dispose
            public ref class TestDispose : public IDisposable
            {
            private:
                int a;
                byte *buf;
            public:
                TestDispose()
                {
                    a = 4;
                    buf = new byte[100];
                }
                !TestDispose()
                {
                    delete[] buf;
                }
                ~TestDispose()
                {
                    delete[] buf;
                }
            };

            // Any CLI class wtih destructor, dtor (i.e. ~ method) automatically becomes IDisposable
            // Following code is then automatically generated
            // [HandleProcessCorruptedStateExceptions]
            //protected override void Dispose([MarshalAs(UnmanagedType.U1)] bool flag1)
            //{
            //    if (flag1)
            //    {
            //        try
            //            this.~CppDispose();
            //        finally
            //            base.Dispose(true);
            //            GC::SuppressFinalize(this);
            //    }
            //    else
            //    {
            //        try
            //            this.!CppDispose();
            //        finally
            //            base.Dispose(false);
            //    }
            //}
            //
            //protected override void Finalize()
            //{
            //    this.Dispose(false);
            //}
            //
            // In addition, if and only if there is no base class in the hierarchy which is IDisposable, the class implements IDisposable and following is added
            //virtual void Dispose(array<byte> ^buf, int offset, int cnt) = IDisposable::Dispose
            //{
            //    this.Dispose(true);
            //}
            // Therefore for CLI
            // 1. Never directly implement IDisposable
            // 2. Instead do following - the created dispose method will automatically have suppressfinalize call
            //    a) Create destructor to free managed resources
            //    ~Class()
            //    {
            //        ... free managed resources ... (stuff that goes inside "if bDisposing" block)
            //        ... call finalizer if it exists ...
            //    }
            //    !Class
            //    {
            //        ... free unmanaged resources ...
            //    }

            // In C#, the destructor is actually the finalizer and simply inserts following code:
            //protected override void Finalize()
            //{
            //    try
            //    {
            //        // Cleanup statements...
            //    }
            //    finally
            //    {
            //        base.Finalize();
            //    }
            //}
        }
    }
}

namespace Prajna {
    namespace Tools {
        namespace Native {

            // with null object, lock is removed
            ref class Lock {
                Object^ m_pObject;
            public:
                Lock(Object ^ pObject) : m_pObject(pObject) 
                {
                    if (nullptr != m_pObject)
                        Monitor::Enter(m_pObject);
                }
                ~Lock() 
                {
                    if (nullptr != m_pObject)
                        Monitor::Exit(m_pObject);
                }
            };

            ref class NativeHelper
            {
            private:
                generic <class T> static int SizeOf()
                {
                    //return sizeof(T::typeid);
                    return sizeof(T);
                }
            };

            // expand a function from Func<T,TResult> to Func<Object^,T,TResult>
            generic <class N, class T, class TResult> private ref class Func2to3
            {
            private:
                N m_val;
                Func<N, T, TResult> ^m_fn;

                TResult PerformFn(T x)
                {
                    return m_fn(m_val, x);
                }

            public:
                Func2to3(N val, Func<N, T, TResult> ^fn) : m_val(val), m_fn(fn) {}

                static Func<T, TResult>^ Init(N val, Func<N, T, TResult>^ fn)
                {
                    Func2to3 ^x = gcnew Func2to3(val, fn);
                    return gcnew Func<T, TResult>(x, &Func2to3::PerformFn);
                }
            };

            //===================================================================

            delegate void NativeIOCallback(int ioResult, __int64 polap, void *pState, void *pBuffer, int bytesTransferred);

            generic <class T> public delegate void IOCallbackDel(int ioResult, Object ^pState, array<T> ^pBuffer, int offset, int bytesTransferred);

            public interface class IIO
            {
            public:
                virtual void UpdateIO(IntPtr tpio, __int64 amt);
            };

            public ref class IOStateBase
            {
            protected:
                Dictionary<Int64, IOStateBase^> ^m_olapMap;
                GCHandle m_self;
                IntPtr m_selfPtr;
                IIO ^m_parent;
                IntPtr m_tpio;

                static void __clrcall Callback(int ioResult, __int64 polap, void *pState, void *pBuffer, int bytesTransferred)
                {
                    GCHandle ^pStateHandle = GCHandle::FromIntPtr(static_cast<IntPtr>(pState));
                    IOStateBase ^ioState = safe_cast<IOStateBase^>(pStateHandle->Target);
                    if (nullptr != ioState->m_olapMap)
                        ioState = ioState->m_olapMap[polap];
                    ioState->InvokeCb(ioResult, bytesTransferred);
                    ioState->Free();
                }

            public:
                IOStateBase() : m_olapMap(nullptr)
                {
                    m_self = GCHandle::Alloc(this);
                    m_selfPtr = GCHandle::ToIntPtr(m_self);
                }

                static IOStateBase^ CreateDictionary()
                {
                    IOStateBase ^x = gcnew IOStateBase();
                    x->m_olapMap = gcnew Dictionary<Int64, IOStateBase^>();
                    return x;
                }

                __forceinline void AddMap(Int64 key, IOStateBase ^ioState)
                {
                    m_olapMap[key] = ioState;
                }

                __forceinline void AddToMap(IOStateBase ^ioState)
                {
                    TPIO *tpio = (TPIO*)(ioState->m_tpio.ToPointer());
                    Int64 key = tpio->OverlappedPtr();
                    AddMap(key, ioState);
                }

                __forceinline void* SelfPtr()
                {
                    return m_selfPtr.ToPointer();
                }

                virtual void InvokeCb(int ioResult, int bytesTransferred)
                {
                    assert(false);
                }
                virtual void Free()
                {
                    assert(false);
                }
            };

            generic <class T> public ref class IOState : public IOStateBase
            {
            private:
                static int s_managedTypeSize;
                static bool s_staticInit = false;
                static Object ^s_initLock = gcnew Object();
                static GCHandle ^s_handleCallback;
                static CallbackFn *s_pfn;

                static void StaticInit()
                {
                    if (!s_staticInit)
                    {
                        Lock lock(s_initLock);
                        if (!s_staticInit)
                        {
                            //Type^ t = NativeHelper::typeid;q
                            //Object ^o = t->GetMethod("SizeOf", BindingFlags::Static | BindingFlags::NonPublic)
                            //    ->GetGenericMethodDefinition()
                            //    ->MakeGenericMethod(T::typeid)
                            //    ->Invoke(nullptr, nullptr);
                            //m_managedTypeSize = *(safe_cast<int^>(o));
                            //m_managedTypeSize = safe_cast<int>(o); // this will also work as unboxing is implicit in safe_cast
                            s_managedTypeSize = sizeof(T);

                            NativeIOCallback ^ncb = gcnew NativeIOCallback(IOStateBase::Callback); // create delegate from function
                            s_handleCallback = GCHandle::Alloc(ncb); // GCHandle to prevent garbage collection
                            IntPtr ip = Marshal::GetFunctionPointerForDelegate(ncb); // function pointer for the delgate
                            s_pfn = static_cast<CallbackFn*>(ip.ToPointer());

                            s_staticInit = true;
                        }
                    }
                }

            private:
                GCHandle ^m_handleBuffer;
                IOCallbackDel<T> ^m_cb;
                Object ^m_pState;
                array<T> ^m_buffer;
                int m_offset;

            public:
                static IOState()
                {
                    IOState<T>::StaticInit();
                }

                IOState() : IOStateBase()
                {
                    IOState<T>::StaticInit();
                }

                IntPtr Set(IntPtr ptr, Object ^state, array<T> ^buffer, int offset, IOCallbackDel<T> ^cb, IntPtr tpio, IIO ^parent)
                {
                    m_parent = parent;
                    m_pState = state;
                    m_buffer = buffer;
                    m_offset = offset;
                    m_cb = cb;
                    m_tpio = tpio;

                    if (IntPtr::Zero == ptr)
                    {
                        m_handleBuffer = GCHandle::Alloc(buffer, GCHandleType::Pinned); // must pin so unmanaged code can use it            
                        ptr = IntPtr::Add(m_handleBuffer->AddrOfPinnedObject(), m_offset*sizeof(T));
                    }
                    else
                    {
                        m_handleBuffer = nullptr;
                    }
                    return ptr;
                }

                virtual void InvokeCb(int ioResult, int bytesTransferred) new = IOStateBase::InvokeCb
                {
                    m_parent->UpdateIO(m_tpio, bytesTransferred);
                    m_cb->Invoke(ioResult, m_pState, m_buffer, m_offset, bytesTransferred);
                }

                virtual void Free() new = IOStateBase::Free
                {
                    m_self.Free();
                if (nullptr != m_handleBuffer)
                    m_handleBuffer->Free();
                }

                __forceinline static CallbackFn* CbFn()
                {
                    IOState<T>::StaticInit();
                    return s_pfn;
                }
                __forceinline static int TypeSize()
                {
                    IOState<T>::StaticInit();
                    return s_managedTypeSize;
                }
            };

            public ref class AsyncStreamIO : IIO, Stream
            {
            protected:
                IOCallback<true> *m_cb;
                TPIO **m_tpio;
                int m_numTPIO;
                BlockingCollection<IntPtr> ^m_tpioColl;
                //ConcurrentDictionary<Type^, Object^> ^m_cbFns;
                IOStateBase ^m_stateDictionary;
                Object ^m_ioLock;
                // Stream stuff
                bool m_canRead;
                bool m_canWrite;
                bool m_canSeek;
                bool m_bBufferless;
                Int64 m_length;
                Int64 m_position;
                Func<Type^, Object^> ^m_cbCreate;

                //generic <class T> static Object^ GetNewCbX(Object ^x, Type ^t)
                //{
                //    return gcnew IOCallbackClass<T>((IIO^)x);
                //}

                Void SetCb(void *cb)
                {
                    m_cb = (IOCallback<true>*) cb;
                }

                Object^ GetNewCb(Type ^t)
                {
                    Assembly ^a = Assembly::GetExecutingAssembly();
                    //array<Type^> ^tps = a->GetTypes();
                    Type ^cbClass = a->GetType("Prajna.Tools.Native.IOCallbackClass`1");
                    //FullName	"Prajna.Tools.Native.IOCallbackClass`1"	System::String^
                    array<Type^> ^arr = gcnew array<Type^>(1);
                    arr[0] = t;
                    Type ^gType = cbClass->MakeGenericType(arr);
                    array<Object^> ^ao = gcnew array<Object^>(1);
                    ao[0] = this;
                    return Activator::CreateInstance(gType, ao);
                    //return gcnew IOCallbackClass<T>((IIO^)this);
                }

                //generic <class T> IOCallbackClass<T>^ GetCbFn()
                //{
                //    //Func2to3<Object^,Type^,Object^> f(this, gcnew Func<Object^, Type^, Object^>(AsyncStreamIO::GetNewCbX<T>));
                //    //Func<Type^, Object^> ^getNew = gcnew Func2to3::Init(this, gcnew Func<Type^, Object^>(AsyncStreamIO::GetNewCbX));
                //    // unmanaged C++ lambda: [this](Type ^t) -> IOCallbackClass<T> { return gcnew IOCallbackClass<T>(this); }
                //    return (IOCallbackClass<T>^)m_cbFns->GetOrAdd(T::typeid, m_cbCreate);
                //}

                void virtual Free()
                {
                    Lock lock(this);
                    int i;

                    if (nullptr != m_cb)
                    {
                        m_cb->Close();
                        delete m_cb;
                        m_cb = nullptr;
                        if (nullptr != m_tpio)
                        {
                            for (i = 0; i < m_numTPIO; i++)
                                delete m_tpio[i];
                            delete[] m_tpio;
                        }
                    }
                    //ICollection<Object^>^ cb = (ICollection<Object^>^)m_cbFns;
                }

            public:
                AsyncStreamIO() : m_cb(nullptr)
                {
                    m_cbCreate = gcnew Func<Type^, Object^>(this, &AsyncStreamIO::GetNewCb);

                    //m_cbFns = gcnew ConcurrentDictionary<Type^, Object^>();
                    m_ioLock = gcnew Object();
                    m_tpioColl = gcnew BlockingCollection<IntPtr>();

                    m_stateDictionary = IOStateBase::CreateDictionary();
                }

                AsyncStreamIO(HANDLE h, Int32 maxIO) : AsyncStreamIO()
                {
                    InitTPIO(h, maxIO);
                    InitNativeClass(h);
                }

                AsyncStreamIO(String^ name, FileAccess access, FileOptions fOpt, bool bBufferLess) : AsyncStreamIO()
                {
                    HANDLE h = (HANDLE)AsyncStreamIO::OpenFileHandle(name, access, fOpt, bBufferLess);
                    InitTPIO(h, 5);
                    InitNativeClass(h);
                    InitIO(access, fOpt, bBufferLess);
                }

                // destructor (e.g. Dispose with bDisposing = true), automatically adds suppressfinalize call
                ~AsyncStreamIO()
                {
                    this->!AsyncStreamIO(); // finalizer - suppress finalize is already done
                                            //GC::SuppressFinalize(this);
                                            //Free();
                }

                !AsyncStreamIO()
                {
                    Free();
                }

                static IntPtr OpenFileHandle(String^ name, FileAccess access, FileOptions fOpt, UInt32 otherFlags, FileShare share, bool bBufferLess)
                {
                    array<Char> ^nameArr = name->ToCharArray();
                    pin_ptr<Char> namePtr = &nameArr[0];
                    Char *pName = (Char*)namePtr;
                    int dwFlags = (int)fOpt;
                    dwFlags |= otherFlags;
                    if (bBufferLess)
                        dwFlags |= FILE_FLAG_NO_BUFFERING;
                    Int32 accessMode = 0;
                    Int32 creation = 0;
                    //if ((int)access & (int)FileAccess::Read)
                    //    accessMode |= GENERIC_READ;
                    //if ((int)access & (int)FileAccess::Write)
                    //    accessMode |= GENERIC_WRITE;
                    switch (access)
                    {
                    case FileAccess::Read:
                        accessMode = GENERIC_READ;
                        creation = OPEN_EXISTING;
                        break;
                    case FileAccess::Write:
                        accessMode = GENERIC_WRITE;
                        creation = CREATE_ALWAYS;
                        break;
                    case FileAccess::ReadWrite:
                        accessMode = GENERIC_READ | GENERIC_WRITE;
                        creation = OPEN_ALWAYS;
                        break;
                    }
                    HANDLE h = CreateFile(pName, accessMode, (DWORD)share, nullptr, creation, dwFlags, nullptr);
                    if (INVALID_HANDLE_VALUE == h)
                    {
                        int e = GetLastError();
                        printf("Last error on CreateFile: 0x%x", e);
                    }
                    return (IntPtr)h;
                }

                static IntPtr OpenFileHandle(String^ name, FileAccess access, FileOptions fOpt, bool bBufferLess)
                {
                    return OpenFileHandle(name, access, fOpt, 0, FileShare::Read, bBufferLess);
                }

                void InitIO(FileAccess access, FileOptions fOpt, bool bBufferLess)
                {
                    m_bBufferless = bBufferLess;
                    m_position = 0LL;
                    pin_ptr<__int64> pLen = &m_length;
                    int ret = m_cb->FileSize((__int64*)pLen);
                    if (!ret)
                        throw gcnew IOException("Unable to get file length");
                    //m_canRead = ((accessMode & GENERIC_READ) != 0);
                    //m_canWrite = ((accessMode & GENERIC_WRITE) != 0);
                    m_canRead = (access == FileAccess::Read || access == FileAccess::ReadWrite);
                    m_canWrite = (access == FileAccess::Write || access == FileAccess::ReadWrite);
                    m_canSeek = true;
                }

                void InitTPIO(HANDLE h, Int32 maxIO)
                {
                    int i;

                    m_numTPIO = maxIO;
                    m_tpio = new TPIO*[m_numTPIO];
                    for (i = 0; i < maxIO; i++)
                    {
                        m_tpio[i] = new TPIO(h);
                        m_tpioColl->Add((IntPtr)m_tpio[i]);
                    }
                }

                void InitNativeClass(HANDLE h)
                {
                    //m_cb = new IOCallback<false>(h, m_numTPIO, m_tpio, nullptr, nullptr);
                    m_cb = new IOCallback<true>(h, m_numTPIO, m_tpio, IOState<byte>::CbFn(), m_stateDictionary->SelfPtr());
                }

                static AsyncStreamIO^ OpenFile(String^ name, FileAccess access, FileOptions fOpt, bool bBufferLess)
                {
                    IntPtr h = AsyncStreamIO::OpenFileHandle(name, access, fOpt, bBufferLess);
                    AsyncStreamIO ^io = gcnew AsyncStreamIO((HANDLE)h, 5);
                    io->InitIO(access, fOpt, bBufferLess);
                    return io;
                }

                // Since Stream class implements IDisposable, it automatically calls close first prior to the virtual Dispose method, i.e. in Stream class
                // IDisposable::Dispose()
                // {
                //     Dispose(); // calls publicly available Dispose() - for all classes in namespace System (then calls Close)
                // }
                // 
                // void Dispose()
                // {
                //     Close();
                // }
                //
                // virtual void Close()
                // {
                //     Dispose(true)
                // }
                //
                // virtual void Dispose(bool bDisposing) {} -> gets overwritten by us in dtor
                virtual void __clrcall Close() new = Stream::Close
                {
                    // close the stream
                    Lock lock(this);
                if (m_cb != nullptr)
                    m_cb->Close();
                // this will actually dispose stuff, so close stuff before
                Stream::Close();
                }

                    property bool CanRead
                {
                    virtual bool __clrcall get() new = Stream::CanRead::get{ return m_canRead; }
                }
                property bool CanWrite
                {
                    virtual bool __clrcall get() override { return m_canWrite; }
                }
                property bool CanSeek
                {
                    virtual bool __clrcall get() override { return m_canSeek; }
                }
                virtual property __int64 Length
                {
                    __int64 __clrcall get() override { return m_length; }
                }
                virtual property __int64 Position
                {
                    __int64 __clrcall get() override { return m_position; }
                    void __clrcall set(__int64 pos) override
                    {
                        this->Seek(pos, SeekOrigin::Begin);
                    }
                }
                virtual void __clrcall Flush() override
                {
                    if (FALSE == m_cb->FlushFile())
                        throw gcnew IOException("Unable to flush file");
                }
                virtual __int64 __clrcall Seek(__int64 offset, SeekOrigin origin) override
                {
                    __int64 newPos;
                    int ret = m_cb->SeekFile(offset, (int)origin, &newPos);
                    if (FALSE == ret)
                        throw gcnew IOException("Unable to seek");
                    return newPos;
                }
                virtual void __clrcall SetLength(__int64 length) override
                {
                    assert(false);
                    // not supported
                }
                virtual int __clrcall Read(array<byte> ^buf, int offset, int cnt) new = Stream::Read
                {
                    return ReadFileSync(buf, offset, cnt);
                }
                    virtual void __clrcall Write(array<byte> ^buf, int offset, int cnt) new = Stream::Write
                {
                    int ret = WriteFileSync(buf, offset, cnt);
                if (ret != cnt)
                    throw gcnew IOException("Unable to write to stream");
                }

                bool BufferLess() { return m_bBufferless; }

                virtual void UpdatePos(IntPtr tpio, Int64 amt) = IIO::UpdateIO
                {
                    m_position += amt;
                m_length = max(m_length, m_position);
                if (IntPtr::Zero != tpio)
                    m_tpioColl->Add(tpio);
                }

                    void SetLock(Object^ lockObj)
                {
                    m_ioLock = lockObj;
                }

                // not useful as both m_ptp and FileStream cannot both simultaneously exist!!
                //static FileStream^ OpenFileAsyncWrite(String^ name)
                //{
                //    array<wchar_t> ^nameArr = name->ToCharArray();
                //    pin_ptr<wchar_t> namePtr = &nameArr[0];
                //    LPCWSTR pName = (LPCWSTR)namePtr;
                //    HANDLE h = CreateFile(pName, GENERIC_WRITE, FILE_SHARE_READ, nullptr, CREATE_ALWAYS, FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH, nullptr);
                //    SafeFileHandle ^sh = gcnew SafeFileHandle(IntPtr(h), true); // cannot do with "true" as finalizer attempts to close the handle without checking
                //    FileStream ^fs = gcnew FileStream(sh, FileAccess::Write, 4096, true);
                //    return fs;
                //}

                generic <class T> int ReadFilePos(IntPtr ptr, array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
                {
                    //Lock lock(m_ioLock);
                    IOState<T>^ ioState = gcnew IOState<T>();
                    IntPtr tpio = m_tpioColl->Take(); // may block until one is available
                    IntPtr pBuf = ioState->Set(ptr, state, pBuffer, offset, cb, tpio, this);
                    m_stateDictionary->AddToMap(ioState);

                    //int ret = m_cb->ReadFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*sizeof(T), cbFn->CbFn(), ioState->SelfPtr(), position*sizeof(T));
                    int ret = m_cb->ReadFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*sizeof(T), position*sizeof(T));
                    if (-1 == ret || ret > 0)
                    {   // > 0 not really an error, but finished sync, so no callback
                        if (-1 == ret)
                        {
                            Int32 error = GetLastError();
                            throw gcnew Exception("Error reading file:" + error.ToString());
                        }
                        ioState->InvokeCb(NO_ERROR, 0);
                        ioState->Free();
                    }
                    return ret;
                }

                generic <class T> __forceinline int ReadFilePos(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
                {
                    return ReadFilePos(IntPtr::Zero, pBuffer, offset, nNum, cb, state, position);
                }

                generic <class T> __forceinline int ReadFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
                {
                    return ReadFilePos(pBuffer, offset, nNum, cb, state, m_position);
                }

                generic <class T> int WriteFilePos(IntPtr ptr, array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
                {
                    //Lock lock(m_ioLock);
                    IOState<T> ^ioState = gcnew IOState<T>();
                    IntPtr tpio = m_tpioColl->Take();
                    IntPtr pBuf = ioState->Set(ptr, state, pBuffer, offset, cb, tpio, this);
                    m_stateDictionary->AddToMap(ioState);

                    //printf("Write first elem: %d\n", *(int*)pBuf.ToPointer());
                    //int ret = m_cb->WriteFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*sizeof(T), cbFn->CbFn(), ioState->SelfPtr(), position*sizeof(T));
                    int ret = m_cb->WriteFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*sizeof(T), position*sizeof(T));
                    if (-1 == ret || ret > 0)
                    {   // > 0 not really an error, but finished sync, so no callback
                        if (-1 == ret)
                        {
                            Int32 error = GetLastError();
                            throw gcnew Exception("Error writing file:" + error.ToString());
                        }
                        ioState->InvokeCb(NO_ERROR, 0);
                        ioState->Free();
                    }
                    return ret;
                }

                generic <class T> __forceinline int WriteFilePos(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
                {
                    return WriteFilePos(IntPtr::Zero, pBuffer, offset, nNum, cb, state, position);
                }

                generic <class T> __forceinline int WriteFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
                {
                    return WriteFilePos(pBuffer, offset, nNum, cb, state, m_position);
                }

                generic <class T> int ReadFileSync(array<T> ^pBuffer, int offset, int nNum)
                {
                    Lock lock(m_ioLock);
                    pin_ptr<T> pBuf = &pBuffer[0];
                    int amtRead = m_cb->ReadFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
                    if (amtRead >= 0)
                        UpdatePos(IntPtr::Zero, amtRead);
                    return amtRead;
                }

                generic <class T> int WriteFileSync(array<T> ^pBuffer, int offset, int nNum)
                {
                    Lock lock(m_ioLock);
                    pin_ptr<T> pBuf = &pBuffer[0];
                    int amtWrite = m_cb->WriteFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
                    if (amtWrite >= 0)
                        UpdatePos(IntPtr::Zero, amtWrite);
                    return amtWrite;
                }

                generic <class T> void AdjustFilePosition(int amt)
                {
                    Lock lock(m_ioLock);
                    Int64 newPos;
                    m_position += amt*sizeof(T);
                    if (FALSE == m_cb->SeekFile(amt*sizeof(T), (Int32)SeekOrigin::Current, &newPos))
                        throw gcnew IOException("Unable to seek");
                }

                generic <class T> void SetFileLength(Int64 length)
                {
                    Int64 newPos;
                    // wait for callbacks to complete
                    m_cb->WaitForIoComplete();
                    m_position = m_length = length*sizeof(T);
                    if (FALSE == m_cb->SeekFile(length*sizeof(T), (Int32)SeekOrigin::Begin, &newPos))
                        throw gcnew IOException("Unable to seek to end");
                    if (FALSE == m_cb->SetFileEnd())
                        throw gcnew IOException("Unable to set end of file");
                }

                void WaitForIOFinish()
                {
                    Lock lock(this);
                    if (nullptr != m_cb)
                        m_cb->WaitForIoComplete();
                }
            };
        }
    }
}


