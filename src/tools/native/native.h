/*-------------------------------------------------------------------------- -
	Copyright 2013 Microsoft

	Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http ://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Sanjeev Mehrotra
-------------------------------------------------------------------------- - */
#pragma once

#pragma unmanaged

#include "stdio.h"
#include <windows.h>
#include <assert.h>

using namespace System;

typedef void (__stdcall CallbackFn)(int ioResult, __int64 polap, void *pState, void *pBuffer, int bytesTransferred);
typedef BOOL (__stdcall *OperFn)(HANDLE, LPVOID, DWORD, LPDWORD, LPOVERLAPPED);

typedef enum _Oper
{
    ReadFileOper = 0,
    WriteFileOper = 1
} Oper;

public class TPIO {
public:
    OVERLAPPED m_olap;
    void *m_pstate;
    void *m_pBuffer;
    volatile unsigned int m_inUse;
    CallbackFn *m_pfn;

public:
    TPIO(HANDLE hFile) : m_pfn(nullptr), m_pstate(nullptr), m_pBuffer(nullptr), m_inUse(0)
    {
        memset(&m_olap, 0, sizeof(m_olap));
        //m_olap.hEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
    }

    ~TPIO()
    {
        //CloseHandle(m_olap.hEvent);
    }

    __forceinline __int64 OverlappedPtr() { return (__int64)(&m_olap); }
};

template <int bUniversalCb>
public class IOCallback {
protected:
    HANDLE m_hFile;
    PTP_IO m_ptp;
    CRITICAL_SECTION *m_cs;
    CallbackFn *m_pfn;
    void *m_state;
    TPIO **m_tpio;
    int m_numTp;

    static void CALLBACK Callback(
        PTP_CALLBACK_INSTANCE Instance,
        PVOID state,
        PVOID olap,
        ULONG ioResult,
        ULONG_PTR bytesTransferred,
        PTP_IO ptp)
    {
        IOCallback *x = (IOCallback*)state;
        LPOVERLAPPED polap = (LPOVERLAPPED)olap;
        int i;
        if (NO_ERROR != ioResult)
            bytesTransferred = 0LL;
        if (bUniversalCb)
        {
            (*x->m_pfn)(ioResult, (__int64)polap, x->m_state, NULL, (int)bytesTransferred);
        }
        else
        {
            for (i = 0; i < x->m_numTp; i++)
                if (&(x->m_tpio[i]->m_olap) == polap)
                    break;
            //assert(i < x->m_numTp); 
            TPIO *tpio = x->m_tpio[i];
            tpio->m_inUse = 0;
            (*tpio->m_pfn)(ioResult, (__int64)polap, tpio->m_pstate, tpio->m_pBuffer, (int)bytesTransferred);
        }
    }

    // use enum as WriteFile and ReadFile have slightly differing signatures (LPVOID vs. LPCVOID)
    //template <class T, OperFn fn>
    template <class T, Oper oper>
    int OperFile(TPIO *tpio, T *pBuffer, DWORD nNumberOfElems, CallbackFn *pfn, void *state, __int64 pos)
    {
        if (bUniversalCb || 0 == InterlockedCompareExchange(&tpio->m_inUse, 1, 0))
        {
            if (!bUniversalCb)
            {
                tpio->m_pfn = pfn;
                tpio->m_pstate = state;
                tpio->m_pBuffer = pBuffer;
            }
            if (pos < 0)
                pos = SetFilePointer(m_hFile, 0, 0, FILE_CURRENT); // won't work for files > 32-bit in length
            tpio->m_olap.Offset = (DWORD)(pos & 0x00000000ffffffff);
            tpio->m_olap.OffsetHigh = (DWORD)(pos >> 32);
            StartThreadpoolIo(m_ptp);
            //return fn(m_hFile, (void*)pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
            int ret = 0;
            switch (oper)
            {
            case ReadFileOper:
                ret = ReadFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), NULL, &tpio->m_olap);
                break;
            case WriteFileOper:
                ret = WriteFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), NULL, &tpio->m_olap);
                break;
            default:
                assert(false);
                return -1;
            }
            if (0 == ret)
            {
                int e = GetLastError();
                if (ERROR_IO_PENDING == e)
                    return 0;
                else
                    return -1; // some other error
            }
            else
                return ret;
        }
        else
        {
            return -1;
        }
    }

public:
    // no callback supported
    IOCallback(HANDLE hFile) : m_hFile(hFile)
    {
        m_cs = new CRITICAL_SECTION;
        m_ptp = nullptr;
        m_numTp = 0;
        m_tpio = nullptr;
        m_pfn = nullptr;
        m_state = nullptr;
        InitializeCriticalSection(m_cs);
    }

    // last two arguments can be null if universal state & callback is not being used
    IOCallback(HANDLE hFile, int numTP, TPIO **tpio, CallbackFn *pfn, void *pState) : m_hFile(hFile)
    {
        m_cs = new CRITICAL_SECTION;
        m_ptp = CreateThreadpoolIo(hFile, IOCallback::Callback, this, NULL);
        m_numTp = numTP;
        m_tpio = tpio;
        m_pfn = pfn;
        m_state = pState;
        if (nullptr == m_ptp)
        {
            int e = GetLastError();
            printf("Last error on CreateThreadPoolIo: 0x%x", e);
            throw "Fail to initialize threadpool I/O error";
        }
        InitializeCriticalSection(m_cs);
    }

    virtual ~IOCallback()
    {
        Close(); // in case we didn't call close
        DeleteCriticalSection(m_cs);
        delete m_cs;
        m_cs = NULL;
    }

    template <class T>
    __forceinline int ReadFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, __int64 pos = -1LL)
    {
        return OperFile<T, ReadFileOper>(tpio, pBuffer, nNum, NULL, NULL, pos);
    }

    template <class T>
    __forceinline int ReadFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return OperFile<T, ReadFileOper>(tpio, pBuffer, nNum, pfn, state, pos);
    }

    template <class T>
    __forceinline int ReadFileSync(T *pBuffer, DWORD nNum)
    {
        DWORD num;
        if (ReadFile(m_hFile, (void*)pBuffer, nNum*sizeof(T), &num, nullptr))
        {
            return num;
        }
        else
        {
            return -1;
        }
    }

    template <class T>
    __forceinline int WriteFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, __int64 pos = -1LL)
    {
        return OperFile<T, WriteFileOper>(tpio, pBuffer, nNum, NULL, NULL, pos);
    }

    template <class T>
    __forceinline int WriteFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return OperFile<T, WriteFileOper>(tpio, pBuffer, nNum, pfn, state, pos);
    }

    template <class T>
    __forceinline int WriteFileSync(T *pBuffer, DWORD nNum)
    {
        DWORD num;
        if (WriteFile(m_hFile, (void*)pBuffer, nNum*sizeof(T), &num, nullptr))
        {
            return num;
        }
        else
        {
            return -1;
        }
    }

    __forceinline BOOL SeekFile(__int64 offset, DWORD moveMethod, __int64 *newPos)
    {
        LARGE_INTEGER liOffset;
        LARGE_INTEGER newLiOffset;
        liOffset.QuadPart = offset;
        BOOL ret = SetFilePointerEx(m_hFile, liOffset, &newLiOffset, moveMethod);
        *newPos = newLiOffset.QuadPart;
        return ret;
    }

    __forceinline BOOL FlushFile()
    {
        return FlushFileBuffers(m_hFile);
    }

    __forceinline BOOL FileSize(__int64 *fsize)
    {
        if (!fsize)
            return FALSE;
        else
        {
            LARGE_INTEGER size;
            BOOL ret = GetFileSizeEx(m_hFile, &size);
            *fsize = size.QuadPart;
            return ret;
        }
    }

    void WaitForIoComplete()
    {
        if (m_ptp)
            WaitForThreadpoolIoCallbacks(m_ptp, FALSE);
    }

    __forceinline BOOL Close()
    {
        BOOL ret = TRUE;
        //printf("InClose - Close handle %llx\n", (__int64)m_hFile);
        EnterCriticalSection(m_cs);
        //printf("InClose - Enter\n");
        if (m_ptp)
        {
            WaitForThreadpoolIoCallbacks(m_ptp, FALSE); // wait for stuff to complete
            CloseThreadpoolIo(m_ptp);
            m_ptp = nullptr;
        }
        if (m_hFile != NULL)
        {
            //printf("Closing handle %llx\n", (__int64)m_hFile);
            ret = CloseHandle(m_hFile);
            //printf("Handle %llx close\n", (__int64)m_hFile);
            m_hFile = NULL;
        }
        LeaveCriticalSection(m_cs);
        //printf("InClose - Leave\n");
        return ret;
    }

    __forceinline BOOL SetFileEnd()
    {
        return SetEndOfFile(m_hFile);
    }
};

