// ImageProcUM.h

#pragma once

#pragma unmanaged
void DownSample2x2(unsigned char *in, unsigned char *out, int widthIn, int heightIn, int strideIn, int strideOut);

#pragma managed
using namespace System;

namespace ImageProcUM {

	public ref class ProcUM
	{
    public:
        ProcUM() {}
        ~ProcUM() {}

        void Process(IntPtr input, IntPtr output, int widthIn, int heightIn, int strideIn, int strideOut)
        {
            DownSample2x2((unsigned char*)input.ToPointer(), (unsigned char*)output.ToPointer(), 
                          widthIn, heightIn, strideIn, strideOut);
        }
	};
}
