// This is the main DLL file.

#include "stdafx.h"

#include "ImageProcUM.h"

// ====================================================
#pragma unmanaged
// unmanaged code goes here

// Assumes input is 24bpp RGB, interleaved
void DownSample2x2(unsigned char *in, unsigned char *out, int widthIn, int heightIn, int strideIn, int strideOut)
{
    int i, j;
    unsigned char *pRIn,  *pGIn, *pBIn;
    unsigned char *pRIn2, *pGIn2, *pBIn2;
    unsigned char *pROut, *pGOut, *pBOut;
    int widthOut = widthIn/2;
    int heightOut = heightIn/2;
    for (i=0; i < heightOut; i++)
    {
        pRIn = in + 2*i*strideIn;
        pGIn = pRIn+1;
        pBIn = pGIn+1;
        pRIn2 = pRIn + strideIn;
        pGIn2 = pRIn2+1;
        pBIn2 = pGIn2+1;
        pROut = out + i*strideOut;
        pGOut = pROut+1;
        pBOut = pGOut+1;
        for (j=0; j < widthOut; j++)
        {
            *pROut = (*pRIn+*(pRIn+3)+*pRIn2+*(pRIn2+3))>>2;
            *pGOut = (*pGIn+*(pGIn+3)+*pGIn2+*(pGIn2+3))>>2;
            *pBOut = (*pBIn+*(pBIn+3)+*pBIn2+*(pBIn2+3))>>2;
            pROut += 3;
            pGOut += 3;
            pBOut += 3;
            pRIn += 6;
            pGIn += 6;
            pBIn += 6;
            pRIn2 += 6;
            pGIn2 += 6;
            pBIn2 += 6;
        }
    }
}
