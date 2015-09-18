(*---------------------------------------------------------------------------
	Copyright 2013 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools.Crypt

open System
open System.IO
open System.Security.Cryptography
open Prajna.Tools

[<AbstractClass>]
type internal Crypt() =

    static member SaltBytes = 32
    static member DeriveByteIter = 10000
    static member DefaultKeySize = 256
    static member DefaultBlkSize = 128

    // symmetric key rjnd encrypt/decrypt byte[] -> byte[]
    static member Encrypt(clearBuf : byte[], pwd : string, keySize : int, blkSize : int) =
        let rjndn = new AesCryptoServiceProvider()
        rjndn.BlockSize <- blkSize
        rjndn.KeySize <- keySize
        rjndn.Padding <- PaddingMode.None
        assert(rjndn.BlockSize/8 < 256)
        let blkBytes = byte (rjndn.BlockSize / 8)
        let ms = new MemStream()

        // use random salt to generate key
        let salt = Array.zeroCreate<byte>(Crypt.SaltBytes)
        let rnd = new Random()
        rnd.NextBytes(salt)
        let mutable nPadding = blkBytes - byte (clearBuf.Length % int blkBytes)
        let clearBuf =
            if (nPadding = blkBytes) then
                nPadding <- 0uy
                clearBuf
            else
                let padding = Array.zeroCreate<byte>(int nPadding)
                rnd.NextBytes(padding)
                Array.concat([clearBuf; padding])
        salt.[0] <- nPadding // overwrite first byte of salt with padding
        let keygen = new Rfc2898DeriveBytes(pwd, salt, Crypt.DeriveByteIter)
        rjndn.Key <- keygen.GetBytes(rjndn.KeySize/8)
        rjndn.IV <- keygen.GetBytes(int blkBytes)
        ms.Write(salt, 0, Crypt.SaltBytes)

        // write out
        let cs = new CryptoStream(ms, rjndn.CreateEncryptor(), CryptoStreamMode.Write)
        cs.Write(clearBuf, 0, clearBuf.Length)
        cs.Flush() // should flush underlying MemStream
        let outBuf = Array.sub (ms.GetBuffer()) 0 (int ms.Length)
        ms.DecRef()
        outBuf

    // symmetric key rjnd encrypt/decrypt byte[] -> byte[]
    static member EncryptWithParams(clearBuf : byte[], pwd : string, ?keySize : int, ?blkSize : int) =
        let rjndn = new AesCryptoServiceProvider()
        let keySize = defaultArg keySize Crypt.DefaultKeySize
        let blkSize = defaultArg blkSize Crypt.DefaultBlkSize
        rjndn.BlockSize <- blkSize
        rjndn.KeySize <- keySize
        rjndn.Padding <- PaddingMode.None
        assert(rjndn.BlockSize/8 < 256)
        let blkBytes = byte (rjndn.BlockSize / 8)
        let ms = new MemStream()

        // use random salt to generate key
        let salt = Array.zeroCreate<byte>(Crypt.SaltBytes)
        let rnd = new Random()
        rnd.NextBytes(salt)
        let mutable nPadding = blkBytes - byte (clearBuf.Length % int blkBytes)
        let clearBuf =
            if (nPadding = blkBytes) then
                nPadding <- 0uy
                clearBuf
            else
                let padding = Array.zeroCreate<byte>(int nPadding)
                rnd.NextBytes(padding)
                Array.concat([clearBuf; padding])
        let keygen = new Rfc2898DeriveBytes(pwd, salt, Crypt.DeriveByteIter)
        rjndn.Key <- keygen.GetBytes(rjndn.KeySize/8)
        rjndn.IV <- keygen.GetBytes(int blkBytes)
        // write parameters used
        ms.WriteInt32(Crypt.SaltBytes)
        ms.WriteInt32(Crypt.DeriveByteIter)
        ms.WriteInt32(keySize)
        ms.WriteInt32(blkSize)
        ms.WriteByte(nPadding)
        ms.WriteBytes(salt)

        // write out clear buf using encryption - attach to ms
        let cs = new CryptoStream(ms, rjndn.CreateEncryptor(), CryptoStreamMode.Write)
        cs.Write(clearBuf, 0, clearBuf.Length)
        cs.Flush() // should flush underlying MemStream
        let outBuf = Array.sub (ms.GetBuffer()) 0 (int ms.Length)
        ms.DecRef()
        outBuf

    static member Decrypt(cipherBuf : byte[], pwd : string, keySize : int, blkSize : int) =
        let rjndn = new AesCryptoServiceProvider()
        rjndn.BlockSize <- blkSize
        rjndn.KeySize <- keySize
        rjndn.Padding <- PaddingMode.None
        assert(rjndn.BlockSize/8 < 256)
        let blkBytes = byte (rjndn.BlockSize / 8)
        let ms = new MemStream(cipherBuf)

        // read salt & get padding
        let salt = Array.zeroCreate<byte>(Crypt.SaltBytes)
        ms.Read(salt, 0, Crypt.SaltBytes) |> ignore
        let nPadding = salt.[0]
        let keygen = new Rfc2898DeriveBytes(pwd, salt, Crypt.DeriveByteIter)
        rjndn.Key <- keygen.GetBytes(rjndn.KeySize/8)
        rjndn.IV <- keygen.GetBytes(int blkBytes)

        // read
        let cs = new CryptoStream(ms, rjndn.CreateDecryptor(), CryptoStreamMode.Read)
        let clearBuf = Array.zeroCreate<byte>((int ms.Length)-Crypt.SaltBytes)
        cs.Read(clearBuf, 0, clearBuf.Length) |> ignore
        let outBuf = Array.sub clearBuf 0 (clearBuf.Length - int nPadding)
        ms.DecRef()
        outBuf

    // no need to separately send parameters being used
    static member DecryptWithParams(cipherBuf : byte[], pwd : string) =
        use ms = new MemStream(cipherBuf)
        // read parameters
        let saltBytes = ms.ReadInt32()
        let deriveByteIter = ms.ReadInt32()
        let keySize = ms.ReadInt32()
        let blkSize = ms.ReadInt32()
        let nPadding = ms.ReadByte()
        let salt = Array.zeroCreate<byte>(saltBytes)
        ms.ReadBytes(salt) |> ignore

        let rjndn = new AesCryptoServiceProvider()
        rjndn.BlockSize <- blkSize
        rjndn.KeySize <- keySize
        rjndn.Padding <- PaddingMode.None
        assert(rjndn.BlockSize/8 < 256)
        let blkBytes = byte (rjndn.BlockSize / 8)

        let keygen = new Rfc2898DeriveBytes(pwd, salt, deriveByteIter)
        rjndn.Key <- keygen.GetBytes(rjndn.KeySize/8)
        rjndn.IV <- keygen.GetBytes(int blkBytes)

        // read
        let cs = new CryptoStream(ms, rjndn.CreateDecryptor(), CryptoStreamMode.Read)
        let clearBuf = Array.zeroCreate<byte>((int ms.Length)-(int ms.Position))
        cs.Read(clearBuf, 0, clearBuf.Length) |> ignore
        let outBuf = Array.sub clearBuf 0 (clearBuf.Length - int nPadding)
        ms.DecRef()
        outBuf

    static member EncryptStr(str : string, pwd : string) =
        let strBuf = Text.Encoding.UTF8.GetBytes(str)
        let encBuf = Crypt.Encrypt(strBuf, pwd, 256, 128)
        System.Convert.ToBase64String(encBuf)

    static member EncryptStrWithParams(str : string, pwd : string) =
        let strBuf = Text.Encoding.UTF8.GetBytes(str)
        let encBuf = Crypt.EncryptWithParams(strBuf, pwd, Crypt.DefaultKeySize, Crypt.DefaultBlkSize)
        System.Convert.ToBase64String(encBuf)

    static member DecryptStr(str : string, pwd : string) =
        let buf = System.Convert.FromBase64String(str)
        let decBuf = Crypt.Decrypt(buf, pwd, 256, 128)
        Text.Encoding.UTF8.GetString(decBuf)

    static member DecryptStrWithParams(str : string, pwd : string) =
        let buf = System.Convert.FromBase64String(str)
        let decBuf = Crypt.DecryptWithParams(buf, pwd)
        Text.Encoding.UTF8.GetString(decBuf)

    static member EncryptBufToStr(buf : byte[], pwd : string) =
        let encBuf = Crypt.Encrypt(buf, pwd, 256, 128)
        System.Convert.ToBase64String(encBuf)

    static member EncryptBufToStrWithParams(buf : byte[], pwd : string) =
        let encBuf = Crypt.EncryptWithParams(buf, pwd, Crypt.DefaultKeySize, Crypt.DefaultBlkSize)
        System.Convert.ToBase64String(encBuf)

    static member DecryptStrToBuf(str : string, pwd : string) =
        let buf = System.Convert.FromBase64String(str)
        Crypt.Decrypt(buf, pwd, 256, 128)

    static member DecryptStrToBufWithParams(str : string, pwd : string) =
        let buf = System.Convert.FromBase64String(str)
        Crypt.DecryptWithParams(buf, pwd)

    static member SHAStr(str : string) =
        let sha = new SHA256Managed()
        let strBuf = Text.Encoding.UTF8.GetBytes(str)
        let sha = sha.ComputeHash(strBuf)
        System.Convert.ToBase64String(sha)

    static member SHABufStr(buf : byte[]) =
        let sha = new SHA256Managed()
        System.Convert.ToBase64String(sha.ComputeHash(buf))

    static member RSAGetNewKey(keyNumber : KeyNumber) : byte[]*byte[] =
        let csp = new CspParameters()
        csp.ProviderType <- 1 // RSA_FULL
        csp.ProviderName <- "Microsoft Strong Cryptographic Provider" // MS_STRONG_PROV
        csp.KeyNumber <- int keyNumber
        let rsa = new RSACryptoServiceProvider(csp)
        (rsa.ExportCspBlob(true), rsa.ExportCspBlob(false))

    static member RSAGetNewKeysStr(password : string, keyNumber : KeyNumber) : string*string =
        let (privateKey, publicKey) = Crypt.RSAGetNewKey(keyNumber)
        // encrypt private data
        let privateKeyStr = Crypt.EncryptBufToStrWithParams(privateKey, password)
        let publicKeyStr = System.Convert.ToBase64String(publicKey)
        (privateKeyStr, publicKeyStr)

    static member RSAGetNewKeys(password : string, keyNumber : KeyNumber) : byte[]*byte[] =
        let (privateKey, publicKey) = Crypt.RSAGetNewKey(keyNumber)
        (Crypt.EncryptWithParams(privateKey, password, Crypt.DefaultKeySize, Crypt.DefaultBlkSize), publicKey)

    static member RSAToPublicKeyStr(rsa : RSACryptoServiceProvider) =
        let keyBlob = rsa.ExportCspBlob(false)
        System.Convert.ToBase64String(keyBlob)

    static member RSAToPublicKey(rsa : RSACryptoServiceProvider) =
        rsa.ExportCspBlob(false)

    static member RSAToPrivateKey(rsa : RSACryptoServiceProvider, password : string) =
        Crypt.EncryptWithParams(rsa.ExportCspBlob(true), password, Crypt.DefaultKeySize, Crypt.DefaultBlkSize)

    static member RSAFromKey(keyBlob : byte[]) =
        let rsa = new RSACryptoServiceProvider()
        rsa.ImportCspBlob(keyBlob)
        rsa

    static member RSAFromPrivateKey(keyBlob : byte[], password : string) =
        Crypt.RSAFromKey(Crypt.DecryptWithParams(keyBlob, password))

    static member RSAFromPublicKeyStr(publicKeyStr : string) =
        Crypt.RSAFromKey(System.Convert.FromBase64String(publicKeyStr))

    static member RSAFromPrivateKeyStr(privateKeyStr : string, password : string) =
        Crypt.RSAFromKey(Crypt.DecryptStrToBufWithParams(privateKeyStr, password))

    static member Encrypt(rjnd : AesCryptoServiceProvider, buf : byte[]) : MemStream =
        rjnd.GenerateIV()
        let blkBytes = byte (rjnd.BlockSize >>> 3)
        let mutable nPadding = blkBytes - byte (buf.Length % int blkBytes)
        rjnd.Padding <- PaddingMode.None
//        if (nPadding = blkBytes) then
//            nPadding <- 0uy
//            rjnd.Padding <- PaddingMode.None
//        else
//            rjnd.Padding <- PaddingMode.PKCS7
        let buf =
            if (nPadding = 0uy) then
                buf
            else
                Array.concat([buf; Array.zeroCreate<byte>(int nPadding)])
        let ms = new MemStream()
        ms.WriteByte(nPadding)
        ms.WriteBytesWLen(rjnd.IV)
        let cs = new CryptoStream(ms, rjnd.CreateEncryptor(), CryptoStreamMode.Write)
        cs.Write(buf, 0, buf.Length)
        cs.Flush()
        ms

    static member Decrypt(rjnd : AesCryptoServiceProvider, ms : StreamBase<byte>) : byte[]*int =
        let nPadding = ms.ReadByte()
        rjnd.IV <- ms.ReadBytesWLen()
        rjnd.Padding <- PaddingMode.None
//        if (nPadding = 0) then
//            rjnd.Padding <- PaddingMode.None
//        else
//            rjnd.Padding <- PaddingMode.PKCS7
        let cs = new CryptoStream(ms, rjnd.CreateDecryptor(), CryptoStreamMode.Read)
        let clearBuf = Array.zeroCreate<byte>((int ms.Length)-(int ms.Position))
        cs.Read(clearBuf, 0, clearBuf.Length) |> ignore
        (clearBuf, clearBuf.Length-nPadding)

    static member Decrypt(rjnd : AesCryptoServiceProvider, buf : byte[]) : byte[]*int =
        use ms = new MemStream(buf)
        let outParam = Crypt.Decrypt(rjnd, ms)
        ms.DecRef()
        outParam
