#if !NET16
using System;
using System.IO;
using System.Security.Cryptography;

namespace Storage.Net.Blobs.Sinks.Impl
{
   /// <summary>
   /// 
   /// </summary>
   public class SymmetricEncryptionSink : ITransformSink
   {
      private readonly SymmetricAlgorithm _cryptoAlgorithm;

      /// <summary>
      /// 
      /// </summary>
      public SymmetricEncryptionSink(string base64Key)
      {
         _cryptoAlgorithm = new RijndaelManaged();
         _cryptoAlgorithm.Key = Convert.FromBase64String(base64Key);
         _cryptoAlgorithm.GenerateIV();
      }

      /// <summary>
      /// 
      /// </summary>
      public Stream OpenReadStream(string fullPath, Stream parentStream)
      {
         return new CryptoStream(parentStream, _cryptoAlgorithm.CreateDecryptor(), CryptoStreamMode.Read);
      }

      /// <summary>
      /// 
      /// </summary>
      public Stream OpenWriteStream(string fullPath, Stream parentStream)
      {
         return new CryptoStream(parentStream, _cryptoAlgorithm.CreateEncryptor(), CryptoStreamMode.Write);
      }
   }
}
#endif