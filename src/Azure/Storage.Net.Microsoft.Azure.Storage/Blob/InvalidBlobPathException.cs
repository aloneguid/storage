using System;
using System.Runtime.Serialization;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   [Serializable]
   internal class InvalidBlobPathException : Exception
   {
      public InvalidBlobPathException()
      {
      }

      public InvalidBlobPathException(string message) : base(message)
      {
      }

      public InvalidBlobPathException(string message, Exception innerException) : base(message, innerException)
      {
      }

      protected InvalidBlobPathException(SerializationInfo info, StreamingContext context) : base(info, context)
      {
      }
   }
}