using System;
using System.Collections.Generic;

namespace Storage.Net
{
   /// <summary>
   /// Generic storage exception
   /// </summary>
   public class StorageException : Exception
   {
      private static readonly Dictionary<ErrorCode, string> ErrorCodeToMessage = new Dictionary<ErrorCode, string>();

      /// <summary>
      /// Creates a new instance of <see cref="StorageException"/>
      /// </summary>
      public StorageException()
      {
      }

      /// <summary>
      /// Creates a new instance of <see cref="StorageException"/> with exception message
      /// </summary>
      public StorageException(string message) : base(message)
      {
      }

      static StorageException()
      {
         foreach(ErrorCode code in Enum.GetValues(typeof(ErrorCode)))
         {
            string message = $"request failed with code '{code}'";
            ErrorCodeToMessage[code] = message;
         }
      }

      /// <summary>
      /// Creates a new instance of <see cref="StorageException"/> by error code
      /// </summary>
      public StorageException(ErrorCode code, Exception innerException) : base(ErrorCodeToMessage[code], innerException)
      {
         ErrorCode = code;
      }

      /// <summary>
      /// Creates a new instance of <see cref="StorageException"/> with exception message and inner exception
      /// </summary>
      public StorageException(string message, Exception inner) : base(message, inner)
      {
      }

      /// <summary>
      /// Indicates the error code for this exception
      /// </summary>
      public ErrorCode ErrorCode { get; private set; }
   }
}
