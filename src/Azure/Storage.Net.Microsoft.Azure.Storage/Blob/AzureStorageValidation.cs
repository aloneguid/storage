using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using AzureStorageException = Microsoft.WindowsAzure.Storage.StorageException;

namespace Storage.Net.Microsoft.Azure.Storage.Blob
{
   static class AzureStorageValidation
   {
      public static void ValidateContainerName(string containerName)
      {
         if (containerName == null) throw new ArgumentNullException(nameof(containerName));

         /* from MSDN:
          *
          A container name must be a valid DNS name, conforming to the following naming rules:
          1. Container names must start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
          2. Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted in container names.
          3. All letters in a container name must be lowercase.
          4. Container names must be from 3 through 63 characters long.
         */

         //1. not sure

         //2. not sure

         //3. check that all characters are lowercase
         for (int i = 0; i < containerName.Length; i++)
         {
            if (char.IsLetter(containerName[i]) && !char.IsLower(containerName, i))
            {
               throw new ArgumentOutOfRangeException(nameof(containerName),
                  $"container [{containerName}] has uppercase character at position {i}");
            }
         }

         //4. check for length
         if (containerName.Length < 3 || containerName.Length > 63)
         {
            throw new ArgumentOutOfRangeException(nameof(containerName),
               $"container [{containerName}] length must be between 3 and 63 but it's {containerName.Length}");
         }
      }

      public static bool IsDoesntExist(AzureStorageException ex)
      {
         return ex.RequestInformation?.HttpStatusCode == 404;
      }

      public static bool TryHandleStorageException(AzureStorageException ex)
      {
         if (IsDoesntExist(ex))
         {
            throw new StorageException(ErrorCode.NotFound, ex);
         }

         return false;
      }

   }
}
