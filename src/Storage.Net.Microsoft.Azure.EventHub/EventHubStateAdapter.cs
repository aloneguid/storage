using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Storage.Net.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Net.Microsoft.Azure.EventHub
{
   class EventHubStateAdapter
   {
      private readonly IBlobStorage _blobStorage;

      public EventHubStateAdapter(IBlobStorage blobStorage)
      {
         _blobStorage = blobStorage;
      }

      public async Task<EventPosition> GetPartitionPosition(string partitionId)
      {
         if (partitionId == null)
            throw new ArgumentNullException(nameof(partitionId));

         if (_blobStorage == null) return EventPosition.FromStart();

         string state;

         try
         {
            state = await _blobStorage.ReadTextAsync(GetBlobName(partitionId));
         }
         catch(StorageException ex) when (ex.ErrorCode == ErrorCode.NotFound)
         {
            state = null;
         }

         if (state == null) return EventPosition.FromStart();

         StateToken token = state.AsJsonObject<StateToken>();

         return token.SequenceNumber == null ? EventPosition.FromStart() : EventPosition.FromSequenceNumber(token.SequenceNumber.Value);
      }

      public async Task SetPartitionStateAsync(string partitionId, long? sequenceNumber)
      {
         if (partitionId == null)
            throw new ArgumentNullException(nameof(partitionId));

         if (_blobStorage == null) return;

         if(sequenceNumber == null)
         {
            await _blobStorage.DeleteAsync(GetBlobName(partitionId));
         }
         else
         {
            var state = new StateToken
            {
               PartitionId = partitionId,
               SequenceNumber = sequenceNumber,
               CreatedAt = DateTime.UtcNow
            };

            await _blobStorage.WriteTextAsync(GetBlobName(partitionId), state.ToJsonString());
         }
      }

      private static string GetBlobName(string partitionId)
      {
         return $"partition-{partitionId}.json";
      }

      class StateToken
      {
         [JsonProperty("partitionId")]
         public string PartitionId { get; set; }

         [JsonProperty("sequenceNumber")]
         public long? SequenceNumber { get; set; }

         [JsonProperty("createdAt")]
         public DateTime CreatedAt { get; set; }
      }
   }
}