using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using IAzTableEntity = Microsoft.WindowsAzure.Storage.Table.ITableEntity;
using Storage.Net.KeyValue;
using NetBox.Extensions;

namespace Storage.Net.Microsoft.Azure.Storage.KeyValue
{
   class EntityAdapter : IAzTableEntity
   {
      private readonly Value _row;

      private static Dictionary<Type, Func<object, EntityProperty>> TypeToEntityPropertyFunc = new Dictionary<Type, Func<object, EntityProperty>>
      {
         [typeof(string)] = o => EntityProperty.GeneratePropertyForString((string)o),
         [typeof(byte[])] = o => EntityProperty.GeneratePropertyForByteArray((byte[])o),
         [typeof(bool)] = o => EntityProperty.GeneratePropertyForBool((bool)o),
         [typeof(DateTimeOffset)] = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset)o),
         [typeof(DateTime)] = o => EntityProperty.GeneratePropertyForDateTimeOffset((DateTimeOffset)(DateTime)o),
         [typeof(double)] = o => EntityProperty.GeneratePropertyForDouble((double)o),
         [typeof(Guid)] = o => EntityProperty.GeneratePropertyForGuid((Guid)o),
         [typeof(int)] = o => EntityProperty.GeneratePropertyForInt((int)o),
         [typeof(long)] = o => EntityProperty.GeneratePropertyForLong((long)o)
      };

      public EntityAdapter(Value row)
      {
         _row = row;

         Init(row?.Id, true);
      }

      public EntityAdapter(Key rowId)
      {
         Init(rowId, true);
      }

      private void Init(Key rowId, bool useConcurencyKey)
      {
         if (rowId == null)
            throw new ArgumentNullException("rowId");

         PartitionKey = ToInternalId(rowId.PartitionKey);
         RowKey = ToInternalId(rowId.RowKey);
         ETag = "*";
      }

      public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
      {
         throw new NotSupportedException();
      }

      public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
      {
         //Azure Lib calls this when it wants to transform this entity to a writeable one

         var dic = new Dictionary<string, EntityProperty>();
         foreach (KeyValuePair<string, object> cell in _row)
         {
            if (cell.Value == null)
            {
               continue;
            }

            EntityProperty ep;
            Type t = cell.Value.GetType();

            if (!TypeToEntityPropertyFunc.TryGetValue(t, out Func<object, EntityProperty> factoryMethod))
            {
               ep = EntityProperty.GeneratePropertyForString(cell.Value.ToString());
            }
            else
            {
               ep = factoryMethod(cell.Value);
            }

            dic[cell.Key] = ep;
         }
         return dic;
      }

      public string PartitionKey { get; set; }

      public string RowKey { get; set; }

      public DateTimeOffset Timestamp { get; set; }

      public string ETag { get; set; }

      private static string ToInternalId(string userId)
      {
         return userId.UrlEncode();
      }
   }
}
