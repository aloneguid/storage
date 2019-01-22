 using Storage.Net.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Storage.Net.Tests.Core
{
   public class QueueMessageTest
   {
      [Fact]
      public void Binary_Serialize_Deserializes()
      {
         var qm = new QueueMessage("id", "content");
         qm.DequeueCount = 4;
         qm.Properties.Add("key", "value");

         byte[] data = qm.ToByteArray();

         QueueMessage qm2 = QueueMessage.FromByteArray(data);

         Assert.Equal("id", qm2.Id);
         Assert.Equal("content", qm2.StringContent);
         Assert.Equal(4, qm2.DequeueCount);
         Assert.Single(qm2.Properties);
         Assert.Equal("value", qm2.Properties["key"]);
      }

      [Fact]
      public void Binary_NullId_Handled()
      {
         QueueMessage qm1 = QueueMessage.FromText("content2");
         QueueMessage qm2 = QueueMessage.FromByteArray(qm1.ToByteArray());

         Assert.Null(qm2.Id);
         Assert.Equal("content2", qm2.StringContent);
      }
   }
}
