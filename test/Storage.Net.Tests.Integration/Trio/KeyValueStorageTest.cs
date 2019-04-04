using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Storage.Net.KeyValue;
using System.IO;
using Config.Net;
using System.Threading.Tasks;
using NetBox.Extensions;

namespace Storage.Net.Tests.Integration.KeyValue
{
   public class CsvFilesTest : KeyValueStorageTest
   {
      public CsvFilesTest() : base("csv-files") { }
   }

   public class AzureTableTest : KeyValueStorageTest
   {
      public AzureTableTest() : base("azure") { }
   }

   //there is no storage emulator on build server
   /*
   public class AzureTableEmulatorTest : KeyValueStorageTest
   {
      public AzureTableEmulatorTest() : base("azure-emulator") { }
   }*/

   public abstract class KeyValueStorageTest : AbstractTestFixture
   {
      private readonly string _name;
      private readonly IKeyValueStorage _tables;
      private readonly string _tableName;
      private readonly ITestSettings _settings;

      protected KeyValueStorageTest(string name)
      {
         _settings = new ConfigurationBuilder<ITestSettings>()
            .UseIniFile("c:\\tmp\\integration-tests.ini")
            .UseEnvironmentVariables()
            .Build();

         _name = name;

         if(_name == "csv-files")
         {
            _tables = StorageFactory.KeyValue.CsvFiles(TestDir);
         }
         else if(_name == "azure")
         {
            _tables = StorageFactory.KeyValue.AzureTableStorage(
               _settings.AzureStorageName,
               _settings.AzureStorageKey);
         }
         else if(_name == "azure-emulator")
         {
            _tables = StorageFactory.KeyValue.AzureTableDevelopmentStorage();
         }

         _tableName = "TableStorageTest" + Guid.NewGuid().ToString().Replace("-", "");
      }

      public override void Dispose()
      {
         //_tables.DeleteAsync(_tableName).Wait();

         _tables.Dispose();

         Cleanup();

         base.Dispose();
      }

      [Fact]
      public async Task DeleteTable_NonExisting_DoesntCrash()
      {
         await _tables.DeleteAsync(_tableName + "del");
      }

      [Fact]
      public async Task DeleteTable_CreateAndDelete_BackToPreviousState()
      {
         int tableNo1 = (await _tables.ListTableNamesAsync()).Count();

         await _tables.InsertAsync(_tableName, new[] { new Value("pk", "rk") });

         int tableNo2 = (await _tables.ListTableNamesAsync()).Count();
         Assert.Equal(tableNo1 + 1, tableNo2);

         await _tables.DeleteAsync(_tableName);

         int tableNo3 = (await _tables.ListTableNamesAsync()).Count();
         Assert.Equal(tableNo3, tableNo1);
      }

      [Fact]
      public async Task DeleteTable_NullInput_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.DeleteAsync(null));
      }

      [Fact]
      public async Task ListTables_NoTablesWriteARow_OneTable()
      {
         int count = (await _tables.ListTableNamesAsync()).Count();

         var row1 = new Value("part1", "k1")
         {
            ["col1"] = "value1"
         };
         await _tables.InsertAsync(_tableName, new[] { row1 });

         var names = (await _tables.ListTableNamesAsync()).ToList();
         Assert.Equal(count + 1, names.Count);
         Assert.Contains(_tableName, names);
         await _tables.DeleteAsync(_tableName);
      }

      [Fact]
      public async Task DeleteRows_AddTwoRows_DeletedDisappears()
      {
         var row1 = new Value("part1", "1")
         {
            ["col1"] = "value1"
         };
         var row2 = new Value("part1", "2")
         {
            ["col1"] = "value2"
         };
         await _tables.InsertAsync(_tableName, new[] { row1, row2 });
         await _tables.DeleteAsync(_tableName, new[] { new Key("part1", "2") });
         IReadOnlyCollection<Value> rows = await _tables.GetAsync(_tableName, new Key("part1", null));

         Assert.Single(rows);
      }

      [Fact]
      public async Task DeleteRows_NullArrayInput_DoesntCrash()
      {
         await _tables.DeleteAsync(_tableName, (Key[])null);
      }

      [Fact]
      public async Task DeleteRows_NullInput_DoesntCrash()
      {
         await _tables.DeleteAsync(_tableName, (Key[])null);
      }

      [Fact]
      public async Task DeleteRows_TableDoesNotExist_DoesntCrash()
      {
         await _tables.DeleteAsync(_tableName + "d", (Key[])null);
      }

      [Fact]
      public async Task Insert_OneRowNullTable_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.InsertAsync(null, new[] { new Value("pk", "rk") }));
      }

      [Fact]
      public async Task Insert_MultipleRowsNullTable_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.InsertAsync(null, new[] { new Value("pk", "rk1"), new Value("pk", "rk2") }));
      }

      [Fact]
      public async Task Insert_ValidTableNullRows_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.InsertAsync(_tableName, (Value[])null));
      }

      [Fact]
      public async Task Insert_VariableRows_StillReads()
      {
         var row1 = new Value("pk", "rk1")
         {
            ["col1"] = "val1",
            ["col2"] = "val2"
         };
         var row2 = new Value("pk", "rk2")
         {
            ["col2"] = "val2",
            ["col3"] = "val3"
         };
         await _tables.InsertAsync(_tableName, new[] { row1, row2 });

         Value row11 = await _tables.GetSingleAsync(_tableName, new Key("pk", "rk1"));
         Value row12 = await _tables.GetSingleAsync(_tableName, new Key("pk", "rk2"));


         Assert.Equal("val1", (string)row11["col1"]);
         Assert.Equal("val2", (string)row11["col2"]);

         Assert.Equal("val2", (string)row12["col2"]);
         Assert.Equal("val3", (string)row12["col3"]);

      }

      [Fact]
      public async Task Insert_DifferentTypes_ReadsBack()
      {
         var row = new Value("pk", "rk")
         {
            ["str"] = "some string",
            ["int"] = 4,
            ["bytes"] = new byte[] { 0x1, 0x2 },
            ["date"] = DateTime.UtcNow
         };
         await _tables.InsertAsync(_tableName, new Value[] { row });
      }

      [Fact]
      public async Task Insert_ManyRows_Succeeds()
      {
         await _tables.InsertAsync(_tableName,
            Enumerable.Range(0, 1000).Select(i => new Value("pk" + 1, "rk" + i) { ["col"] = i }).ToList());
      }

      [Fact]
      public async Task Dates_are_travelled_in_correct_timezone()
      {
         DateTime date = DateTime.UtcNow.RoundToSecond();

         var i = new Value("pk", "rk")
         {
            ["date"] = date
         };

         await _tables.InsertAsync(_tableName, new Value[] { i });

         Value o = await _tables.GetSingleAsync(_tableName, "pk", "rk");

         object dateObj = o["date"];
         DateTime date2 = (dateObj is string dateObjs) ? DateTime.Parse((string)dateObj).ToUniversalTime() : (DateTime)dateObj;

         Assert.Equal(date, date2);
      }

      class TestEntity
      {
         public string PartitionKey { get; set; }

         public string RowKey { get; set; }

         public string M1 { get; set; }

         public DateTime Date { get; set; }
      }

      [Fact]
      public async Task Insert_TwoRows_DoesntFail()
      {
         var row1 = new Value("part1", "k1")
         {
            ["col1"] = "value1"
         };
         var row2 = new Value("part2", "1")
         {
            ["col1"] = "value2"
         };
         await _tables.InsertAsync(_tableName, new[] { row1, row2 });
      }

      [Fact]
      public async Task Insert_RowsWithMissingValues_Succeeds()
      {
         var row1 = new Value("p1", "k1");
         var row2 = new Value("p1", "k2")
         {
            ["col1"] = "v1"
         };

         await _tables.InsertAsync(_tableName, new[] { row1, row2 });
      }

      [Fact]
      public async Task Insert_EmailRowKey_CanFetchBack()
      {
         //this only tests encoding problem

         var row = new Value("partition", "ivan@si.com");
         await _tables.InsertAsync(_tableName, new Value[] { row });

         Value foundRow = await _tables.GetSingleAsync(_tableName, "partition", "ivan@si.com");
         Assert.NotNull(foundRow);
      }

      [Fact]
      public async Task Insert_DuplicateRow_StorageExceptionWithDuplicateKeyCode()
      {
         var row = new Value("pk", "rk");
         await _tables.InsertAsync(_tableName, new Value[] { row });

         StorageException ex = await Assert.ThrowsAsync<StorageException>(() => _tables.InsertAsync(_tableName, new Value[] { row }));
         Assert.Equal(ErrorCode.DuplicateKey, ex.ErrorCode);
      }

      [Fact]
      public async Task Insert_LoadsOfDuplicateRows_StorageExceptionWithDuplicateKeyCode()
      {
         var rows = Enumerable.Range(0, 100)
            .Select(i => new Value("pk" + i, "rk" + i))
            .ToList();

         await _tables.InsertAsync(_tableName, rows);

         StorageException ex = await Assert.ThrowsAsync<StorageException>(() => _tables.InsertAsync(_tableName, rows));
         Assert.Equal(ErrorCode.DuplicateKey, ex.ErrorCode);
      }

      [Theory]
      [InlineData("test string", 1)]
      [InlineData("test string", 100)]
      [InlineData(true, 1)]
      [InlineData(true, 100)]
      [InlineData(1d, 1)]
      [InlineData(3.5d, 100)]
      public async Task Insert_AllSupportedTypes_Without_Crashing(object value, int repeats)
      {
         var rows = Enumerable.Range(0, repeats)
            .Select(i => new Value("pk" + i, "rk" + i) { ["col"] = value })
            .ToList();

         await _tables.InsertAsync(_tableName, rows);
      }


      [Fact]
      public async Task Insert_row_fetches_back_exactly()
      {
         var row = new Value("pk", "rk");
         row["C1"] = 1;
         row["C2"] = "string";

         await _tables.InsertAsync(_tableName, new Value[] { row });

         Value row1 = await _tables.GetSingleAsync(_tableName, "pk", "rk");
         Assert.Equal("pk", row1.PartitionKey);
         Assert.Equal("rk", row1.RowKey);
         Assert.Equal("1", row1["C1"].ToString());
         Assert.Equal("string", row1["C2"]);
      }

      [Fact]
      public async Task Insert_CleanTableDuplicateRows_FailsWithDuplicateKeyCode()
      {
         Value[] rows = new[]
         {
            new Value("pk", "rk"),
            new Value("pk", "rk"),
            new Value("pk", "rk1")
         };

         StorageException ex = await Assert.ThrowsAsync<StorageException>(() => _tables.InsertAsync(_tableName, rows));
         Assert.Equal(ErrorCode.DuplicateKey, ex.ErrorCode);

         IEnumerable<Value> rows2 = await _tables.GetAsync(_tableName, new Key("pk", null));
         Assert.Empty(rows2);
      }

      [Fact]
      public async Task Insert_RowsExistInsertDuplicateRows_FailsWithDuplicateKeyCode()
      {
         var dupeRow = new Value("pk", "rk1");
         Value[] insertRows = new[] { new Value("pk", "rk2"), dupeRow };

         await _tables.InsertAsync(_tableName, new Value[] { dupeRow });

         StorageException ex = await Assert.ThrowsAsync<StorageException>(() => _tables.InsertAsync(_tableName, insertRows));
         Assert.Equal(ErrorCode.DuplicateKey, ex.ErrorCode);

         IEnumerable<Value> rows2 = await _tables.GetAsync(_tableName, new Key("pk", null));
         Assert.Single(rows2);
      }

      [Fact]
      public async Task InsertOrReplace_CleanTableDuplicateRowsInRequest_ContinuesAnyway()
      {
         Value[] rows = new[]
         {
            new Value("pk", "rk"),
            new Value("pk", "rk"),
            new Value("pk", "rk1")
         };


         StorageException ex = await Assert.ThrowsAsync<StorageException>(() => _tables.InsertOrReplaceAsync(_tableName, rows));
         Assert.Equal(ErrorCode.DuplicateKey, ex.ErrorCode);

         IEnumerable<Value> rows2 = await _tables.GetAsync(_tableName, new Key("pk", null));
         Assert.Empty(rows2);
      }

      [Fact]
      public async Task InsertOrReplace_UpdateValues_Updated()
      {
         var row = new Value("pk", "rk")
         {
            ["col1"] = "v1"
         };

         await _tables.InsertAsync(_tableName, new[] { row });

         //update the row
         row["col1"] = "v2";
         await _tables.InsertOrReplaceAsync(_tableName, new[] { row });

         //check it's updated
         row = await _tables.GetSingleAsync(_tableName, "pk", "rk");
         Assert.Equal("v2", (string)row["col1"]);
      }

      [Fact]
      public async Task Get_NullTablePartition_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.GetAsync(null, new Key("p", null)));
      }

      [Fact]
      public async Task Get_TableButNullPartition_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.GetSingleAsync(_tableName, null));
      }

      [Fact]
      public async Task Get_TableNullPartitionRowKey_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.GetSingleAsync(_tableName, null, "rk"));
      }

      [Fact]
      public async Task Get_NullTablePartitionRowKey_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.GetSingleAsync(null, "pk", "rk"));
      }

      [Fact]
      public async Task Get_TablePartitionNullRowKey_ArgumentNullException()
      {
         await Assert.ThrowsAsync<ArgumentNullException>(() => _tables.GetSingleAsync(_tableName, "pk", null));
      }

      [Fact]
      public async Task Get_NonExistingTable_EmptyNotNull()
      {
         IEnumerable<Value> rows = await _tables.GetAsync(_tableName, new Key("pk", null));

         Assert.NotNull(rows);
         Assert.True(!rows.Any());
      }

      [Fact]
      public async Task Get_WriteToTwoPartitions_GetsAll()
      {
         var row1 = new Value("pk1", "rk1");
         var row2 = new Value("pk2", "rk2");

         await _tables.InsertAsync(_tableName, new[] { row1, row2 });

         IReadOnlyCollection<Value> rows1 = await _tables.GetAsync(_tableName, new Key("pk1", null));
         IReadOnlyCollection<Value> rows2 = await _tables.GetAsync(_tableName, new Key("pk2", null));

         Assert.True(rows1.Count >= 1);
         Assert.True(rows2.Count >= 1);
      }

      [Fact]
      public async Task Get_AddTwoRows_ReturnsTheOne()
      {
         var row1 = new Value("part1", "1")
         {
            ["col1"] = "value1"
         };
         var row2 = new Value("part1", "2")
         {
            ["col1"] = "value2"
         };
         await _tables.InsertAsync(_tableName, new[] { row1, row2 });

         Value theOne = await _tables.GetSingleAsync(_tableName, "part1", "2");
         Assert.Equal("part1", theOne.PartitionKey);
         Assert.Equal("2", theOne.RowKey);
      }

      [Fact]
      public async Task Get_TwoPartitions_ReadSeparately()
      {
         var row1 = new Value("pk1", "rk1");
         var row2 = new Value("pk2", "rk1");

         await _tables.InsertAsync(_tableName, new[] { row1, row2 });

         Value row11 = await _tables.GetSingleAsync(_tableName, "pk1", "rk1");
         Value row22 = await _tables.GetSingleAsync(_tableName, "pk2", "rk1");

         Assert.NotNull(row11);
         Assert.NotNull(row22);
         Assert.Equal("pk1", row11.PartitionKey);
         Assert.Equal("rk1", row11.RowKey);
         Assert.Equal("pk2", row22.PartitionKey);
         Assert.Equal("rk1", row22.RowKey);
      }

      [Fact]
      public async Task Get_PartitionDoesntExist_EmptyCollection()
      {
         IEnumerable<Value> rows = await _tables.GetAsync(_tableName, new Key(Guid.NewGuid().ToString(), null));

         Assert.NotNull(rows);
         Assert.Empty(rows);
      }

      [Fact]
      public async Task Get_RowDoesntExist_Null()
      {
         await _tables.InsertAsync(_tableName, new[] { new Value("pk", "rk1") });

         Value row = await _tables.GetSingleAsync(_tableName, "pk", Guid.NewGuid().ToString());

         Assert.Null(row);
      }

      [Fact]
      public async Task Get_RowsDontExist_EmptyCollection()
      {
         await _tables.InsertAsync(_tableName, new[] { new Value("pk", "rk1") });
         await _tables.DeleteAsync(_tableName, new[] { new Key("pk", "rk1") });

         IReadOnlyCollection<Value> rows = await _tables.GetAsync(_tableName, new Key("pk", null));
         Assert.NotNull(rows);
         Assert.Equal(0, rows.Count);
      }
   }
}