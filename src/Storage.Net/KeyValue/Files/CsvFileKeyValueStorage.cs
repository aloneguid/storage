using NetBox.Extensions;
using NetBox.FileFormats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.Net.KeyValue.Files
{
   /// <summary>
   /// Creates an abstaction of <see cref="IKeyValueStorage"/> in a CSV file structure.
   /// Works relative to the root directory specified in the constructor.
   /// Each table will be a separate subfolder, where files are partitions.
   /// </summary>
   public class CsvFileKeyValueStorage : IKeyValueStorage
   {
      private const string TablePartitionFormat = "{0}.partition.csv";
      private const string TablePartitionSearchFilter = "*.partition.csv";
      private const string TablePartitionSuffix = ".partition.csv";
      private const string RowKeyName = "RowKey";
      private const string TableNsFormat = "{0}.table";
      private const string TableNamesSuffix = ".table";
      private const string TableNamesSearchPattern = "*.table";
      private readonly DirectoryInfo _rootDir;
      private readonly string _rootDirPath;

      /// <summary>
      /// Creates a new instance of CSV file storage
      /// </summary>
      /// <param name="rootDir"></param>
      /// <exception cref="ArgumentNullException"></exception>
      public CsvFileKeyValueStorage(DirectoryInfo rootDir)
      {
         _rootDir = rootDir ?? throw new ArgumentNullException(nameof(rootDir));
         _rootDirPath = rootDir.FullName;
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public bool HasOptimisticConcurrency => false;

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task<IReadOnlyCollection<string>> ListTableNamesAsync()
      {
         return Task.FromResult<IReadOnlyCollection<string>>(_rootDir
            .GetDirectories(TableNamesSearchPattern, SearchOption.TopDirectoryOnly)
            .Select(d => d.Name.Substring(0, d.Name.Length - TableNamesSuffix.Length))
            .ToList());
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task DeleteAsync(string tableName)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));

         DirectoryInfo table = OpenTable(tableName, false);
         table?.Delete(true);

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task<IReadOnlyCollection<Value>> GetAsync(string tableName, Key key)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (key == null) throw new ArgumentNullException(nameof(key));

         return Task.FromResult(InternalGet(tableName, key));
      }

      private IReadOnlyCollection<Value> InternalGet(string tableName, Key key)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));

         IEnumerable<string> partitions = key.PartitionKey == null
            ? GetAllPartitionNames(tableName)
            : new List<string> { key.PartitionKey };

         var result = new List<Value>();

         foreach(string partition in partitions)
         {
            Dictionary<string, Value> rows = ReadPartition(tableName, partition, key.RowKey);
            if(rows == null) continue;

            if(key.RowKey != null)
            {
               if (rows.TryGetValue(key.RowKey, out Value row))
               {
                  result.Add(row);
                  break;
               }
            }
            else
            {
               result.AddRange(rows.Values);
            }
         }

         return result;
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task InsertAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (values == null) throw new ArgumentNullException(nameof(values));

         OperateRows(tableName, values, (p, g) => Insert(p, g, true, true));

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task InsertOrReplaceAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (values == null) throw new ArgumentNullException(nameof(values));

         OperateRows(tableName, values, (p, g) => Insert(p, g, true, false));
         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task UpdateAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task MergeAsync(string tableName, IReadOnlyCollection<Value> values)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(values == null) return Task.FromResult(true);

         foreach(IGrouping<string, Value> group in values.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, Value> partition = ReadPartition(tableName, partitionKey);
            if(partition == null) partition = new Dictionary<string, Value>();
            Merge(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task DeleteAsync(string tableName, IReadOnlyCollection<Key> rowIds)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(rowIds == null) return Task.FromResult(true);

         foreach(IGrouping<string, Key> group in rowIds.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, Value> partition = ReadPartition(tableName, partitionKey);
            if(partition == null) partition = new Dictionary<string, Value>();
            Delete(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public IEnumerable<Value> Get(string tableName, string partitionKey, string rowKey, int maxRecords)
      {
         throw new NotImplementedException();
      }

      #region [ Big Data Processing ]

      private void OperateRows(string tableName, IEnumerable<Value> rows,
         Action<Dictionary<string, Value>, IEnumerable<Value>> partitionAction)
      {
         foreach (IGrouping<string, Value> group in rows.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, Value> partition = ReadPartition(tableName, partitionKey) ??
                                                     new Dictionary<string, Value>();
            partitionAction(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }
      }

      private void Insert(Dictionary<string, Value> data, IEnumerable<Value> rows,
         bool detectInputDuplicates, bool detectDataDuplicates)
      {
         var rowsList = rows.ToList();
         if (rowsList.Count == 0) return;

         if (detectInputDuplicates && !Value.AreDistinct(rowsList))
         {
            throw new StorageException(ErrorCode.DuplicateKey, null);
         }

         if(detectDataDuplicates && rowsList.Any(r => data.ContainsKey(r.RowKey)))
         {
            throw new StorageException(ErrorCode.DuplicateKey, null);
         }

         foreach (Value row in rowsList)
         {
            data[row.RowKey] = row;
         }
      }

      private void Merge(Dictionary<string, Value> data, IEnumerable<Value> rows)
      {
         foreach(Value row in rows)
         {
            if (!data.TryGetValue(row.RowKey, out Value prow))
            {
               data[row.RowKey] = row;
            }
            else
            {
               foreach (KeyValuePair<string, object> line in row)
               {
                  prow[line.Key] = line.Value;
               }
            }
         }
      }

      private void Delete(Dictionary<string, Value> data, IEnumerable<Key> rows)
      {
         foreach(Key row in rows)
         {
            data.Remove(row.RowKey);
         }
      }

      #endregion

      #region [ Disk Partitioning ]

      private DirectoryInfo OpenTable(string name, bool createIfNotExists)
      {
         if(createIfNotExists)
         {
            if(!_rootDir.Exists) _rootDir.Create();

            var result = new DirectoryInfo(Path.Combine(_rootDirPath, string.Format(TableNsFormat, name).SanitizePath()));
            if(!result.Exists) result.Create();

            return result;
         }

         if(!_rootDir.Exists) return null;
         var dir = new DirectoryInfo(Path.Combine(_rootDirPath, string.Format(TableNsFormat, name).SanitizePath()));
         if(!dir.Exists) return null;
         return dir;
      }

      private Stream OpenTablePartition(string tableName, string partitionName, bool createIfNotExists)
      {
         DirectoryInfo fs = OpenTable(tableName, createIfNotExists);
         if(fs == null) return null;

         string partitionPath = Path.Combine(fs.FullName,
            string.Format(TablePartitionFormat, partitionName).SanitizePath());

         if(!File.Exists(partitionPath))
         {
            if(!createIfNotExists) return null;
            return File.OpenWrite(partitionPath);
         }

         return File.Open(partitionPath, FileMode.Open, FileAccess.ReadWrite);
      }

      private IEnumerable<string> GetAllPartitionNames(string tableName)
      {
         DirectoryInfo fs = OpenTable(tableName, false);

         FileInfo[] partFiles = fs?.GetFiles(TablePartitionSearchFilter, SearchOption.TopDirectoryOnly);
         if(partFiles == null || partFiles.Length == 0) return Enumerable.Empty<string>();

         return partFiles.Select(d => d.Name.Substring(0, d.Name.Length - TablePartitionSuffix.Length));
      }

      #endregion

      #region [ CSV Read & Write ]

      private void WritePartition(string tableName, string partitionName, IEnumerable<Value> rows)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(partitionName == null) throw new ArgumentNullException(nameof(partitionName));
         if(rows == null) throw new ArgumentNullException(nameof(rows));

         var rowsList = new List<Value>(rows);

         using(Stream s = OpenTablePartition(tableName, partitionName, true))
         {
            s.Seek(0, SeekOrigin.Begin);
            s.SetLength(0);
            var writer = new CsvWriter(s, Encoding.UTF8);

            string[] columnNames = GetAllColumnNames(rowsList);

            //write header
            writer.Write(columnNames);

            //write data
            var writeableRow = new List<string>(columnNames.Length);
            writeableRow.AddRange(Enumerable.Repeat(string.Empty, columnNames.Length));
            foreach(Value row in rowsList)
            {
               FillWriteableRow(row, writeableRow, columnNames);
               writer.Write(writeableRow);
            }
         }
      }

      private static void FillWriteableRow(Value row, List<string> writeableRow, string[] allColumnNames)
      {
         writeableRow[0] = row.RowKey;

         for(int i = 1; i < allColumnNames.Length; i++)
         {
            string name = allColumnNames[i];
            if (row.TryGetValue(name, out object cell))
            {
               string s;

               if (cell is DateTime dt)
               {
                  s = dt.ToUniversalTime().ToString("s") + "Z";
               }
               else
               {
                  s = cell?.ToString();
               }

               writeableRow[i] = s;
            }
            else
            {
               writeableRow[i] = string.Empty;
            }
         }
      }

      private static string[] GetAllColumnNames(List<Value> rows)
      {
         //collect all possible column names
         var allColumns = new HashSet<string>();
         foreach(Value row in rows)
         {
            foreach(string col in row.Keys)
            {
               allColumns.Add(col);
            }
         }

         var columns = new List<string>(allColumns);
         columns.Sort();
         columns.Insert(0, RowKeyName);
         return columns.ToArray();
      }

      private Dictionary<string, Value> ReadPartition(string tableName, string partitionName, string stopOnRowKey = null)
      {
         using(Stream s = OpenTablePartition(tableName, partitionName, false))
         {
            if(s == null) return null;

            var result = new Dictionary<string, Value>();

            var reader = new CsvReader(s, Encoding.UTF8);
            string[] allColumns = reader.ReadNextRow()?.ToArray();
            if(allColumns == null) return null;
            allColumns = allColumns.Select(c => c.Trim('\r')).ToArray();

            Value row;
            while((row = ReadNextRow(reader, partitionName, allColumns)) != null)
            {
               result[row.RowKey] = row;
               if (stopOnRowKey != null && row.RowKey == stopOnRowKey) break;
            }

            return result;
         }
      }

      private static Value ReadNextRow(CsvReader reader, string partitionKey, string[] allColumns)
      {
         string[] values = reader.ReadNextRow()?.ToArray();
         if(values == null || values.Length == 0) return null;

         var row = new Value(partitionKey, values[0]);

         for(int i = 1; i < values.Length; i++)
         {
            string value = values[i];
            if(string.IsNullOrEmpty(value)) continue;

            row[allColumns[i]] = value;
         }

         return row;
      }


      #endregion

      /// <summary>
      /// Does nothing as no handles are kept open
      /// </summary>
      public void Dispose()
      {
      }
   }
}
