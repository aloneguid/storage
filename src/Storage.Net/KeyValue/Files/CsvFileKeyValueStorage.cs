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
      public Task<IReadOnlyCollection<TableRow>> GetAsync(string tableName, string partitionKey)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (partitionKey == null) throw new ArgumentNullException(nameof(partitionKey));

         return Task.FromResult(InternalGet(tableName, partitionKey, null));
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task<TableRow> GetAsync(string tableName, string partitionKey, string rowKey)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (partitionKey == null) throw new ArgumentNullException(nameof(partitionKey));
         if (rowKey == null) throw new ArgumentNullException(nameof(rowKey));

         return Task.FromResult(InternalGet(tableName, partitionKey, rowKey)?.FirstOrDefault());
      }

      private IReadOnlyCollection<TableRow> InternalGet(string tableName, string partitionKey, string rowKey)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));

         IEnumerable<string> partitions = partitionKey == null
            ? GetAllPartitionNames(tableName)
            : new List<string> { partitionKey };

         var result = new List<TableRow>();

         foreach(string partition in partitions)
         {
            Dictionary<string, TableRow> rows = ReadPartition(tableName, partition, rowKey);
            if(rows == null) continue;

            if(rowKey != null)
            {
               if (rows.TryGetValue(rowKey, out TableRow row))
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
      public Task InsertAsync(string tableName, IEnumerable<TableRow> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (rows == null) throw new ArgumentNullException(nameof(rows));

         OperateRows(tableName, rows, (p, g) => Insert(p, g, true, true));

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface
      /// </summary>
      public Task InsertOrReplaceAsync(string tableName, IEnumerable<TableRow> rows)
      {
         if (tableName == null) throw new ArgumentNullException(nameof(tableName));
         if (rows == null) throw new ArgumentNullException(nameof(rows));

         OperateRows(tableName, rows, (p, g) => Insert(p, g, true, false));
         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task UpdateAsync(string tableName, IEnumerable<TableRow> rows)
      {
         throw new NotImplementedException();
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task MergeAsync(string tableName, IEnumerable<TableRow> rows)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(rows == null) return Task.FromResult(true);

         foreach(IGrouping<string, TableRow> group in rows.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, TableRow> partition = ReadPartition(tableName, partitionKey);
            if(partition == null) partition = new Dictionary<string, TableRow>();
            Merge(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public Task DeleteAsync(string tableName, IEnumerable<Key> rowIds)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(rowIds == null) return Task.FromResult(true);

         foreach(IGrouping<string, Key> group in rowIds.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, TableRow> partition = ReadPartition(tableName, partitionKey);
            if(partition == null) partition = new Dictionary<string, TableRow>();
            Delete(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }

         return Task.FromResult(true);
      }

      /// <summary>
      /// See interface documentation
      /// </summary>
      public IEnumerable<TableRow> Get(string tableName, string partitionKey, string rowKey, int maxRecords)
      {
         throw new NotImplementedException();
      }

      #region [ Big Data Processing ]

      private void OperateRows(string tableName, IEnumerable<TableRow> rows,
         Action<Dictionary<string, TableRow>, IEnumerable<TableRow>> partitionAction)
      {
         foreach (IGrouping<string, TableRow> group in rows.GroupBy(r => r.PartitionKey))
         {
            string partitionKey = group.Key;

            Dictionary<string, TableRow> partition = ReadPartition(tableName, partitionKey) ??
                                                     new Dictionary<string, TableRow>();
            partitionAction(partition, group);
            WritePartition(tableName, partitionKey, partition.Values);
         }
      }

      private void Insert(Dictionary<string, TableRow> data, IEnumerable<TableRow> rows,
         bool detectInputDuplicates, bool detectDataDuplicates)
      {
         var rowsList = rows.ToList();
         if (rowsList.Count == 0) return;

         if (detectInputDuplicates && !TableRow.AreDistinct(rowsList))
         {
            throw new StorageException(ErrorCode.DuplicateKey, null);
         }

         if(detectDataDuplicates && rowsList.Any(r => data.ContainsKey(r.RowKey)))
         {
            throw new StorageException(ErrorCode.DuplicateKey, null);
         }

         foreach (TableRow row in rowsList)
         {
            data[row.RowKey] = row;
         }
      }

      private void Merge(Dictionary<string, TableRow> data, IEnumerable<TableRow> rows)
      {
         foreach(TableRow row in rows)
         {
            if (!data.TryGetValue(row.RowKey, out TableRow prow))
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

      private void Delete(Dictionary<string, TableRow> data, IEnumerable<Key> rows)
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

      private void WritePartition(string tableName, string partitionName, IEnumerable<TableRow> rows)
      {
         if(tableName == null) throw new ArgumentNullException(nameof(tableName));
         if(partitionName == null) throw new ArgumentNullException(nameof(partitionName));
         if(rows == null) throw new ArgumentNullException(nameof(rows));

         var rowsList = new List<TableRow>(rows);

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
            foreach(TableRow row in rowsList)
            {
               FillWriteableRow(row, writeableRow, columnNames);
               writer.Write(writeableRow);
            }
         }
      }

      private static void FillWriteableRow(TableRow row, List<string> writeableRow, string[] allColumnNames)
      {
         writeableRow[0] = row.RowKey;

         for(int i = 1; i < allColumnNames.Length; i++)
         {
            string name = allColumnNames[i];
            if (row.TryGetValue(name, out object cell))
            {
               writeableRow[i] = cell?.ToString();
            }
            else
            {
               writeableRow[i] = string.Empty;
            }
         }
      }

      private static string[] GetAllColumnNames(List<TableRow> rows)
      {
         //collect all possible column names
         var allColumns = new HashSet<string>();
         foreach(TableRow row in rows)
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

      private Dictionary<string, TableRow> ReadPartition(string tableName, string partitionName, string stopOnRowKey = null)
      {
         using(Stream s = OpenTablePartition(tableName, partitionName, false))
         {
            if(s == null) return null;

            var result = new Dictionary<string, TableRow>();

            var reader = new CsvReader(s, Encoding.UTF8);
            string[] allColumns = reader.ReadNextRow()?.ToArray();
            if(allColumns == null) return null;
            allColumns = allColumns.Select(c => c.Trim('\r')).ToArray();

            TableRow row;
            while((row = ReadNextRow(reader, partitionName, allColumns)) != null)
            {
               result[row.RowKey] = row;
               if (stopOnRowKey != null && row.RowKey == stopOnRowKey) break;
            }

            return result;
         }
      }

      private static TableRow ReadNextRow(CsvReader reader, string partitionKey, string[] allColumns)
      {
         string[] values = reader.ReadNextRow()?.ToArray();
         if(values == null || values.Length == 0) return null;

         var row = new TableRow(partitionKey, values[0]);

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
