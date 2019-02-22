using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;

namespace Storage.Net.Tests.Integration.Utils
{
    public static class Utility
    {
        public static void ClearTestData(string fileProjectRelativePath)
        {
            var fileName = fileProjectRelativePath.Split(Path.DirectorySeparatorChar).LastOrDefault();
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }
        }

        public static void PrepareTestData(string fileProjectRelativePath)
        {
            var filePath = fileProjectRelativePath.Replace("/", @"\");
            var fileName = fileProjectRelativePath.Split(Path.DirectorySeparatorChar).LastOrDefault();

            var environmentDir = new DirectoryInfo(Environment.CurrentDirectory);

            var itemPathUri = new Uri(Path.Combine(environmentDir?.Parent?.Parent?.Parent?.FullName, filePath));
            var itemPath = itemPathUri.LocalPath;

            var binFolderPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            var itemPathInBinUri = new Uri(Path.Combine(binFolderPath, fileName));
            var itemPathInBin = itemPathInBinUri.LocalPath;

            if (File.Exists(itemPath) && !File.Exists(itemPathInBin))
            {
                File.Copy(itemPath, itemPathInBin);
            }
        }

    }
}
