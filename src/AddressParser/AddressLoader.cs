using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NLog;
using RUAddress.Source;
using RUAddress.Storage;
using SharpCompress.Archive;

namespace RUAddress
{
    public sealed class AddressLoader
    {
        private const string ClassName = "[AddressLoader] ";

        internal static Logger Log = LogManager.GetCurrentClassLogger();

        private const string Base = "BASE.7Z";

        private static readonly string[] FilesToCheck = new[]
        {
            Base,
        };

        private static readonly string[] FilesIntoBase = new[]
        {
//            "ALTNAMES.DBF",
//            "DOMA.DBF",
//            "FLAT.DBF",
            KLADR.FileName,
            SOCRBASE.FileName,
            STREET.FileName,
        };


        private readonly Logger _log;

        public AddressLoader(Logger log)
        {
            _log = Log = log;
        }

        public void ReplaceDb(string folderWithBase7z, string folderWithLucene)
        {
            _log.Trace(ClassName + "Проверяем каталог: '{0}' на наличие необходимых файлов", folderWithBase7z);
            if (!FilesToCheck.All(fileName => File.Exists(Path.Combine(folderWithBase7z, fileName))))
                throw new ApplicationException(String.Format("Каталог: '{0}'  не содержит необходимых файлов из КЛАДР", folderWithBase7z));

            string folderPath = Path.Combine(folderWithBase7z, Base);
            using (FileStream stream = new FileStream(folderPath, FileMode.Open))
            using (IArchive archive = ArchiveFactory.Open(stream))
            {
                IList<string> filesInZip = new List<string>();
                foreach (var entry in archive.Entries)
                {
                    if (entry.IsDirectory) continue;
                    filesInZip.Add(entry.Key);
                }

                bool isSupport = FilesIntoBase.All(fileName => filesInZip.Contains(fileName));
                if (!isSupport)
                    throw new ApplicationException(ClassName + "При работе с файлом адресов не были найдены все файлы");

                _log.Trace(ClassName + String.Format("Файл: '{0}' содержит все необходимые таблицы:", folderPath));
                foreach (var fileZip in filesInZip)
                {
                    _log.Trace("\t{0}", fileZip);
                }

                using (var source = new LuceneDataSource(_log, folderWithLucene))
                {
                    source.Clear();

                    foreach (var entry in archive.Entries)
                    {
                        if (entry.IsDirectory) continue;

                        if (entry.Key.Equals(KLADR.FileName))
                        {
                            var list = KLADR.ParseItems(entry.OpenEntryStream());
                            source.Add(list);
                        }

                        if (entry.Key.Equals(SOCRBASE.FileName))
                        {
                            var list = SOCRBASE.ParseItems(entry.OpenEntryStream()).ToList();
                            source.Add(list);

                            var path = Path.Combine(folderWithLucene, SOCRBASE.FileName);
                            path = Path.ChangeExtension(path, ".csv");
                            SOCRBASE.SaveToCSV(list, path);
                        }

                        if (entry.Key.Equals(STREET.FileName))
                        {
                            var list = STREET.ParseItems(entry.OpenEntryStream());
                            source.Add(list);
                        }
                    }

                    source.Build();
                }
            }
        }
    }
}
