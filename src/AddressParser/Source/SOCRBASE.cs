using System.Collections.Generic;
using System.IO;
using System.Text;
using RUAddress.Storage;
using SocialExplorer.IO.FastDBF;

namespace RUAddress.Source
{
    internal sealed class SOCRBASE : _Base
    {
        public static readonly string FileName = "SOCRBASE.DBF";

        public static IEnumerable<AddressPartReduction> ParseItems(Stream ms)
        {
            DbfFile dbfFile = new DbfFile(Encoding.GetEncoding(866));
            dbfFile.Open(ms);
            try
            {
                DbfHeader header = dbfFile.Header;
                dynamic headerObj = new
                {
                    LEVEL = FindColumnOrThrow(header, "LEVEL", maxLen: 5),
                    SCNAME = FindColumnOrThrow(header, "SCNAME", maxLen: 10),
                    SOCRNAME = FindColumnOrThrow(header, "SOCRNAME", maxLen: 29),
//                    KOD_T_ST = FindColumnOrThrow(header, "KOD_T_ST", maxLen: 3),
                };

                IEnumerable<DbfRecord> records = EnumRecords(dbfFile, header);

                foreach (var record in records)
                {
                    var item = new AddressPartReduction()
                    {
                        Level = int.Parse(record[headerObj.LEVEL].Trim()),
                        Short = record[headerObj.SCNAME].Trim(),
                        Name = record[headerObj.SOCRNAME].Trim(),
//                        KOD_T_ST = record[headerObj.KOD_T_ST].Trim(),
                    };
                    yield return item;
                }
            }
            finally
            {
                dbfFile.Close();
            }
        }

        public static void SaveToCSV(IEnumerable<AddressPartReduction> list, string path)
        {
            // Копируем сокращения как есть из DBF в CSV
            if (File.Exists(path)) File.Delete(path);

            var dataSource = new CsvDataSource(AddressLoader.Log);
            using (var sw = new StreamWriter(path))
            {
                var writer = dataSource.GetWriter(sw);
                writer.WriteHeader<AddressPartReduction>();
                writer.WriteRecords(list);
            }
        }

        public static IList<AddressPartReduction> LoadFromCSV(string path)
        {
            var list = new List<AddressPartReduction>();
            var dataSource = new CsvDataSource(AddressLoader.Log);
            using (var sr = new StreamReader(path))
            {
                var reader = dataSource.GetReader(sr);
                IEnumerable<AddressPartReduction> records = reader.GetRecords<AddressPartReduction>();
                list.AddRange(records);
            }
            return list;
        }

        public string LEVEL { get; set; }
        public string SCNAME { get; set; }
        public string SOCRNAME { get; set; }
//        public string KOD_T_ST { get; set; }

    }
}
