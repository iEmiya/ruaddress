using System.Collections.Generic;
using System.IO;
using System.Text;
using RUAddress.Storage;
using SocialExplorer.IO.FastDBF;

namespace RUAddress.Source
{
    internal sealed class KLADR : _Base
    {
        public static readonly string FileName = "KLADR.DBF";

        public static IEnumerable<AddressPartIndexWrite> ParseItems(Stream stream)
        {
            DbfFile dbfFile = new DbfFile(Encoding.GetEncoding(866));
            dbfFile.Open(stream);
            try
            {
                DbfHeader header = dbfFile.Header;
                dynamic headerObj = new
                {
                    NAME = FindColumnOrThrow(header, "NAME", maxLen: 40),
                    SOCR = FindColumnOrThrow(header, "SOCR", maxLen: 10),
                    CODE = FindColumnOrThrow(header, "CODE", maxLen: 13),
                    INDEX = FindColumnOrThrow(header, "INDEX", maxLen: 6),
//                    GNINMB = FindColumnOrThrow(header, "GNINMB", maxLen: 4),
//                    UNO = FindColumnOrThrow(header, "UNO", maxLen: 4),
//                    OCATD = FindColumnOrThrow(header, "OCATD", maxLen: 11),
//                    STATUS = FindColumnOrThrow(header, "STATUS", maxLen: 1),
                };

                IEnumerable<DbfRecord> records = EnumRecords(dbfFile, header);

                foreach (var record in records)
                {
                    AddressPartIndexWrite d = EnumRecord(headerObj, record);
                    if (d != null) yield return d;
                }
            }
            finally
            {
                dbfFile.Close();
            }
        }
    }
}
