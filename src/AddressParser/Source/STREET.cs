using System.Collections.Generic;
using System.IO;
using System.Text;
using RUAddress.Storage;
using SocialExplorer.IO.FastDBF;

namespace RUAddress.Source
{
    internal sealed class STREET : _Base
    {
        public static readonly string FileName = "STREET.DBF";

        public static IEnumerable<AddressPartIndexWrite> ParseItems(Stream ms)
        {
            DbfFile dbfFile = new DbfFile(Encoding.GetEncoding(866));
            dbfFile.Open(ms);
            try
            {
                DbfHeader header = dbfFile.Header;
                dynamic headerObj = new
                {
                    NAME = FindColumnOrThrow(header, "NAME", maxLen: 40),
                    SOCR = FindColumnOrThrow(header, "SOCR", maxLen: 10),
                    CODE = FindColumnOrThrow(header, "CODE", maxLen: 17),
                    INDEX = FindColumnOrThrow(header, "INDEX", maxLen: 6),
//                    GNINMB = FindColumnOrThrow(header, "GNINMB", maxLen: 4),
//                    UNO = FindColumnOrThrow(header, "UNO", maxLen: 4),
//                    OCATD = FindColumnOrThrow(header, "OCATD", maxLen: 11),
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
