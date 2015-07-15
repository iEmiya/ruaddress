using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using RUAddress.Storage;
using SocialExplorer.IO.FastDBF;

namespace RUAddress.Source
{
    internal abstract class _Base
    {
        private const string ClassName = "[DBFBase] ";

        internal readonly static Regex INDEX = new Regex(@"^\d{6}$", RegexOptions.Compiled);
        internal readonly static Regex CODE = new Regex(@"^(\d{13}|\d{17})$", RegexOptions.Compiled);
        internal const int NoLevel = -1;
        internal const int TopLevel = 1;
        internal const int LastLevel = 5;
        private readonly static Dictionary<Regex, int> LEVELS = new Dictionary<Regex, int>()
        {
            {new Regex(@"^\d{2}0000000000000$",  RegexOptions.Compiled),         1}, // СС – код субъекта Российской Федерации (региона);
            {new Regex(@"^\d{5}0000000000$",     RegexOptions.Compiled),         2}, // РРР – код района;
            {new Regex(@"^\d{8}0000000$",        RegexOptions.Compiled),         3}, // ГГГ – код города;
            {new Regex(@"^\d{11}0000$",          RegexOptions.Compiled),         4}, // ППП – код населенного пункта;
            {new Regex(@"^\d{15}$",              RegexOptions.Compiled), LastLevel}, // УУУУ – код улицы;
        };

        internal static int? GetLevel(string code)
        {
            foreach (var l in LEVELS)
            {
                if (l.Key.IsMatch(code)) return l.Value;
            }
            return null;
        }

        /// <exception cref="Exception">Если файл не содержит необходимого столбца или имеет не верный формат</exception>
        protected static int FindColumnOrThrow(DbfHeader header, String columnName,
                DbfColumn.DbfColumnType? columnType = DbfColumn.DbfColumnType.Character,
                int? maxLen = null)
        {
            var i = header.FindColumn(columnName);
            if (i < 0) throw new Exception(ClassName + "Отсутсвует столбец " + columnName);
            var column = header[i];
            if (columnType.HasValue && column.ColumnType != columnType)
                throw new Exception(String.Format(ClassName + "Тип столбца {0} должен быть {1}", columnName, columnType));
            if (maxLen.HasValue && column.Length > maxLen)
                throw new Exception(String.Format(ClassName + "Длина столбца {0} не должна превышать {1}", columnName, maxLen));
            return i;
        }

        protected static IEnumerable<DbfRecord> EnumRecords(DbfFile file, DbfHeader header)
        {
            var record = new DbfRecord(header);
            while (file.ReadNext(record))
            {
                yield return record;
            }
        }

        protected static AddressPartIndexWrite EnumRecord(dynamic headerObj, DbfRecord record)
        {
            string NAME = record[headerObj.NAME].Trim();
            string SOCR = record[headerObj.SOCR].Trim();
            string CODE = record[headerObj.CODE].Trim();
            string INDEX = record[headerObj.INDEX].Trim();
            //                    string GNINMB = record[headerObj.GNINMB].Trim();
            //                    string UNO = record[headerObj.UNO].Trim();
            //                    string OCATD = record[headerObj.OCATD].Trim();
            //                    string STATUS = record[headerObj.STATUS].Trim();

            if (!string.IsNullOrEmpty(INDEX) && !_Base.INDEX.IsMatch(INDEX))
            {
                AddressLoader.Log.Warn(ClassName + "Значение: {0} не может быть почтовым индексом", INDEX);
                return null;
            }

            if (!_Base.CODE.IsMatch(CODE))
            {
                AddressLoader.Log.Warn(ClassName + "Значение: {0} не может быть кодом", CODE);
                return null;
            }
            if (CODE.Length == 13)
            {
                // не актуальные данные
                if (CODE[11] != '0' || CODE[12] != '0') return null;

                CODE = CODE.Substring(0, 11) + "0000";
            }
            else if (CODE.Length == 17)
            {
                // не актуальные данные
                if (CODE[15] != '0' || CODE[16] != '0') return null;

                CODE = CODE.Substring(0, 15);
            }
            else
            {
                AddressLoader.Log.Warn(ClassName + "Значение: {0} не может быть кодом", CODE);
                return null;
            }

            int level = _Base.NoLevel;
            foreach (var l in _Base.LEVELS)
            {
                if (l.Key.IsMatch(CODE))
                {
                    level = l.Value;
                    break;
                }
            }

            if (level == _Base.NoLevel)
            {
                AddressLoader.Log.Warn(ClassName + "Для кода: {0} не найден уровень в КЛАДР", CODE);
                return null;
            }

            AddressPartIndexWrite d = new AddressPartIndexWrite();
            d.PostalCode = INDEX;
            d.Id = CODE;
            d.Level = level;
            d.Reduction = SOCR;
            d.Name = NAME;

            return d;
        }
    }
}
