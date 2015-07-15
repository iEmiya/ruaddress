using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using NLog;
using RUAddress.Source;
using Version = Lucene.Net.Util.Version;

namespace RUAddress.Storage
{
    internal sealed class LuceneDataSource : IDisposable
    {
        private const string ClassName = "[LuceneDataSource] ";

        private readonly Logger _log;
        private readonly string _indexDirectory;
        private readonly StandardAnalyzer _analyzer;

        internal LuceneDataSource(Logger log, string indexDirectory)
        {
            _log = log;
            _indexDirectory = indexDirectory;
            _analyzer = new StandardAnalyzer(Version.LUCENE_30);
        }

        public void Dispose()
        {
            if (_searcher != null)
            {
                _searcher.Dispose();
                _fsDirectory.Dispose();
            }
            _searcher = null;
            _fsDirectory = null;
        }

        #region ReplaceDb

        private IndexWriteRamStorage _indexWrite;
        private ReductionRamStorage _reduction;

        public void Clear()
        {
            using (var fsDirectory = FSDirectory.Open(_indexDirectory))
            using (var writer = new IndexWriter(fsDirectory, _analyzer, true, IndexWriter.MaxFieldLength.LIMITED))
                writer.DeleteAll();

            _indexWrite = new IndexWriteRamStorage();
            _reduction = new ReductionRamStorage(_indexDirectory);
        }

        public void Add(IEnumerable<AddressPartIndexWrite> list)
        {
            _indexWrite.Add(list);
        }

        public void Add(IEnumerable<AddressPartReduction> list)
        {
            _reduction.Add(list);
        }

        public void Build()
        {
            foreach (var d in _indexWrite.Sort[1].Values)
            {
                d.SearchName = d.Name.ToLower();
                SetNameWithReduction(d);
                d.FullName = d.NameWithReduction;
            }

            for (int level = 2; level <= 5; level++)
            {
                foreach (var d in _indexWrite.Sort[level].Values)
                {
                    var key = AddressParser.GetParentCode(level, d.Id);
                    if (!_indexWrite.Index.ContainsKey(key))
                    {
                        _log.Warn(ClassName + String.Format("Не найден родитель с кодом: '{0}' для элемента: '{1}'", key, d.Id));
                        continue;
                    }
                    AddressPartIndexWrite p = _indexWrite.Index[key];

                    d.SearchName = p.SearchName + " " + d.Name.ToLower();
                    SetNameWithReduction(d);
                    d.FullName = d.NameWithReduction + ", " + p.FullName;
                }
            }

            foreach (var d in _indexWrite.Index.Values)
            {
                if (string.IsNullOrEmpty(d.SearchName)) continue;

                int level = d.Level;
                AddressPartIndexWrite p = d;

                while (level > 1 && string.IsNullOrEmpty(p.PostalCode))
                {
                    p = GetParent(level, d.Id);
                    level--;
                }
                d.SearchName = !string.IsNullOrEmpty(p.PostalCode) ? p.PostalCode + " " + d.SearchName : d.SearchName;
                d.FullName = !string.IsNullOrEmpty(p.PostalCode) ? d.FullName + ", " + p.PostalCode : d.FullName;
            }

            using (var fsDirectory = FSDirectory.Open(_indexDirectory))
            using (var writer = new IndexWriter(fsDirectory, _analyzer, true, IndexWriter.MaxFieldLength.LIMITED))
            {
                writer.UseCompoundFile = true;

                foreach (var d in _indexWrite.Index.Values)
                {
                    var doc = CreateDocument(d);
                    if(doc != null) writer.AddDocument(doc);
                }

                writer.Flush(true, true, true);
                writer.Optimize(true);
                writer.Commit();
            }

            _indexWrite = null;
        }

        private void SetNameWithReduction(AddressPartIndexWrite d)
        {
            d.NameWithShortReduction = GetNameWithReduction(d.Level, d.Id, d.Reduction, d.Name);
            d.NameWithReduction = GetNameWithReduction(d.Level, d.Id, _reduction.Get(d).Name, d.Name);
        }

        private static string GetNameWithReduction(int level, string code, string reduction, string name)
        {
            if (level == 1)
            {
                if ("010000000000000".Equals(code)) return String.Format("{0} {1} ({1})", reduction, name);
                if ("070000000000000".Equals(code)) return String.Format("{1} {0}", reduction, name);
                if ("090000000000000".Equals(code)) return String.Format("{1} {0}", reduction, name);
                if ("160000000000000".Equals(code)) return String.Format("{0} {1} ({1})", reduction, name);
                if ("180000000000000".Equals(code)) return String.Format("{1} {0}", reduction, name);
                if ("200000000000000".Equals(code)) return String.Format("{1} {0}", reduction, name);
                if ("210000000000000".Equals(code)) return String.Format("{1} {0}", reduction, name);
                if ("860000000000000".Equals(code)) return "Ханты-Мансийский автономный округ - Югра";


                if ("Край".Equals(reduction)) return String.Format("{1} {0}", reduction.ToLower(), name);
                if ("Область".Equals(reduction)) return String.Format("{1} {0}", reduction.ToLower(), name);
                if ("Автономная область".Equals(reduction)) return String.Format("{1} {0}", reduction.ToLower(), name);
                if ("Автономный округ".Equals(reduction)) return String.Format("{1} {0}", reduction.ToLower(), name);

                // Города федерального значения
                if ("Москва".Equals(name)) return name;
                if ("Санкт-Петербург".Equals(name)) return name;
                if ("Севастополь".Equals(name)) return name;
                if ("Байконур".Equals(name)) return name;

                return reduction + " " + name;
            }

            return name + ((!string.IsNullOrEmpty(reduction)) ? " " + reduction.ToLower() : "");
        }

        private AddressPartIndexWrite GetParent(int level, string code)
        {
            string key = AddressParser.GetParentCode(level, code);
            return _indexWrite.Index[key];
        }

        private static Document CreateDocument(AddressPartIndexWrite d)
        {
            if (string.IsNullOrEmpty(d.SearchName)) return null;

            var doc = new Document();
            doc.Add(new Field("id", d.Id, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("postalCode", d.PostalCode, Field.Store.YES, Field.Index.ANALYZED));
            doc.Add(new Field("searchName", d.SearchName, Field.Store.YES, Field.Index.ANALYZED));

            doc.Add(new Field("lvl", d.Level.ToString(), Field.Store.YES, Field.Index.NO));
            doc.Add(new Field("reduction", d.Reduction, Field.Store.YES, Field.Index.NO));
            doc.Add(new Field("name", d.Name, Field.Store.YES, Field.Index.NO));
            doc.Add(new Field("name_short_reduction", d.NameWithShortReduction, Field.Store.YES, Field.Index.NO));
            doc.Add(new Field("name_reduction", d.NameWithReduction, Field.Store.YES, Field.Index.NO));
            doc.Add(new Field("fullName", d.FullName, Field.Store.YES, Field.Index.NO));
            return doc;
        }

        #endregion
        #region Search

        internal readonly static Regex SPLIT = new Regex(@"((\b[^\s]+\b)((?<=\.\w).)?)", RegexOptions.Compiled);
        private FSDirectory _fsDirectory;
        private IndexSearcher _searcher;

        private IndexSearcher GetSearcher()
        {
            if (_searcher == null)
            {
                if (!System.IO.Directory.Exists(_indexDirectory)) return null;

                FSDirectory fsDirectory;
                IndexSearcher searcher;
                try
                {
                    fsDirectory = FSDirectory.Open(_indexDirectory);
                    if (fsDirectory == null)
                    {
                        _log.Warn("Failed to open index directory: " + _indexDirectory);
                        return null;
                    }
                    searcher = new IndexSearcher(fsDirectory);
                }
                catch (Exception ignore)
                {
                    _log.Warn(ignore, "Failed to get searcher");
                    return null;
                }
                _fsDirectory = fsDirectory;
                _searcher = searcher;

            }
            return _searcher;
        }

        public IEnumerable<AddressPartIndexSearch> Search(string filter)
        {
            if (string.IsNullOrEmpty(filter)) yield break;
            var queryBuilder = new StringBuilder();
            foreach (Match match in SPLIT.Matches(filter))
            {
                if (queryBuilder.Length > 0) queryBuilder.Append(" ");
                queryBuilder.Append("+" + match.Value.ToLowerInvariant() + "*");
            }

            var queryString = queryBuilder.ToString();
            if (string.IsNullOrEmpty(queryString)) yield break;

            var parser = new QueryParser(Version.LUCENE_30, "searchName", _analyzer);
            Query query = parser.Parse(queryString);

            var searcher = GetSearcher();
            if (searcher == null) yield break;
            TopDocs hits = searcher.Search(query, int.MaxValue);

            int no = 0;
            var totalHits = hits.TotalHits;
            var lastInfo = new ParsedAddressPartInfo();
            while (no < totalHits)
            {
                lastInfo.No = no;
                lastInfo.TotalHits = totalHits;


                Document doc = searcher.Doc(hits.ScoreDocs[no].Doc);

                AddressPartIndexSearch d = new AddressPartIndexSearch();
                d.Id = doc.Get("id");
                d.PostalCode = doc.Get("postalCode");

                d.Level = int.Parse(doc.Get("lvl"));
                d.Reduction = doc.Get("reduction");
                d.Name = doc.Get("name");
                d.NameWithShortReduction = doc.Get("name_short_reduction");
                d.NameWithReduction = doc.Get("name_reduction");
                d.FullName = doc.Get("fullName");

                d.LastInfo = lastInfo;

                yield return d;
                no++;
            }
        }

        public ParsedAddress GetByCode(string code)
        {
            int? level = _Base.GetLevel(code);
            if (!level.HasValue)
            {
                _log.Warn(ClassName + String.Format("Для кода: {0} не найден уровень в КЛАДР", code));
                return null;
            }

            var queryString = new StringBuilder();
            queryString.Append(code);
            for (; level > 0; level--)
            {
                string parentCode = AddressParser.GetParentCode(level.Value, code);
                if (!code.Equals(parentCode, StringComparison.CurrentCultureIgnoreCase)) queryString.Append(" " + parentCode);
                code = parentCode;
            }

            var parser = new QueryParser(Version.LUCENE_30, "id", _analyzer);
            Query query = parser.Parse(queryString.ToString());

            var searcher = GetSearcher();
            if (searcher == null) return null;
            TopDocs hits = searcher.Search(query, int.MaxValue);

            ParsedAddress d = new ParsedAddress();

            var totalHits = hits.TotalHits;
            var list = new List<ParsedAddressPart>();
            int minLvl = _Base.NoLevel;
            for (int i = 0; i < totalHits; i++)
            {
                Document doc = searcher.Doc(hits.ScoreDocs[i].Doc);

                ParsedAddressPart part = new ParsedAddressPart();
                part.Id = doc.Get("id");
                string postalCode = doc.Get("postalCode");

                part.Level = int.Parse(doc.Get("lvl"));
                part.Reduction = doc.Get("reduction");
                part.Name = doc.Get("name");
                part.NameWithShortReduction = doc.Get("name_short_reduction");
                part.NameWithReduction = doc.Get("name_reduction");
                
                list.Add(part);

                if (!string.IsNullOrEmpty(postalCode) && minLvl < part.Level)
                {
                    d.PostalCode = postalCode;
                    minLvl = part.Level;
                }
            }

            d.AddressPart = list.OrderBy(p => p.Level).ToList();
            return d;
        }

        public IEnumerable<ParsedAddress> GetByIndex(string index)
        {
            if (!string.IsNullOrEmpty(index) && !_Base.INDEX.IsMatch(index))
            {
                _log.Warn(ClassName + String.Format("Значение: {0} не может быть почтовым индексом", index));
                yield break;
            }

            var parser = new QueryParser(Version.LUCENE_30, "postalCode", _analyzer);
            Query query = parser.Parse("+" + index);

            var searcher = GetSearcher();
            if (searcher == null) yield break;
            TopDocs hits = searcher.Search(query, int.MaxValue);

            int no = 0;
            var totalHits = hits.TotalHits;
            var lastInfo = new ParsedAddressPartInfo();
            while (no < totalHits)
            {
                lastInfo.No = no;
                lastInfo.TotalHits = totalHits;

                Document doc = searcher.Doc(hits.ScoreDocs[no].Doc);
                int lvl = int.Parse(doc.Get("lvl"));
                if (lvl == _Base.LastLevel)
                {
                    string id = doc.Get("id");
                    var d = GetByCode(id);

                    d.LastInfo = lastInfo;

                    yield return d;
                }
                no++;
            }
        }

        public IEnumerable<AddressPartIndexSearch> GetByIndex4Search(string index)
        {
            if (!string.IsNullOrEmpty(index) && !_Base.INDEX.IsMatch(index))
            {
                _log.Warn(ClassName + String.Format("Значение: {0} не может быть почтовым индексом", index));
                yield break;
            }

            var parser = new QueryParser(Version.LUCENE_30, "postalCode", _analyzer);
            Query query = parser.Parse("+" + index);

            var searcher = GetSearcher();
            if (searcher == null) yield break;
            TopDocs hits = searcher.Search(query, int.MaxValue);

            int no = 0;
            var totalHits = hits.TotalHits;
            var lastInfo = new ParsedAddressPartInfo();
            while (no < totalHits)
            {
                lastInfo.No = no;
                lastInfo.TotalHits = totalHits;

                Document doc = searcher.Doc(hits.ScoreDocs[no].Doc);
                int lvl = int.Parse(doc.Get("lvl"));
                if (lvl == _Base.LastLevel)
                {
                    AddressPartIndexSearch d = new AddressPartIndexSearch();
                    d.Id = doc.Get("id");
                    d.PostalCode = doc.Get("postalCode");

                    d.Level = lvl;
                    d.Reduction = doc.Get("reduction");
                    d.Name = doc.Get("name");
                    d.NameWithShortReduction = doc.Get("name_short_reduction");
                    d.NameWithReduction = doc.Get("name_reduction");
                    d.FullName = doc.Get("fullName");

                    d.LastInfo = lastInfo;

                    yield return d;
                }
                no++;
            }
        }

        public IEnumerable<ParsedAddressPart> GetByLevel(string code)
        {
            int? level = _Base.GetLevel(code);
            if (!level.HasValue)
            {
                _log.Warn(ClassName + String.Format("Для кода: {0} не найден уровень в КЛАДР", code));
                yield break;
            }


            while (true)
            {
                string queryString = GetQueryLevel(level.Value, code);
                if (string.IsNullOrEmpty(queryString)) yield break;
                level++; // необходимый уровень

                var parser = new QueryParser(Version.LUCENE_30, "id", _analyzer);
                var top = (level - 1);
                if (top == _Base.TopLevel) parser.AllowLeadingWildcard = true;

                Query query = parser.Parse(queryString);

                var searcher = GetSearcher();
                if (searcher == null) yield break;
                TopDocs hits = searcher.Search(query, int.MaxValue);

                int no = 0;
                var totalHits = hits.TotalHits;

                if (totalHits < 2) continue;

                var lastInfo = new ParsedAddressPartInfo();
                while (no < totalHits)
                {
                    lastInfo.No = no;
                    lastInfo.TotalHits = totalHits;

                    Document doc = searcher.Doc(hits.ScoreDocs[no].Doc);
                    int lvl = int.Parse(doc.Get("lvl"));
                    if (lvl == top)
                    {
                        ParsedAddressPart part = new ParsedAddressPart();
                        part.LastInfo = lastInfo;

                        part.Id = doc.Get("id");

                        part.Level = lvl;
                        part.Reduction = doc.Get("reduction");
                        part.Name = doc.Get("name");
                        part.NameWithShortReduction = doc.Get("name_short_reduction");
                        part.NameWithReduction = doc.Get("name_reduction");

                        yield return part;
                    }
                    no++;
                }

                yield break;
            }
        }

        public IEnumerable<ParsedAddressPart> GetChildren(string code)
        {
            int? level = _Base.GetLevel(code);
            if (!level.HasValue)
            {
                _log.Warn(ClassName + String.Format("Для кода: {0} не найден уровень в КЛАДР", code));
                yield break;
            }

            while (true)
            {
                string queryString = GetQueryChildren(level.Value, code);
                if (string.IsNullOrEmpty(queryString)) yield break;
                level++; // необходимый уровень

                var parser = new QueryParser(Version.LUCENE_30, "id", _analyzer);
                Query query = parser.Parse(queryString);

                var searcher = GetSearcher();
                if (searcher == null) yield break;
                TopDocs hits = searcher.Search(query, int.MaxValue);

                int no = 0;
                var totalHits = hits.TotalHits;

                if (totalHits < 2) continue;

                var lastInfo = new ParsedAddressPartInfo();
                while (no < totalHits)
                {
                    lastInfo.No = no;
                    lastInfo.TotalHits = totalHits;

                    Document doc = searcher.Doc(hits.ScoreDocs[no].Doc);
                    int lvl = int.Parse(doc.Get("lvl"));
                    if (lvl == level)
                    {
                        ParsedAddressPart part = new ParsedAddressPart();
                        part.LastInfo = lastInfo;

                        part.Id = doc.Get("id");

                        part.Level = lvl;
                        part.Reduction = doc.Get("reduction");
                        part.Name = doc.Get("name");
                        part.NameWithShortReduction = doc.Get("name_short_reduction");
                        part.NameWithReduction = doc.Get("name_reduction");

                        yield return part;
                    }
                    no++;
                }

                yield break;
            }
        }

        private static string GetQueryLevel(int level, string code)
        {
            switch (level)
            {
                case 5:
                    return "+" + code.Substring(0, 11) +         "????";
                case 4:
                    return "+" + code.Substring(0, 8) +       "???0000";
                case 3:
                    return "+" + code.Substring(0, 5) +    "???0000000";
                case 2:
                    return "+" + code.Substring(0, 2) + "???0000000000";
                case 1:
                    return                           "+??0000000000000";
                default:
                    return null;
            }
        }

        private static string GetQueryChildren(int level, string code)
        {
            switch (level)
            {
                case 4:
                    return "+" + code.Substring(0, 11) + "????";
                case 3:
                    return "+" + code.Substring(0, 8) + "???0000";
                case 2:
                    return "+" + code.Substring(0, 5) + "???0000000";
                case 1:
                    return "+" + code.Substring(0, 2) + "???0000000000";
                default:
                    return null;
            }
        }

        #endregion
        #region Info

        public AddressPartReduction GetReduction(ParsedAddressPart part)
        {
            if (_reduction == null)
            {
                _reduction = new ReductionRamStorage(_indexDirectory);
            }
            return _reduction.Get(part.Level, part.Reduction);
        }

        #endregion
    }
}
