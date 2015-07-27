using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using NLog;
using NUnit.Framework;
using RUAddress.Source;
using RUAddress.Storage;

namespace RUAddress.Tests
{
    [TestFixture]
    public class AddressParserTests
    {
        private const string FolderWithLucene = ".\\index";

        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        [Test]
        public void Search()
        {
            var list = new string[]
            {
                "Адыгея, Майкоп, Краснодарская",
                "Воронежская, Петропавловский, Петропавловка, Садовая",
                "Московская, Балашиха, Русавкино-Романово, Энергетическая",
                "Балашиха Фадеева",
                "Фадеева Балашиха",
                "Респ Адыгея",
                "Адыгея"
            };

            var source = new AddressParser(Log, FolderWithLucene);

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var filter in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;
                    var collection = source.Search(filter);

                    System.Diagnostics.Trace.WriteLine(filter + "...");
                    int? totalHits = null;
                    foreach (var d in collection)
                    {
                        if (!totalHits.HasValue)
                        {
                            totalHits = d.LastInfo.TotalHits;
                            System.Diagnostics.Trace.WriteLine("TotalHits: " + totalHits);

                            if (totalHits > 100) break;
                        }
                        System.Diagnostics.Trace.WriteLine("    " + d.Id + ":" + d.FullName);
                    }
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }

        [Test]
        public void GetByCode()
        {
            var source = new AddressParser(Log, FolderWithLucene);

            var list = new string[]
            {
                "500000360000039", 
                "010000010000095", 
                "010000010000000",
                "470130009190000",
                "220610000010033",
            };

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var code in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;
                    var d = source.GetByCode(code);

                    System.Diagnostics.Trace.WriteLine(code + ": " + d.ToString());
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }

        [Test]
        public void GetByIndex()
        {
            var source = new AddressParser(Log, FolderWithLucene);

            var list = new string[]
            {
                "658390", 
                "659063", 
                "397670",
            };

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var index in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;
                    var collection = source.GetByIndex(index);

                    System.Diagnostics.Trace.WriteLine(index + "...");
                    int? totalHits = null;
                    foreach (var d in collection)
                    {
                        if (!totalHits.HasValue)
                        {
                            totalHits = d.LastInfo.TotalHits;
                            System.Diagnostics.Trace.WriteLine("TotalHits: " + totalHits);

                            if (totalHits > 20) break;
                        }
                        System.Diagnostics.Trace.WriteLine(index + ": " + d.ToString());
                    }
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }
        
        [Test]
        public void GetByIndex4Search()
        {
            var source = new AddressParser(Log, FolderWithLucene);

            var list = new string[]
            {
                "658390", 
                "659063", 
                "397670",
            };

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var index in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;
                    var collection = source.GetByIndex4Search(index);

                    System.Diagnostics.Trace.WriteLine(index + "...");
                    int? totalHits = null;
                    foreach (var d in collection)
                    {
                        if (!totalHits.HasValue)
                        {
                            totalHits = d.LastInfo.TotalHits;
                            System.Diagnostics.Trace.WriteLine("TotalHits: " + totalHits);

                            if (totalHits > 20) break;
                        }
                        System.Diagnostics.Trace.WriteLine("    " + d.Id + ":" + d.FullName);
                    }
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }

        [Test]
        public void GetByLevel()
        {
            var source = new AddressParser(Log, FolderWithLucene);

            var list = new string[]
            {
                // 1
                "090000000000000",
                "890000000000000",

                // 2
                "110070000000000",
                "140280000000000",
                "660120000000000",

                // 3
                "020000030000000",
                "240020080000000",
                "540060040000000",

                // 4
                "500000360000000",
                "470130009190000",
                "010000010080000",
                "590190000480000",
                "710240002550000",
            };

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var code in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;

                    int level = source.GetLevel(code).Value;
                    var collection = source.GetByLevel(code);

                    System.Diagnostics.Trace.WriteLine(code + "...");
                    int? totalHits = null;
                    foreach (var d in collection)
                    {
                        if (!totalHits.HasValue)
                        {
                            totalHits = d.LastInfo.TotalHits;
                            System.Diagnostics.Trace.WriteLine("TotalHits: " + totalHits);
                        }

                        if (totalHits < 20) System.Diagnostics.Trace.WriteLine("    " + d.ToString());

                        try
                        {
                            Assert.That(d.Level, Is.EqualTo(level));
                        }
                        catch (Exception)
                        {
                            System.Diagnostics.Trace.WriteLine("warn:" + d.ToString());
                            throw;
                        }

                    }
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }

        [Test]
        public void GetChildren()
        {
            var source = new AddressParser(Log, FolderWithLucene);

            var list = new string[]
            {
                // 1
                "090000000000000",
                "890000000000000",

                // 2
                "110070000000000",
                "140280000000000",
                "660120000000000",

                // 3
                "020000030000000",
                "240020080000000",
                "540060040000000",

                // 4
                "500000360000000",
                "470130009190000",
                "010000010080000",
                "590190000480000",
                "710240002550000",
            };

            long total = 0;
            int count = 0;
            for (int i = 0; i < 10; i++)
            {
                foreach (var code in list)
                {
                    count++;

                    var b = DateTime.UtcNow.Ticks;

                    int level = source.GetLevel(code).Value + 1;
                    var collection = source.GetChildren(code);

                    System.Diagnostics.Trace.WriteLine(code + "...");
                    int? totalHits = null;
                    foreach (var d in collection)
                    {
                        if (!totalHits.HasValue)
                        {
                            totalHits = d.LastInfo.TotalHits;
                            System.Diagnostics.Trace.WriteLine("TotalHits: " + totalHits);
                        }

                        if (totalHits < 20) System.Diagnostics.Trace.WriteLine("    " + d.ToString());

                        try
                        {
                            Assert.That(d.Level, Is.GreaterThanOrEqualTo(level));
                        }
                        catch (Exception)
                        {
                            System.Diagnostics.Trace.WriteLine("warn:" + d.ToString());
                            throw;
                        }
                        
                    }
                    total += (DateTime.UtcNow.Ticks - b);
                }
            }
            System.Diagnostics.Trace.WriteLine(string.Format("Total: {0} ms", 1.0 * total / (TimeSpan.TicksPerMillisecond * count)));
        }

        [Test]
        public void GetReduction()
        {
            var list = LoadReductionInternal();

            var source = new AddressParser(Log, FolderWithLucene);
            foreach (var level in list.Keys.ToArray())
            {
                foreach (var reduction in list[level].Keys.ToArray())
                {
                    var d = source.GetReduction(new ParsedAddressPart() { Level = level, Reduction = reduction });
                    EqualTo(d, list[level][reduction]);
                }
            }
        }

        private static Dictionary<int, Dictionary<string, AddressPartReduction>> LoadReductionInternal()
        {
            var assem = typeof(AddressParserTests).Assembly;
            var resource = assem.GetManifestResourceStream("RUAddress.Tests.Resources.SOCRBASE.csv");

            var dataSource = new CsvDataSource(Log);
            using (var sr = new StreamReader(resource))
            {
                var reader = dataSource.GetReader(sr);
                IEnumerable<AddressPartReduction> records = reader.GetRecords<AddressPartReduction>();

                var reductions = new Dictionary<int, Dictionary<string, AddressPartReduction>>();
                foreach (var d in records)
                {
                    if (!reductions.ContainsKey(d.Level))
                    {
                        reductions.Add(d.Level, new Dictionary<string, AddressPartReduction>());
                    }
                    var dict = reductions[d.Level];
                    var reduction = d.Short.ToLower();
                    dict.Add(reduction, d);
                }
                return reductions;
            }
        }

        private static void EqualTo(AddressPartReduction actual, AddressPartReduction expected)
        {
            Assert.IsNotNull(actual);
            Assert.That(actual.Level, Is.EqualTo(expected.Level));
            Assert.That(actual.Short, Is.EqualTo(expected.Short));
            Assert.That(actual.Name, Is.EqualTo(expected.Name));
        }

        [Test]
        public void GetRegions()
        {
            var list = GetAllRegions();

            var source = new AddressParser(Log, FolderWithLucene);

            foreach (var part in source.GetByLevel("010000000000000"))
            {
                Assert.IsTrue(list.ContainsKey(part.Id));
                var d = list[part.Id];
                Assert.That(part.Id, Is.EqualTo(d.Code));
                Assert.That(part.NameWithReduction, Is.EqualTo(d.Name));
            }
        }

        private static Dictionary<string, Kladr> GetAllRegions()
        {
            var dict = new Dictionary<string, Kladr>();
            var list = LoadKladrInternal();
            foreach (var d in list) dict.Add(d.Code, d);
            return dict;
        }

        private static IEnumerable<Kladr> LoadKladrInternal()
        {
            var serializer = new XmlSerializer(typeof(Kladr));
            var collection = new List<Kladr>();
            var assem = typeof(Kladr).Assembly;
            var resource = assem.GetManifestResourceStream("RUAddress.Tests.Resources.kladr_all_regions.xml");
            using (var st = resource)
            {
                var k = (Kladr)serializer.Deserialize(st);
                if (k.Inner != null) collection.AddRange(k.Inner);
            }
            return collection;
        }

        [XmlRoot("kladr")]
        public class Kladr
        {
            [XmlAttribute("c")]
            public string Code { get; set; }
            [XmlAttribute("t")]
            public string Socr { get; set; }
            [XmlAttribute("n")]
            public string Name { get; set; }
            [XmlElement("k")]
            public List<Kladr> Inner { get; set; }

            public override string ToString()
            {
                return String.Format("{0} {1}", Socr, Name);
            }
        }
    }
}
