using System.Collections.Generic;
using System.IO;
using RUAddress.Source;

namespace RUAddress.Storage
{
    internal class ReductionRamStorage
    {
        private readonly string _indexDirectory;
        private Dictionary<int, Dictionary<string, AddressPartReduction>> _reductions;

        public ReductionRamStorage(string indexDirectory)
        {
            _indexDirectory = indexDirectory;
        }

        public AddressPartReduction Get(AddressPartIndexWrite d)
        {
            return Get(d.Level, d.Reduction);
        }
        
        public AddressPartReduction Get(int level, string reduction)
        {
            if (_reductions == null) _reductions = Build();
            if (!_reductions.ContainsKey(level)) return null;
            var dict = _reductions[level];
            reduction = reduction.ToLower();
            if (!dict.ContainsKey(reduction)) return null;
            return dict[reduction];
        }

        private Dictionary<int, Dictionary<string, AddressPartReduction>> Build()
        {
            var path = Path.Combine(_indexDirectory, SOCRBASE.FileName);
            path = Path.ChangeExtension(path, ".csv");
            var list = SOCRBASE.LoadFromCSV(path);

            var reductions = new Dictionary<int, Dictionary<string, AddressPartReduction>>();
            foreach (var d in list) Insert(reductions, d);
            return reductions;
        }


        public void Add(IEnumerable<AddressPartReduction> list)
        {
            var reductions = new Dictionary<int, Dictionary<string, AddressPartReduction>>();
            foreach (var d in list) Insert(reductions, d);
            _reductions = reductions;
        }

        private static void Insert(Dictionary<int, Dictionary<string, AddressPartReduction>> reductions, AddressPartReduction d)
        {
            if (!reductions.ContainsKey(d.Level))
            {
                reductions.Add(d.Level, new Dictionary<string, AddressPartReduction>());
            }
            var dict = reductions[d.Level];
            var reduction = d.Short.ToLower();
            dict.Add(reduction, d);
        }
    }
}
