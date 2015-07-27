using System.Collections.Generic;

namespace RUAddress.Storage
{
    internal class IndexWriteRamStorage
    {
        private readonly Dictionary<int, Dictionary<string, AddressPartIndexWrite>> _index_addobj_sort;
        private readonly Dictionary<string, AddressPartIndexWrite> _index_addobj_id;

        public IndexWriteRamStorage()
        {
            _index_addobj_sort = new Dictionary<int, Dictionary<string, AddressPartIndexWrite>>();
            _index_addobj_id = new Dictionary<string, AddressPartIndexWrite>();
        }

        public Dictionary<int, Dictionary<string, AddressPartIndexWrite>> Sort { get { return _index_addobj_sort; } }
        public Dictionary<string, AddressPartIndexWrite> Index { get { return _index_addobj_id; } }

        public void Add(IEnumerable<AddressPartIndexWrite> list)
        {
            foreach (var d in list) Add(d);
        }

        private void Add(AddressPartIndexWrite d)
        {
            d.SearchName = null;
            d.FullName = null;

            if (!_index_addobj_sort.ContainsKey(d.Level))
            {
                _index_addobj_sort[d.Level] = new Dictionary<string, AddressPartIndexWrite>();
            }
            var sort = _index_addobj_sort[d.Level];
            sort.Add(d.Id, d);


            _index_addobj_id.Add(d.Id, d);
        }
    }
}
