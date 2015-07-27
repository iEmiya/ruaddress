using System;
using System.Collections.Generic;
using NLog;
using RUAddress.Source;
using RUAddress.Storage;

namespace RUAddress
{
    public sealed class AddressParser : IDisposable
    {
        public static string GetParentCode(int level, string code)
        {
            switch (level)
            {
                case 5:
                    return code.Substring(0, 11) + "0000";
                case 4:
                    return code.Substring(0, 8) + "0000000";
                case 3:
                    return code.Substring(0, 5) + "0000000000"; ;
                case 2:
                    return code.Substring(0, 2) + "0000000000000";
                default:
                    return code;
            }
        }

        private readonly Logger _log;
        private LuceneDataSource _source;

        public AddressParser(Logger log, string indexDirectory)
        {
            _log = log;
            _source = new LuceneDataSource(_log, indexDirectory);
        }

        public void Dispose()
        {
            if(_source != null) _source.Dispose();
            _source = null;
        }

        public int? GetLevel(string code)
        {
            return _Base.GetLevel(code);
        }

        public IEnumerable<AddressPartIndexSearch> Search(string filter)
        {
            return _source.Search(filter);
        }

        public ParsedAddress GetByCode(string code)
        {
            return _source.GetByCode(code);
        }

        public IEnumerable<ParsedAddress> GetByIndex(string index)
        {
            return _source.GetByIndex(index);
        }

        public IEnumerable<AddressPartIndexSearch> GetByIndex4Search(string index)
        {
            return _source.GetByIndex4Search(index);
        }

        public IEnumerable<ParsedAddressPart> GetByLevel(string code)
        {
            return _source.GetByLevel(code);
        }
        
        public IEnumerable<ParsedAddressPart> GetChildren(string code)
        {
            return _source.GetChildren(code);
        }

        public AddressPartReduction GetReduction(ParsedAddressPart part)
        {
            return _source.GetReduction(part);
        }
    }
}
