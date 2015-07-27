using System.Collections.Generic;
using System.Text;

namespace RUAddress.Storage
{
    public class ParsedAddressPartInfo
    {
        public int No { get; set; }
        public int TotalHits { get; set; }
    }

    public class ParsedAddressPart
    {
        public int Level { get; set; }
        public string Id { get; set; }
        public string Reduction { get; set; }
        public string Name { get; set; }

        public string NameWithShortReduction { get; set; }
        public string NameWithReduction { get; set; }

        /// <remarks>
        /// Информация о последнем объекте в выборе, при использовании интерфейсов IEnumerable
        /// </remarks>
        public ParsedAddressPartInfo LastInfo { get; set; }

        public override string ToString()
        {
            return NameWithReduction;
        }
    }

    public abstract class AddressPartIndex : ParsedAddressPart
    {
        public string PostalCode { get; set; }

        public string FullName { get; set; }
    }

    public sealed class AddressPartIndexWrite : AddressPartIndex
    {
        public string SearchName { get; set; }
    }

    public sealed class AddressPartIndexSearch : AddressPartIndex
    {
    }

    public sealed class ParsedAddress
    {
        public string PostalCode { get; set; }
        public List<ParsedAddressPart> AddressPart { get; set; }

        public ParsedAddressPartInfo LastInfo { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            if (!string.IsNullOrEmpty(PostalCode)) sb.Append(PostalCode);
            foreach (var part in AddressPart)
            {
                if (sb.Length > 0) sb.Append(", ");
                sb.Append(part.Reduction + " " + part.Name);
            }
            return sb.ToString();
        }
    }

    public sealed class AddressPartReduction
    {
        public int Level { get; set; }
        public string Short { get; set; }
        public string Name { get; set; }
    }
}
