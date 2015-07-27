using System.IO;
using System.Text;
using CsvHelper;
using NLog;

namespace RUAddress.Source
{
    public interface ICsvDataSource
    {
        Logger GetLogger();
        CsvReader GetReader(StreamReader sr);
        CsvWriter GetWriter(StreamWriter sw);
    }

    public sealed class CsvDataSource : ICsvDataSource
    {
        private readonly Logger _log;

        public CsvDataSource(Logger log)
        {
            _log = log;
        }

        public Logger GetLogger() { return _log; }

        public CsvReader GetReader(StreamReader sr)
        {
            var reader = new CsvReader(sr);
            reader.Configuration.Delimiter = ";";
            reader.Configuration.Encoding = Encoding.UTF8;
            reader.Configuration.Quote = '"';
            return reader;
        }

        public CsvWriter GetWriter(StreamWriter sw)
        {
            var writer = new CsvWriter(sw);
            writer.Configuration.Delimiter = ";";
            writer.Configuration.Encoding = Encoding.UTF8;
            writer.Configuration.Quote = '"';
            return writer;
        }
    }
}
