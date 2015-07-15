using NLog;
using NUnit.Framework;

namespace RUAddress.Tests
{
    [TestFixture]
    public class AddressLoaderTests
    {
        private const string FolderWithBase7z = "..\\3rdparty";
        private const string FolderWithLucene = ".\\index";

        private static readonly Logger Log = LogManager.GetCurrentClassLogger();

        [Test]
        public void ReplaceDb()
        {
            var loader = new AddressLoader(Log);
            loader.ReplaceDb(FolderWithBase7z, FolderWithLucene);
        }
    }
}
