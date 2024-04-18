using Lava.Lib;

namespace Lava.Tests {
    [TestClass]
    public class Tests {
        readonly TextWriter? dbg = Out;

        [TestMethod]
        public void TestKeylessIngest() {
            Heap<int> heap = new(10000);
            
            Assert.IsTrue(heap.Data[..4].SequenceEqual(new int[] { 0, 0, 0, 0 }));

            var keylessIngestInstr = new Instr<int>("KeylessIngest", 
                Opers.KeylessIngest, new int[] { 2, 1, 3 });
            Processor.Perform(keylessIngestInstr, heap, 2, dbg);

            Assert.IsTrue(heap.Data[..4].SequenceEqual(new int[] { 2, 1, 3, 0 }));
        }
    }
}