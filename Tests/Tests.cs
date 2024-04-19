using Lava.Lib;
using Lava.Lib.Operations;

namespace Lava.Tests {
    [TestClass]
    public class Tests {
        readonly TextWriter? dbg = Out;

        [TestMethod]
        public void TestKeylessSingleIngest() {
            Heap<int> heap = new(100);
            
            var items = new int[] { 2, 1, 3 };
            var oper = new KeylessIngest<int>();
            IsTrue(Processor.Perform<KeylessIngest<int>, int, int[]>(
                oper, items, heap, batchSize: 2, dbg
            ));
            
            IsTrue(heap.Data[..4].SequenceEqual(new int[] { 2, 1, 3, 0 }));
        }

        [TestMethod]
        public void TestKeylessDoubleIngest() {
            Heap<int> heap = new(100);
            
            var items = new int[] { 2, 1, 3 };
            var oper = new KeylessIngest<int>();
            IsTrue(Processor.Perform<KeylessIngest<int>, int, int[]>(
                oper, items, heap, batchSize: 2, dbg
            ));
            
            IsTrue(heap.Data[..4].SequenceEqual(new int[] { 2, 1, 3, 0 }));
            
            items = new int[] { 5, 4 };
            IsTrue(Processor.Perform<KeylessIngest<int>, int, int[]>(
                oper, items, heap, batchSize: 2, dbg
            ));
            
            IsTrue(heap.Data[..6].SequenceEqual(new int[] { 2, 1, 3, 5, 4, 0 }));
        }
        
        [TestMethod]
        public void TestClassify() {
            Heap<int> heap = new(100);
            
            var items = new int[] { 2, 1, 3 };
            var oper = new Classify<int>("Classify instance", x => true);
            IsTrue(Processor.Perform<Classify<int>, int, Classify<int>.Arg>(
                oper, items, heap, batchSize: 2, dbg
            ));

            IsTrue(heap.GetProp("Classify instance", out bool[]? vals));

            IsTrue(vals!.Length == heap.Data.Length);

            IsTrue(vals[..4].SequenceEqual(new bool[] { 
                true, true, true, false }));
        }
    }
}