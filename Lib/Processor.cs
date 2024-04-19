namespace Lava.Lib;

/*
Processes like a processor, however at application data level.
- An operation takes a heap and a chunk of input data (or indicators about a 
  chunk, if the input is the heap itself) and modifies the heap by calling 
  methods on the heap
- An instruction (abstract) is a pair of an operation and a total input which 
  is a collection (not individual data points)
- The processor's processing method takes an instruction and a heap and 
  partitions the instruction's total input into a specified batch size and 
  processes each batch by calling the instruction's operation on the batch
  
Summary:
- Operation: knows about processing data
- Instruction: knows about total input
- Processor: knows about batches and coordination

Observations:
- The idea is NOT that operations are low-level; they should be self-sufficient
*/

// Base class for operation function calls

public interface IOperFunc<T, TArg> {
    static abstract string Name { get; }
    
    abstract TArg MakeArg(
        // `batch` is 1-based
        T[] data, int batch, int batchSize, int pos, int rem
    );
    
    // The action executed by the operation
    
    abstract void Do(
        Heap<T> heap,
        TArg arg,
        TextWriter? dbg
    );
}
    
// Heap class

public sealed class Heap<T> {
    public int Capacity { get; }
    int dataSize;
    public T[] Data { get => data; }
    readonly T[] data;
    readonly Dictionary<string, bool[]> props = new();

    public Heap(int capacity) {
        Capacity = capacity;
        dataSize = 0;
        data = new T[capacity];
    }
    
    public bool GetProp(string name, out bool[]? vals) =>
        props.TryGetValue(name, out vals);
    
    // Heap methods are called multiple times, each time with a different 
    // chunk from a batch. 
    // They trust any positional arguments are in-bounds.
    
    // Appends the input array to the current data array
        
    public void Append(T[] input, TextWriter? dbg) {
        dbg?.WriteLine($"[Heap] Append with length {input.Length} at pos {
            dataSize}");
        Array.ConstrainedCopy(input, 0, data, dataSize, input.Length);
        dataSize += input.Length;
        dbg?.WriteLine($"[Heap] Data is now {{ {
            data[..dataSize].Join(", ")} }} of size {dataSize}");
    }
    
    // Writes values for the evaluation of a property on a specific array.
    // If it doesn't exist, the array is created at the first chunk.
    
    public void SetOrAddProp(
        string name, 
        int pos, 
        int rem, 
        Predicate<T> pred, 
        TextWriter? dbg
    ) {
        bool[] vals;
        if (pos > 0) {
            if (!props.TryGetValue(name, out vals!))
                throw new InvalidOperationException(
                    "Property values array is missing");
        }
        else {
            props.Add(name, vals = new bool[Capacity]);
        }
        dbg?.WriteLine($"        SetOrAddProp: from {pos} to {
            pos + rem - 1}");
        for (int i = pos; i <= pos + rem - 1; i++) {
            bool v = pred(data[i]);
            vals[i] = v;
        }
        dbg?.WriteLine($"    Vals is {vals.Join(", ")}");
    }
}

// The processor

public static class Processor {
    public static bool Perform<TOper, T, TArg>(
        // Holds instance values as arguments
        TOper oper,
        T[]? items,
        Heap<T> heap, 
        int batchSize, 
        TextWriter? dbg
    ) where TOper : IOperFunc<T, TArg> {
        // Processes in batches.
        
        // If there is no input, the input is the heap.
        // The output is always the heap -- it's up to the help to not 
        // corrupt itself when it is both the input and the output.
        
        T[] data = items ?? heap.Data;
        
        if (batchSize > data.Length)
            throw new ArgumentException($"Batch size {batchSize
                } is greater than count of data items received {
                data.Length}", nameof(batchSize));
        if (data.Length > heap.Capacity) 
            throw new ArgumentException($"Count of data items received {
                data.Length} greater than heap capacity {heap.Capacity
                }", nameof(items));
        int btch = 1;
        var (q, r) = Math.DivRem(data.Length, batchSize);
        int fullBatches = q;
        int totalBatches = fullBatches + (r > 0 ? 1 : 0);
        dbg?.WriteLine($"Perform {TOper.Name} {{ {data.Join(", ")
            } }} with batch size {batchSize}");
        dbg?.WriteLine($"    fullBatches is {fullBatches}");
        dbg?.WriteLine($"    r is {r}");
        // Chunk/partition the input
        return Try<bool, Exception>(() => {
            for (int i = 0; i < totalBatches; i++) {
                int pos = (btch - 1) * batchSize;
                int bsize = btch > fullBatches ? r : batchSize;
                dbg?.WriteLine($"    bsize is {bsize}");
                // Operate on the chunk
                dbg?.WriteLine($"    from {pos} to {pos + bsize - 1}");
                oper.Do(
                    heap,
                    // Retrieve custom arguments for each operation type, 
                    // which are a function of the data and the batch/bsize, 
                    // defined on the Operator.
                    // If the function does NOT depend on the batch/bsize,
                    // the arguments wouldn't change with each batch (but 
                    // this is opaque to the Processor).
                    arg: oper.MakeArg(data, btch, batchSize, pos, bsize),
                    dbg
                );
                btch++;
            }
            dbg?.WriteLine("Finished perform");
            return true;
        }, ex => {
            WriteLine($"ERROR: Instruction failed at batch {btch} of {
                totalBatches}");
            ex.RecurseExceptionMessages().WriteLines();
            WriteLine();
            return false;
        });
    }
}