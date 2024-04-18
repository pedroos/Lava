namespace Lava.Lib;

/*
Processes like a processor, however at application data level.
- An operation takes a heap and input data and modifies the heap by calling 
  methods on the heap
- An instruction encapsulates an operation and a total input that is a 
  collection (not individual data points)
  - This is like a "job" in Spark
- A processor's processing methods take an instruction and a heap and 
  partitions the instruction's total input into a specified batch size and 
  processes each batch atomically by calling the instruction's operation 
  on the batch
  
Summary:
- Operation: knows about processing data
- Instruction: knows about total input
- Processor: knows about chunks of input and coordination

Observations:
- The idea is NOT that operations are low-level; they should be 
  self-sufficient
*/

public static class Opers {
    public static void KeylessIngest<T>(
        Heap<T> heap, T[] input, TextWriter? dbg
    ) where T : notnull {
        // Append
        dbg?.WriteLine($"        KeylessIngest {{ {input.Join(", ")} }}");
        heap.Append(input, dbg);
    }
}

public record Instr<T> (
    string Name,
    Action<Heap<T>, T[], TextWriter?> Oper,
    T[] Items
) where T : notnull;

public sealed class Heap<T> {
    public int Capacity { get; }
    int DataSize { get; set; }
    public T[] Data { get; }
    public Heap(int capacity) {
        Capacity = capacity;
        DataSize = 0;
        Data = new T[capacity];
    }
    public void Append(T[] input, TextWriter? dbg) {
        dbg?.WriteLine($"[Heap] Append with length {input.Length} at pos {
            DataSize}");
        Array.ConstrainedCopy(input, 0, Data, DataSize, input.Length);
        DataSize += input.Length;
        dbg?.WriteLine($"[Heap] Data is now {{ {
            Data[..DataSize].Join(", ")} }} of size {DataSize}");
    }
}
    
public static class Processor {
    public static bool Perform<T>(
        Instr<T> instr, 
        Heap<T> heap, 
        int batchSize, 
        TextWriter? dbg
    ) where T : notnull {
        // Processes in batches.
        
        if (batchSize > instr.Items.Length)
            throw new ArgumentException($"Batch size {batchSize
                } is greater than count of items received {
                instr.Items.Length}", nameof(batchSize));
        if (instr.Items.Length > heap.Capacity) 
            throw new ArgumentException($"Count of items received {
                instr.Items.Length} greater than heap capacity {
                heap.Capacity}", nameof(instr));
        int pos = 0;
        int btch = 1;
        var (q, r) = Math.DivRem(instr.Items.Length, batchSize);
        int btches = q;
        if (r > 0) btches++;
        dbg?.WriteLine($"Perform {instr.Name} {{ {instr.Items.Join(", ")
            } }} with batch size {batchSize} ({btches} batches)");
        for (int i = 0; i < btches; i++) {
        // Chunk/partition the input
            int rem = Math.Min(batchSize, instr.Items.Length - pos);
            dbg?.WriteLine($"    rem is {rem}");
            // Operate on the chunk
            try {
                dbg?.WriteLine($"        from {pos} to {pos + rem - 1}");

                instr.Oper(heap, instr.Items[pos..(pos + rem)], dbg);
                pos += rem;
                btch++;
                dbg?.WriteLine($"        pos is {pos}");
            }
            catch (Exception ex) {
                WriteLine($"ERROR: Instruction failed at batch {btch} of {
                    btches}");
                ex.RecurseExceptionMessages().WriteLines();
                return false;
            }
        }
        dbg?.WriteLine("Finished perform");
        return true;
    }
}