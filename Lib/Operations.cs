namespace Lava.Lib.Operations;
    
// Ingests unordered data at the tail of the heap

public sealed class KeylessIngest<T> : IOperFunc<T, T[]> 
    where T : unmanaged 
{
    public static string Name { get; } = "KeylessIngest";
    
    public T[] MakeArg(
        T[] data, int batch, int batchSize, int pos, int rem
    ) => data[pos..(pos + rem)];
    
    public void Do(Heap<T> heap, T[] arg, TextWriter? dbg) {
        if (arg == null) throw new ArgumentNullException(nameof(arg));
        heap.Append(arg, dbg);
    }
}

// Evaluates a predicate against data
    
public sealed class Classify<T> : IOperFunc<T, Classify<T>.Arg> 
    where T : unmanaged 
{
    public readonly record struct Arg(
        string Name, 
        int Pos, 
        int Rem, 
        Predicate<T> Pred
    );
    
    // Instance members are used as operator arguments consumable 
    // from `MakeArg`
    
    public static string Name { get; } = "Classify";
    
    public Classify(string instanceName, Predicate<T> predicate) {
        InstanceName = instanceName;
        Predicate = predicate;
    }
    
    public string InstanceName { get; }
    public Predicate<T> Predicate { get; }
    
    public Arg MakeArg(
        T[] data, int batch, int batchSize, int pos, int rem
    ) => 
        new(
            Name: InstanceName,
            Pos: pos,
            Rem: rem,
            Pred: Predicate
        );
    
    public void Do(Heap<T> heap, Arg arg, TextWriter? dbg) {
        heap.SetOrAddProp(arg.Name, arg.Pos, arg.Rem, arg.Pred, dbg);
    }
}