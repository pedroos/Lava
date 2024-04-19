namespace Lava.Lib;

public static class Utils {
    public static string Join<T>(this IEnumerable<T> list, string separator) => 
        string.Join(separator, list);

    public static IEnumerable<string> RecurseExceptionMessages(
        this Exception ex
    ) {
        static IEnumerable<string> Inner(Exception ex) {
            yield return ex.Message;
            if (ex.InnerException != null) 
                foreach (string ln in Inner(ex.InnerException)) 
                    yield return ln;
        }
        foreach (string ln in Inner(ex)) yield return ln;
    }
    
    // Yields an iterator "joined" by an element of the same type.
    // That is, a[0], b, a[1], b, ..., a[n]

    public static IEnumerable<T> Joined<T>(
        this IEnumerable<T> a, T b
    ) {
        int i = 0;
        int acnt = a.TryGetNonEnumeratedCount(out int cnt) ? cnt : a.Count();
        foreach (T x in a) {
            yield return x;
            if (i == acnt - 1) yield break;
            yield return b;
            i++;
        }
    }

    // `WriteLines` does not append a trailing newline. It would normally be
    // necessary for calling code to do so.

    public static void WriteLines(
        this IEnumerable<string> list, 
        bool noNewLines = false,
        Func<string, string>? modifier = null,
        TextWriter? tw = null
    ) {
        foreach (string ln in noNewLines ? list : list.Joined(NewLine)) 
            (tw ?? Out).Write(modifier != null ? modifier(ln) : ln);
    }
    
    // Executes a function, treating a possible exception. The purposes are to 
    // internalize the try/catch clause and to return a single consistent type 
    // in all cases.
    
    public static T Try<T, TEx>(Func<T> f, Func<TEx, T> fex) 
        where TEx : Exception 
    {
        try {
            return f();
        }
        catch (TEx ex) {
            return fex(ex);
        }
    }
    
    public static T Try<T, TEx1, TEx2>(Func<T> f, Func<TEx1, T> fex1, 
        Func<TEx2, T> fex2) where TEx1 : Exception where TEx2 : Exception
    {
        try {
            return f();
        }
        catch (TEx1 ex) {
            return fex1(ex);
        }
        catch (TEx2 ex) {
            return fex2(ex);
        }
    }
}