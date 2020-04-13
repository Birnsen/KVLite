using System.Collections.Generic;
using System.Linq;

namespace KVL
{
    public static class Helper
    {

        public static async IAsyncEnumerable<IAsyncEnumerable<T>> Batch<T>(this IAsyncEnumerable<T> source, int batch)
        {
            var nextBatch = new List<T>(batch);
            await foreach(var item in source)
            {
                nextBatch.Add(item);
                if(nextBatch.Count == batch)
                {
                    yield return nextBatch.ToAsyncEnumerable();
                    nextBatch = new List<T>();
                }
            }

            if(nextBatch.Count > 0)
                yield return nextBatch.ToAsyncEnumerable();
        }
    }
}
