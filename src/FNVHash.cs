namespace KVL
{
    internal class FNVHash
    {
        private const ulong FNV64_PRIME = 1099511628211;
        private const ulong FNV64_OFFSETBASIS = 14695981039346656037;
        private static ulong fastHash(byte[] array, int ibStart, int cbSize)
        {
            var hash = FNV64_OFFSETBASIS;
            for (int i = 0; i < cbSize; i++)
                hash = (hash ^ array[ibStart + i]) * FNV64_PRIME;

            return hash;
        }

        public static ulong Hash(byte[] hashee, uint modulo)
        {
            return fastHash(hashee, 0, hashee.Length) % modulo;
        }
    }
}