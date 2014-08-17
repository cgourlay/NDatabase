using NDatabase.Utilities.Wrappers;

namespace NDatabase.Utilities
{
    internal static class UniqueIdGenerator
    {
        internal static long GetRandomLongId()
        {
            lock (typeof (UniqueIdGenerator))
            {
                return (long) (OdbRandom.GetRandomDouble() * long.MaxValue);
            }
        }
    }
}
