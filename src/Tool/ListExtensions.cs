using System.Collections.Generic;

namespace NDatabase.Tool
{
    internal static class ListExtensions
    {
        internal static bool IsNullOrEmpty<T>(this IList<T> list)
        {
            if (list == null) { return true; }
            return list.Count == 0;
        }
    }
}