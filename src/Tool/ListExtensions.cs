using System.Collections.Generic;

namespace NDatabase.Tool
{
    internal static class ListExtensions
    {
        internal static bool IsNullOrEmpty<T>(this IList<T> list)
        {
            return list == null || list.Count == 0;
        }
    }
}