using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class Extensions
    {
        public static string FormatArgsInvariant(this string str, params object[] objs)
        {
            return string.Format(str, objs);

        }
    }
}
