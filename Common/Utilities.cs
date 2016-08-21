using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class Utilities
    {
        private static ConcurrentBag<KeyValuePair<DateTime, string>> ErrorList = new ConcurrentBag<KeyValuePair<DateTime, string>>();

        public static void PrintWithTime(string printString)
        {
            Console.WriteLine("[{0}]: {1}", DateTime.Now, printString);
        }
        public static void Print(string printString)
        {
            Console.WriteLine("{0}", printString);
        }

        public static void PrintErrorWithTime(string printString)
        {
            Console.WriteLine("[{0}: ERROR!!]: {1}", DateTime.Now, printString);
            ErrorList.Add(new KeyValuePair<DateTime, string>(DateTime.Now, printString));
        }

        public static void PrintAllErrors()
        {
            Print("=========== ERRORS START ===========");
            foreach (var error in ErrorList.OrderBy(c => c.Key))
            {
                Print("{0}: {1}".FormatArgsInvariant(error.Key, error.Value));
            }
            Print("=========== ERRORS END ===========");

        }
    }
}
