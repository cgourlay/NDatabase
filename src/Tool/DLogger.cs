using System.Collections.Generic;

namespace NDatabase.Tool
{
    internal static class Log4NetHelper
    {
        private static readonly IList<ILogger> Loggers = new List<ILogger>();

        internal static void Register(ILogger logger)
        {
            Loggers.Add(logger);
        }

        internal static void Warning(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Warning(MessageToLog(@object));
            }
        }

        private static string MessageToLog(object @object)
        {
            return @object == null
                ? "null"
                : @object.ToString();
        }

        internal static void Debug(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Debug(MessageToLog(@object));
            }
        }

        internal static void Info(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Info(MessageToLog(@object));
            }
        }

        internal static void Error(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Error(MessageToLog(@object));
            }
        }
    }
}
