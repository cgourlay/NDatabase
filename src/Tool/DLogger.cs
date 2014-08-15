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


        private static string MessageToLog(object @object)
        {
            return @object == null
                ? "null"
                : @object.ToString();
        }





        internal static void LogFatalMessage(object @object)
        {
        }

        internal static void LogWarningMessage(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Warning(MessageToLog(@object));
            }
        }

        internal static void LogDebugMessage(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Debug(MessageToLog(@object));
            }
        }

        internal static void LogInfoMessage(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Info(MessageToLog(@object));
            }
        }

        internal static void LogErrorMessage(object @object)
        {
            foreach (var logger in Loggers)
            {
                logger.Error(MessageToLog(@object));
            }
        }
    }
}
