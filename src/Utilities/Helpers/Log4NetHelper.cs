using System.Configuration;
using System.IO;

using log4net;
using log4net.Config;

namespace NDatabase.Utilities.Helpers
{
    internal class Log4NetHelper
    {
        internal static readonly Log4NetHelper Instance = new Log4NetHelper();
        private readonly FileInfo _loggingConfigurationFile = new FileInfo(ConfigurationManager.AppSettings["logging-configuration-file"]);

        private Log4NetHelper()
        {
            XmlConfigurator.ConfigureAndWatch(_loggingConfigurationFile);
        }

        private static string GetDeclaringType()
        {
            return typeof (Log4NetHelper).FullName;
        }

        private static ILog Logger
        {
            get { return LogManager.GetLogger(GetDeclaringType()); }
        }

        internal void LogDebugMessage(string messageToLog)
        {
            Logger.Debug(messageToLog);
        }

        internal void LogErrorMessage(string messageToLog)
        {
            Logger.Error(messageToLog);
        }

        internal void LogFatalMessage(string messageToLog)
        {
            Logger.Fatal(messageToLog);
        }

        internal void LogInfoMessage(string messageToLog)
        {
            Logger.Info(messageToLog);
        }

        internal void LogWarningMessage(string messageToLog)
        {
            Logger.Warn(messageToLog);
        }
    }
}