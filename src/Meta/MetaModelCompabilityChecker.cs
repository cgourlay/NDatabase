using System;
using System.Collections.Generic;
using NDatabase.Exceptions;
using NDatabase.Services;
using NDatabase.Tool;
using NDatabase.Utilities.Helpers;

namespace NDatabase.Meta
{
    internal class MetaModelCompabilityChecker : IMetaModelCompabilityChecker
    {
        private readonly IList<ClassInfoCompareResult> _results = new List<ClassInfoCompareResult>();

        /// <summary>
        ///     Receive the current class info (loaded from current classes present on runtime and check against the persisted meta model
        /// </summary>
        public bool Check(IDictionary<Type, ClassInfo> currentCIs, IMetaModelService metaModelService)
        {
            foreach (var persistedCI in metaModelService.GetAllClasses())
                CheckClass(currentCIs, persistedCI);

            foreach (var result in _results)
            {
                Log4NetHelper.Instance.LogInfoMessage(string.Format("MetaModelCompabilityChecker: Class {0} has changed :", result.GetFullClassName()));
                Log4NetHelper.Instance.LogInfoMessage("MetaModelCompabilityChecker: " + result);
            }

            return _results.Count != 0;
        }

        private void CheckClass(IDictionary<Type, ClassInfo> currentCIs, ClassInfo persistedCI)
        {
            var currentCI = currentCIs[persistedCI.UnderlyingType];
            var classInfoCompareResult = persistedCI.ExtractDifferences(currentCI, true);

            if (!classInfoCompareResult.IsCompatible())
                throw new OdbRuntimeException(NDatabaseError.IncompatibleMetamodel.AddParameter(currentCI.ToString()));

            if (classInfoCompareResult.HasCompatibleChanges())
                _results.Add(classInfoCompareResult);
        }
    }
}
