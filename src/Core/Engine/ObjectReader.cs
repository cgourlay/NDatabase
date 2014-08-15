using System;
using System.Collections.Generic;
using System.Linq;



















using NDatabase.Api;
using NDatabase.Api.Query;
using NDatabase.Btree;
using NDatabase.Cache;
using NDatabase.Container;
using NDatabase.Core.BTree;
using NDatabase.Core.Query;
using NDatabase.Core.Query.Criteria;
using NDatabase.Core.Query.Values;
using NDatabase.Core.Session;
using NDatabase.Exceptions;
using NDatabase.Meta;
using NDatabase.Storage;
using NDatabase.Tool;
using NDatabase.Tool.Wrappers;

namespace NDatabase.Core.Engine
{
    internal sealed class ObjectReader : IObjectReader
    {
        private readonly IFileSystemInterface _fileSystemInterface;
        private readonly IFileSystemReader _fileSystemReader;
        private IStorageEngine _storageEngine;

        private AbstractObjectInfo ReadObjectInfoFromPosition(ClassInfo classInfo, long objectPosition, bool useCache, bool returnObjects)
        {
            _currentDepth++;
            try
            {
                // Protection against bad parameter value
                if (objectPosition > _fileSystemInterface.GetLength())
                {
                    throw new OdbRuntimeException(
                        NDatabaseError.InstancePositionOutOfFile.AddParameter(objectPosition).AddParameter(
                            _fileSystemInterface.GetLength()));
                }
                if (objectPosition == StorageEngineConstant.DeletedObjectPosition || objectPosition == StorageEngineConstant.NullObjectPosition)
                {
                    // TODO Is this correct ?
                    return new NonNativeDeletedObjectInfo();
                }

                // Read block size and block type
                // block type is used to decide what to do
                _fileSystemInterface.SetReadPosition(objectPosition);
                // Reads the block size
                //TODO:  we are reading blockSize, but not using them, is that needed?
                _fileSystemInterface.ReadInt();
                // And the block type
                var objectBlockType = _fileSystemInterface.ReadByte();
                if (BlockTypes.IsNullNonNativeObject(objectBlockType)){return new NonNativeNullObjectInfo(classInfo);}
                if (BlockTypes.IsNullNativeObject(objectBlockType)){return NullNativeObjectInfo.GetInstance();}
                if (BlockTypes.IsDeletedObject(objectBlockType)){return new NonNativeDeletedObjectInfo();}
                // Checks if what we are reading is only a pointer to the real
                // block, if
                // it is the case, just recall this method with the right position
                if (BlockTypes.IsPointer(objectBlockType)) {throw new CorruptedDatabaseException(NDatabaseError.FoundPointer.AddParameter(objectPosition));}
                // Native of non native object ?
                if (BlockTypes.IsNative(objectBlockType))
                {
                    // Reads the odb type id of the native objects
                    var odbTypeId = _fileSystemInterface.ReadInt();
                    // Reads a boolean to know if object is null
                    var isNull = _fileSystemInterface.ReadBoolean(); // Native object is null ?

                    // last parameter is false=> no need to read native object
                    // header, it has been done
                    return isNull 
                               ? new NullNativeObjectInfo(odbTypeId)
                               : ReadNativeObjectInfo(odbTypeId, objectPosition, useCache, returnObjects, false);
                }

                if (BlockTypes.IsNonNative(objectBlockType)) {throw new OdbRuntimeException(NDatabaseError.ObjectReaderDirectCall);}

                throw new OdbRuntimeException(NDatabaseError.UnknownBlockType.AddParameter(objectBlockType).AddParameter(_fileSystemInterface.GetPosition() - 1));
            }
            finally
            {
                _currentDepth--;
            }
        }

        private ArrayObjectInfo ReadArrayFromDatabaseFile(long position, bool useCache, bool returnObjects)
        {
            var realArrayComponentClassName = _fileSystemInterface.ReadString();
            var subTypeId = OdbType.GetFromName(realArrayComponentClassName);

            // read the size of the array
            var arraySize = _fileSystemInterface.ReadInt();
            Log4NetHelper.Instance.LogDebugMessage(string.Format("{0}ObjectReader: reading an array of {1} with {2} elements.", OdbString.DepthToSpaces(_currentDepth), realArrayComponentClassName, arraySize));

            var array = new object[arraySize];
            // build a n array to store all element positions
            var objectIdentifications = new long[arraySize];
            for (var i = 0; i < arraySize; i++) {objectIdentifications[i] = _fileSystemInterface.ReadLong();}
            for (var i = 0; i < arraySize; i++)
            {
                try
                {
                    if (objectIdentifications[i] != StorageEngineConstant.NullObjectIdId)
                    {
                        object o = ReadObjectInfo(objectIdentifications[i], useCache, returnObjects);
                        if (!(o is NonNativeDeletedObjectInfo))
                        {
                            array.SetValue(o, i);
                        }
                    }
                    else
                    {
                        array.SetValue(NullNativeObjectInfo.GetInstance(), i);
                    }
                }
                catch (Exception e)
                {
                    throw new OdbRuntimeException(NDatabaseError.InternalError.AddParameter(string.Format("in ObjectReader.readArray - at position {0}", position)), e);
                }
            }
            var aoi = new ArrayObjectInfo(array);
            aoi.SetRealArrayComponentClassName(realArrayComponentClassName);
            aoi.SetComponentTypeId(subTypeId.Id);
            return aoi;
        }

        private AbstractObjectInfo ReadObjectInfo(long objectIdentification, bool useCache, bool returnObjects)
        {
            if (!IsObjectIdentifier(objectIdentification)) {return ReadObjectInfoFromPosition(null, objectIdentification, useCache, returnObjects);}
            var oid = OIDFactory.BuildObjectOID(-objectIdentification);
            return ReadNonNativeObjectInfoFromOid(null, oid, useCache, returnObjects);
        }

        private static bool IsObjectIdentifier(long objectIdentification)
        {
            return objectIdentification < 0;
        }

        private AtomicNativeObjectInfo ReadAtomicNativeObjectInfo(int objectDatabaseTypeId)
        {
            return new AtomicNativeObjectInfo(_fileSystemReader.ReadAtomicNativeObjectInfoAsObject(objectDatabaseTypeId), objectDatabaseTypeId);
        }

        private EnumNativeObjectInfo ReadEnumeration()
        {
            var objectId = _fileSystemInterface.ReadLong();
            var enumeratedValue = _fileSystemInterface.ReadString();
            var enumeratedType = GetClassInfoFromObjectId(objectId);
            return new EnumNativeObjectInfo(enumeratedType, enumeratedValue);
        }

        private ISession GetSession()
        {
            return _storageEngine.GetSession();
        }

        private IOdbCache GetCache()
        {
            return GetSession().GetCache();
        }

        private ClassInfo GetClassInfoFromObjectId(long objectId)
        {
            return GetClassInfoFromObjectId(OIDFactory.BuildClassOID(objectId));
        }

        private ClassInfo GetClassInfoFromObjectId(OID objectId)
        {
            return GetSession().GetMetaModel().GetClassInfoFromId(objectId);
        }

        private ClassInfo GetClassInfoFromName(string className)
        {
            return GetSession().GetMetaModel().GetClassInfo(fullClassName: className, throwExceptionIfDoesNotExist: true);
        }

        private void ResetCommitListeners()
        {
            _storageEngine.ResetCommitListeners();
        }




        /// <summary>
        ///   to build instances
        /// </summary>
        private readonly IInstanceBuilder _instanceBuilder;

        

        /// <summary>
        ///   A local variable to monitor object recursion
        /// </summary>
        private int _currentDepth;



        

        /// <summary>
        ///   The constructor
        /// </summary>
        public ObjectReader(IStorageEngine engine)
        {
            _storageEngine = engine;
            _fileSystemReader = new FileSystemReader(_storageEngine);
            _fileSystemInterface = engine.GetObjectWriter().FileSystemProcessor.FileSystemInterface;

            _instanceBuilder = BuildInstanceBuilder();
        }

        #region IObjectReader Members

        public void ReadDatabaseHeader()
        {
            _fileSystemReader.ReadDatabaseHeader();
        }

        public void LoadMetaModel(IMetaModel metaModel, bool full)
        {
            ClassInfo classInfo;
            var nbClasses = _fileSystemReader.ReadNumberOfClasses();
            if (nbClasses == 0)
                return;

            // Set the cursor Where We Can Find The First Class info OID
            _fileSystemInterface.SetReadPosition(StorageEngineConstant.DatabaseHeaderFirstClassOid);
            var classOID = OIDFactory.BuildClassOID(_fileSystemReader.ReadFirstClassOid());
            // read headers
            for (var i = 0; i < nbClasses; i++)
            {
                classInfo = _fileSystemReader.ReadClassInfoHeader(classOID);
                Log4NetHelper.Instance.LogDebugMessage(string.Format(
                    "{0}ObjectReader: Reading class header for {1} - oid = {2} prevOid={3} - nextOid={4}", OdbString.DepthToSpaces(_currentDepth),
                    classInfo.FullClassName, classOID, classInfo.PreviousClassOID,
                    classInfo.NextClassOID));
                metaModel.AddClass(classInfo);
                classOID = classInfo.NextClassOID;
            }

            if (!full)
                return;

            var allClasses = metaModel.GetAllClasses().ToList();

            // Read class info bodies
            foreach (var currentClassInfo in allClasses)
            {
                classInfo = ReadClassInfoBody(currentClassInfo);

                Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader:  class body for " + classInfo.FullClassName);
            }

            // No need to add it to metamodel, it is already in it.
            // metaModel.addClass(classInfo);
            // Read last object of each class
            foreach (var actualClassInfo in allClasses)
            {

                Log4NetHelper.Instance.LogDebugMessage(string.Format("{0}ObjectReader: Reading class info last instance {1}", OdbString.DepthToSpaces(_currentDepth),
                                            actualClassInfo.FullClassName));
                if (actualClassInfo.CommitedZoneInfo.HasObjects())
                {
                    // TODO Check if must use true or false in return object
                    // parameter
                    try
                    {
                        // Retrieve the object by oid instead of position
                        var oid = actualClassInfo.CommitedZoneInfo.Last;
                        actualClassInfo.LastObjectInfoHeader = ReadObjectInfoHeaderFromOid(oid, true);
                    }
                    catch (OdbRuntimeException e)
                    {
                        throw new OdbRuntimeException(
                            NDatabaseError.MetamodelReadingLastObject.AddParameter(actualClassInfo.FullClassName).
                                AddParameter(actualClassInfo.CommitedZoneInfo.Last), e);
                    }
                }
            }

            ResetCommitListeners();

            // Read class info indexes
            foreach (var actualClassInfo in allClasses)
            {
                IOdbList<ClassInfoIndex> indexes = new OdbList<ClassInfoIndex>();
                IQuery queryClassInfo = new SodaQuery(typeof(ClassInfoIndex));
                queryClassInfo.Descend("ClassInfoId").Constrain(actualClassInfo.ClassInfoId).Equal();
                var classIndexes = GetObjects<ClassInfoIndex>(queryClassInfo, true, -1, -1);
                indexes.AddAll(classIndexes);
                // Sets the btree persister
                foreach (var classInfoIndex in indexes)
                {
                    IBTreePersister persister = new LazyOdbBtreePersister(_storageEngine);
                    var btree = classInfoIndex.BTree;
                    btree.SetPersister(persister);
                    btree.GetRoot().SetBTree(btree);
                }


                var count = indexes.Count.ToString();
                Log4NetHelper.Instance.LogDebugMessage(
                    string.Format("{0}ObjectReader: Reading indexes for {1} : ", OdbString.DepthToSpaces(_currentDepth), actualClassInfo.FullClassName) +
                    count + " indexes");

                actualClassInfo.SetIndexes(indexes);
            }

            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Current Meta Model is :" + metaModel);
        }

        

        /// <summary>
        ///   Reads the pointers(ids or positions) of an object that has the specific oid
        /// </summary>
        /// <param name="oid"> The oid of the object we want to read the pointers </param>
        /// <param name="useCache"> </param>
        /// <returns> The ObjectInfoHeader @ </returns>
        public ObjectInfoHeader ReadObjectInfoHeaderFromOid(OID oid, bool useCache)
        {
            if (useCache)
            {
                var objectInfoHeader = GetCache().GetObjectInfoHeaderByOid(oid, false);
                if (objectInfoHeader != null)
                    return objectInfoHeader;
            }

            var position = GetObjectPositionFromItsOid(oid, useCache, true);
            return ReadObjectInfoHeaderFromPosition(oid, position, useCache);
        }

        public NonNativeObjectInfo ReadNonNativeObjectInfoFromOid(ClassInfo classInfo, OID oid, bool useCache,
                                                                  bool returnObjects)
        {
            // FIXME if useCache, why not directly search the cache?
            var position = GetObjectPositionFromItsOid(oid, useCache, false);
            if (position == StorageEngineConstant.DeletedObjectPosition)
                return new NonNativeDeletedObjectInfo();
            if (position == StorageEngineConstant.ObjectDoesNotExist)
                throw new OdbRuntimeException(NDatabaseError.ObjectWithOidDoesNotExist.AddParameter(oid));
            var nnoi = ReadNonNativeObjectInfoFromPosition(classInfo, oid, position, useCache, returnObjects);

            return nnoi;
        }

        /// <summary>
        ///   Reads a non non native Object Info (Layer2) from its position
        /// </summary>
        /// <param name="classInfo"> </param>
        /// <param name="oid"> can be null </param>
        /// <param name="position"> </param>
        /// <param name="useCache"> </param>
        /// <param name="returnInstance"> </param>
        /// <returns> The meta representation of the object @ </returns>
        public NonNativeObjectInfo ReadNonNativeObjectInfoFromPosition(ClassInfo classInfo, OID oid, long position,
                                                                       bool useCache, bool returnInstance)
        {
            var lsession = GetSession();
            // Get a temporary cache just to cache NonNativeObjectInfo being read to
            // avoid duplicated reads
            var cache = lsession.GetCache();
            var tmpCache = lsession.GetTmpCache();
            // ICache tmpCache =cache;
            // We are dealing with a non native object
            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Reading Non Native Object info with oid " + oid);
            // If the object is already being read, then return from the cache
            if (tmpCache.IsReadingObjectInfoWithOid(oid))
                return tmpCache.GetObjectInfoByOid(oid);
            var objectInfoHeader = GetObjectInfoHeader(oid, position, useCache, cache);
            if (classInfo == null) {classInfo = GetClassInfoFromObjectId(objectInfoHeader.GetClassInfoId());}
            oid = objectInfoHeader.GetOid();
            // if class info do not match, reload class info
            if (!classInfo.ClassInfoId.Equals(objectInfoHeader.GetClassInfoId())) {classInfo = GetClassInfoFromObjectId(objectInfoHeader.GetClassInfoId());}

            var positionAsString = objectInfoHeader.GetPosition().ToString();
            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Reading Non Native Object info of " + (classInfo == null
                                                                                        ? "?"
                                                                                        : classInfo.FullClassName) + " at " +
                          positionAsString + " with id " + oid);
            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Object Header is " + objectInfoHeader);

            var objectInfo = new NonNativeObjectInfo(objectInfoHeader, classInfo);
            objectInfo.SetOid(oid);
            objectInfo.SetClassInfo(classInfo);
            objectInfo.SetPosition(objectInfoHeader.GetPosition());
            // Adds the Object Info in cache. The remove (cache clearing) is done by
            // the Query Executor. This tmp cache is used to resolve cyclic reference problem.
            // When an object has cyclic reference, if we don t cache the object info, we will read the reference for ever!
            // With the cache , we detect the cyclic reference and return what has been read already
            tmpCache.StartReadingObjectInfoWithOid(objectInfo.GetOid(), objectInfo);
            AbstractObjectInfo aoi;
            IOdbList<PendingReading> pendingReadings = new OdbList<PendingReading>();
            for (var id = 1; id <= classInfo.MaxAttributeId; id++)
            {
                var cai = objectInfo.GetClassInfo().GetAttributeInfoFromId(id);
                if (cai == null)
                {
                    // the attribute does not exist anymore
                    continue;
                }
                var attributeIdentification = objectInfoHeader.GetAttributeIdentificationFromId(id);
                if (attributeIdentification == StorageEngineConstant.NullObjectPosition ||
                    attributeIdentification == StorageEngineConstant.NullObjectIdId)
                {
                    aoi = NullNativeObjectInfo.GetInstance();
                    objectInfo.SetAttributeValue(id, aoi);
                }
                else
                {
                    // Here we can not use cai.isNonNative because of interfaces :
                    // because an interface will always be considered as non native
                    // (Object for example) but
                    // could contain a String for example. So we assume that if
                    // attributeIdentification is negative
                    // the object is non native,if positive the object is native.
                    if (attributeIdentification < 0)
                    {
                        // ClassInfo ci =
                        // storageEngine.getSession(true).getMetaModel().getClassInfo(cai.getFullClassname(),
                        // true);
                        // For non native objects. attribute identification is the
                        // oid (*-1)
                        var attributeOid = OIDFactory.BuildObjectOID(-
                                                                     attributeIdentification);
                        // We do not read now, store the reading as pending and
                        // reads it later
                        pendingReadings.Add(new PendingReading(id, null, attributeOid));
                    }
                    else
                    {
                        aoi = ReadObjectInfo(attributeIdentification, useCache, returnInstance);
                        objectInfo.SetAttributeValue(id, aoi);
                    }
                }
            }
            foreach (var pendingReading in pendingReadings)
            {
                // If object is not in connected zone , the cache must be used
                var useCacheForAttribute = useCache ||
                                           !cache.IsInCommitedZone(pendingReading.GetAttributeOID());
                aoi = ReadNonNativeObjectInfoFromOid(pendingReading.GetCi(), pendingReading.GetAttributeOID(),
                                                     useCacheForAttribute, returnInstance);
                objectInfo.SetAttributeValue(pendingReading.GetId(), aoi);
            }

            return objectInfo;
        }

        public AttributeValuesMap ReadObjectInfoValuesFromOID(ClassInfo classInfo, OID oid, bool useCache,
                                                              IOdbList<string> attributeNames,
                                                              IOdbList<string> relationAttributeNames,
                                                              int recursionLevel)
        {
            var position = GetObjectPositionFromItsOid(oid, useCache, true);
            return ReadObjectInfoValuesFromPosition(classInfo, oid, position, useCache, attributeNames,
                                                    relationAttributeNames, recursionLevel);
        }



        

        /// <summary>
        ///   Gets the next object oid of the object with the specific oid
        /// </summary>
        /// <returns> The position of the next object. If there is no next object, return -1 @ </returns>
        public OID GetNextObjectOID(OID oid)
        {
            var position = _storageEngine.GetObjectWriter().GetIdManager().GetObjectPositionWithOid(oid, true);
            _fileSystemInterface.SetReadPosition(position + StorageEngineConstant.ObjectOffsetNextObjectOid);
            return OIDFactory.BuildObjectOID(_fileSystemInterface.ReadLong());
        }

        public long ReadOidPosition(OID oid)
        {
            return _fileSystemReader.ReadOidPosition(oid);
        }

        public object GetObjectFromOid(OID oid, bool returnInstance, bool useCache)
        {
            var position = GetObjectPositionFromItsOid(oid, useCache, true);
            var o = ReadNonNativeObjectAtPosition(position, useCache, returnInstance);
            // Clear the tmp cache. This cache is use to resolve cyclic references
            GetSession().GetTmpCache().ClearObjectInfos();
            return o;
        }

        /// <summary>
        ///   Gets the real object position from its OID
        /// </summary>
        /// <param name="oid"> The oid of the object to get the position </param>
        /// <param name="useCache"> </param>
        /// <param name="throwException"> To indicate if an exception must be thrown if object is not found </param>
        /// <returns> The object position, if object has been marked as deleted then return StorageEngineConstant.DELETED_OBJECT_POSITION @ </returns>
        public long GetObjectPositionFromItsOid(OID oid, bool useCache, bool throwException)
        {
            return _fileSystemReader.GetObjectPositionFromItsOid(oid, useCache, throwException);
        }

        /// <summary>
        ///   Returns information about all OIDs of the database
        /// </summary>
        /// <param name="idType"> </param>
        /// <returns> @ </returns>
        public IList<long> GetAllIds(byte idType)
        {
            return _fileSystemReader.GetAllIds(idType);
        }

        public void Close()
        {
            _storageEngine = null;
            _fileSystemReader.Close();
        }

        public object BuildOneInstance(NonNativeObjectInfo objectInfo)
        {
            return _instanceBuilder.BuildOneInstance(objectInfo, GetCache());
        }

        public IInternalObjectSet<T> GetObjects<T>(IQuery query, bool inMemory, int startIndex, int endIndex)
        {
            IMatchingObjectAction queryResultAction = new QueryResultAction<T>(query, inMemory, _storageEngine,
                                                                                         true, _instanceBuilder);

            var queryManager = DependencyContainer.Resolve<IQueryManager>();
            var queryExecutor = queryManager.GetQueryExecutor(query, _storageEngine);

            return queryExecutor.Execute<T>(inMemory, startIndex, endIndex, true, queryResultAction);
        }

        public IValues GetValues(IInternalValuesQuery valuesQuery, int startIndex, int endIndex)
        {
            IMatchingObjectAction queryResultAction;
            if (valuesQuery.HasGroupBy())
                queryResultAction = new GroupByValuesQueryResultAction(valuesQuery, _instanceBuilder);
            else
                queryResultAction = new ValuesQueryResultAction(valuesQuery, _storageEngine, _instanceBuilder);
            var objects = GetObjectInfos<IObjectValues>(valuesQuery, true, startIndex, endIndex, false,
                                                        queryResultAction);
            return (IValues)objects;
        }

        public IObjectSet<TResult> GetObjectInfos<TResult>(IQuery query, bool inMemory, int startIndex, int endIndex,
                                             bool returnObjects, IMatchingObjectAction queryResultAction)
        {
            var queryManager = DependencyContainer.Resolve<IQueryManager>();
            var executor = queryManager.GetQueryExecutor(query, _storageEngine);
            return executor.Execute<TResult>(inMemory, startIndex, endIndex, returnObjects, queryResultAction);
        }

        public IInstanceBuilder GetInstanceBuilder()
        {
            return _instanceBuilder;
        }

        #endregion

        private IInstanceBuilder BuildInstanceBuilder()
        {
            return new InstanceBuilder(_storageEngine);
        }



        /// <summary>
        ///   Reads the body of a class info
        /// </summary>
        /// <param name="classInfo"> The class info to be read with already read header </param>
        /// <returns> The read class info @ </returns>
        private ClassInfo ReadClassInfoBody(ClassInfo classInfo)
        {
            var attributesDefinitionPositionAsString = classInfo.AttributesDefinitionPosition.ToString();
            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Reading new Class info Body at " + attributesDefinitionPositionAsString);

            _fileSystemInterface.SetReadPosition(classInfo.AttributesDefinitionPosition);
            var blockSize = _fileSystemInterface.ReadInt();
            var blockType = _fileSystemInterface.ReadByte();
            if (!BlockTypes.IsClassBody(blockType))
            {
                throw new OdbRuntimeException(
                    NDatabaseError.WrongTypeForBlockType.AddParameter("Class Body").AddParameter(blockType).AddParameter(
                        classInfo.AttributesDefinitionPosition));
            }
            // TODO This should be a short instead of long
            var nbAttributes = _fileSystemInterface.ReadLong();
            IOdbList<ClassAttributeInfo> attributes = new OdbList<ClassAttributeInfo>((int)nbAttributes);
            for (var i = 0; i < nbAttributes; i++)
                attributes.Add(ReadClassAttributeInfo());
            classInfo.Attributes = attributes;
            // FIXME Convert blocksize to long ??
            var realBlockSize = (int)(_fileSystemInterface.GetPosition() - classInfo.AttributesDefinitionPosition);
            if (blockSize != realBlockSize)
            {
                throw new OdbRuntimeException(
                    NDatabaseError.WrongBlockSize.AddParameter(blockSize).AddParameter(realBlockSize).AddParameter(
                        classInfo.AttributesDefinitionPosition));
            }
            return classInfo;
        }

        /// <summary>
        ///   Read an attribute of a class at the current position
        /// </summary>
        /// <returns> The ClassAttributeInfo description of the class attribute @ </returns>
        private ClassAttributeInfo ReadClassAttributeInfo()
        {
            var cai = new ClassAttributeInfo();
            var attributeId = _fileSystemInterface.ReadInt();
            var isNative = _fileSystemInterface.ReadBoolean();
            if (isNative)
            {
                var attributeTypeId = _fileSystemInterface.ReadInt();
                var type = OdbType.GetFromId(attributeTypeId);
                // if it is an array, read also the subtype
                if (type.IsArray())
                {
                    type = type.Copy();
                    var subTypeId = _fileSystemInterface.ReadInt();
                    var subType = OdbType.GetFromId(subTypeId);
                    if (subType.IsNonNative())
                    {
                        var fullClassName = GetClassInfoFromObjectId(OIDFactory.BuildClassOID(_fileSystemInterface.ReadLong())).FullClassName;

                        subType = subType.Copy(fullClassName);
                    }
                    type.SubType = subType;
                }
                cai.SetAttributeType(type);
                // For enum, we get the class info id of the enum class
                if (type.IsEnum())
                {
                    var classInfoId = _fileSystemInterface.ReadLong();
                    cai.SetFullClassName(GetClassInfoFromObjectId(OIDFactory.BuildClassOID(classInfoId)).FullClassName);
                    // For enum, we need to create a new type just to set the real enum class name
                    type = type.Copy(cai.GetFullClassname());
                    cai.SetAttributeType(type);
                }
                else
                    cai.SetFullClassName(cai.GetAttributeType().Name);
            }
            else
            {
                // This is a non native, gets the id of the type and gets it from
                // meta-model
                var typeId = _fileSystemInterface.ReadLong();
                cai.SetFullClassName(GetClassInfoFromObjectId(OIDFactory.BuildClassOID(typeId)).FullClassName);
                cai.SetClassInfo(GetClassInfoFromName(cai.GetFullClassname()));
                cai.SetAttributeType(OdbType.GetFromName(cai.GetFullClassname()));
            }
            cai.SetName(_fileSystemInterface.ReadString());
            cai.SetIndex(_fileSystemInterface.ReadBoolean());
            cai.SetId(attributeId);
            return cai;
        }

        /// <summary>
        ///   Reads an object at the specific position
        /// </summary>
        /// <param name="position"> The position to read </param>
        /// <param name="useCache"> To indicate if cache must be used </param>
        /// <param name="returnInstance"> indicate if an instance must be return of just the meta info </param>
        /// <returns> The object with position @ </returns>
        private object ReadNonNativeObjectAtPosition(long position, bool useCache, bool returnInstance)
        {
            // First reads the object info - which is a meta representation of the
            // object
            var nnoi = ReadNonNativeObjectInfoFromPosition(null, null, position, useCache, returnInstance);
            if (nnoi.IsDeletedObject())
                throw new OdbRuntimeException(NDatabaseError.ObjectIsMarkedAsDeletedForPosition.AddParameter(position));
            if (!returnInstance)
                return nnoi;
            // Then converts it to the real object
            var o = _instanceBuilder.BuildOneInstance(nnoi, GetCache());
            return o;
        }

        

        private ObjectInfoHeader ReadObjectInfoHeaderFromPosition(OID oid, long position, bool useCache)
        {
            if (position > _fileSystemInterface.GetLength())
            {
                throw new CorruptedDatabaseException(
                    NDatabaseError.InstancePositionOutOfFile.AddParameter(position).AddParameter(_fileSystemInterface.GetLength()));
            }
            if (position < 0)
            {
                throw new CorruptedDatabaseException(
                    NDatabaseError.InstancePositionIsNegative.AddParameter(position).AddParameter(oid.ToString()));
            }
            // adds an integer because, we pull the block size
            _fileSystemInterface.SetReadPosition(position + OdbType.Integer.Size);
            var objectBlockType = _fileSystemInterface.ReadByte();

            if (BlockTypes.IsNonNative(objectBlockType))
            {
                // compute the number of bytes to read
                // OID + ClassOid + PrevOid + NextOid + createDate + update Date + objectVersion + nbAttributes + RefCoutner + IsRoot
                // Long + Long +    Long    +  Long    + Long       + Long       +   int         + Int          + long       + byte
                var tsize = 7 * OdbType.SizeOfLong + 2 * OdbType.SizeOfInt + OdbType.SizeOfByte;
                var abytes = _fileSystemInterface.ReadBytes(tsize);
                var readOid = ByteArrayConverter.DecodeOid(abytes, 0);
                // oid can be -1 (if was not set),in this case there is no way to
                // check
                if (oid != null && readOid.CompareTo(oid) != 0)
                {
                    throw new CorruptedDatabaseException(
                        NDatabaseError.WrongOidAtPosition.AddParameter(oid).AddParameter(position).AddParameter(readOid));
                }
                // If oid is not defined, uses the one that has been read
                if (oid == null)
                    oid = readOid;
                // It is a non native object
                var classInfoId = OIDFactory.BuildClassOID(ByteArrayConverter.ByteArrayToLong(abytes, 8));
                var prevObjectOID = ByteArrayConverter.DecodeOid(abytes, 16);
                var nextObjectOID = ByteArrayConverter.DecodeOid(abytes, 24);
                var creationDate = ByteArrayConverter.ByteArrayToLong(abytes, 32);
                var updateDate = ByteArrayConverter.ByteArrayToLong(abytes, 40);
                var objectVersion = ByteArrayConverter.ByteArrayToInt(abytes, 48);
                var refCounter = ByteArrayConverter.ByteArrayToLong(abytes, 52);
                var isRoot = ByteArrayConverter.ByteArrayToBoolean(abytes, 60);

                // Now gets info about attributes
                var nbAttributesRead = ByteArrayConverter.ByteArrayToInt(abytes, 61);
                // Now gets an array with the identification all attributes (can be
                // positions(for native objects) or ids(for non native objects))
                var attributesIdentification = new long[nbAttributesRead];
                var attributeIds = new int[nbAttributesRead];
                var atsize = OdbType.SizeOfInt + OdbType.SizeOfLong;
                // Reads the bytes and then convert to values
                var bytes = _fileSystemInterface.ReadBytes(nbAttributesRead * atsize);
                for (var i = 0; i < nbAttributesRead; i++)
                {
                    attributeIds[i] = ByteArrayConverter.ByteArrayToInt(bytes, i * atsize);
                    attributesIdentification[i] = ByteArrayConverter.ByteArrayToLong(bytes,
                                                                                      i * atsize + OdbType.SizeOfInt);
                }
                var oip = new ObjectInfoHeader(position, prevObjectOID, nextObjectOID, classInfoId,
                                               attributesIdentification, attributeIds);
                oip.SetObjectVersion(objectVersion);
                oip.SetCreationDate(creationDate);
                oip.SetUpdateDate(updateDate);
                oip.SetOid(oid);
                oip.SetClassInfoId(classInfoId);
                oip.IsRoot = isRoot;
                oip.RefCounter = refCounter;

                if (useCache)
                {
                    // the object info does not exist in the cache
                    GetCache().AddObjectInfoOfNonCommitedObject(oip);
                }
                return oip;
            }
            if (BlockTypes.IsPointer(objectBlockType))
                throw new CorruptedDatabaseException(NDatabaseError.FoundPointer.AddParameter(oid).AddParameter(position));

            var positionAsString = position.ToString();
            throw new CorruptedDatabaseException(
                NDatabaseError.WrongTypeForBlockType.AddParameter(BlockTypes.BlockTypeNonNativeObject).AddParameter(
                    objectBlockType).AddParameter(positionAsString + "/oid=" + oid));
        }

       

        /// <param name="classInfo"> The class info of the objects to be returned </param>
        /// <param name="oid"> The Object id of the object to return data </param>
        /// <param name="position"> The position of the object to read </param>
        /// <param name="useCache"> To indicate if cache must be used </param>
        /// <param name="attributeNames"> The list of the attribute name for which we need to return a value, an attributename can contain relation like profile.name </param>
        /// <param name="relationAttributeNames"> The original names of attributes to read the values, an attributename can contain relation like profile.name </param>
        /// <param name="recursionLevel"> The recursion level of this call </param>
        /// <returns> A Map where keys are attributes names and values are the values of there attributes @ </returns>
        private AttributeValuesMap ReadObjectInfoValuesFromPosition(ClassInfo classInfo, OID oid, long position,
                                                                    bool useCache, IList<string> attributeNames,
                                                                    IList<string> relationAttributeNames,
                                                                    int recursionLevel)
        {
            _currentDepth++;
            // The resulting map
            var map = new AttributeValuesMap();
            // Protection against bad parameter value
            if (position > _fileSystemInterface.GetLength())
            {
                throw new OdbRuntimeException(
                    NDatabaseError.InstancePositionOutOfFile.AddParameter(position).AddParameter(_fileSystemInterface.GetLength()));
            }
            var cache = GetCache();
            // If object is already being read, simply return its cache - to avoid
            // stackOverflow for cyclic references
            // FIXME check this : should we use cache?
            // Go to the object position
            _fileSystemInterface.SetReadPosition(position);
            // Read the block size of the object
            //TODO:  we are reading blockSize, but not using them, is that needed?
            _fileSystemInterface.ReadInt();
            // Read the block type of the object
            var blockType = _fileSystemInterface.ReadByte();
            if (BlockTypes.IsNull(blockType) || BlockTypes.IsDeletedObject(blockType))
                return map;
            // Checks if what we are reading is only a pointer to the real block, if
            // it is the case, Throw an exception. Pointer are not used anymore
            if (BlockTypes.IsPointer(blockType))
                throw new CorruptedDatabaseException(NDatabaseError.FoundPointer.AddParameter(oid).AddParameter(position));
            try
            {
                // Read the header of the object, no need to cache when reading
                // object infos
                // For local mode, we need to use cache to get unconnected objects.
                // TestDelete.test14
                var objectInfoHeader = GetObjectInfoHeader(oid, position, true, cache);
                //TODO: Get the object id is that needed?
                objectInfoHeader.GetOid();
                // If class info is not defined, define it
                if (classInfo == null) { classInfo = GetClassInfoFromObjectId(objectInfoHeader.GetClassInfoId()); }
                if (recursionLevel == 0)
                    map.SetObjectInfoHeader(objectInfoHeader);
                // If object is native, it can have attributes, just return the
                // empty
                // map
                if (BlockTypes.IsNative(blockType))
                    return map;
                var nbAttributes = attributeNames.Count;
                // The query contains a list of attribute to search
                // Loop on attribute to search
                for (var attributeIndex = 0; attributeIndex < nbAttributes; attributeIndex++)
                {
                    var attributeNameToSearch = attributeNames[attributeIndex];
                    var relationNameToSearch = relationAttributeNames[attributeIndex];
                    // If an attribute name has a ., it is a relation
                    var mustNavigate = attributeNameToSearch.IndexOf(".", StringComparison.Ordinal) != -1;
                    long attributeIdentification;
                    long attributePosition;
                    OID attributeOid = null;
                    ClassAttributeInfo cai;
                    if (mustNavigate)
                    {
                        // Get the relation name and the relation attribute name
                        // profile.name => profile = singleAttributeName, name =
                        // relationAttributeName
                        var firstDotIndex = attributeNameToSearch.IndexOf(".", StringComparison.Ordinal);
                        var relationAttributeName = attributeNameToSearch.Substring(firstDotIndex + 1);
                        var singleAttributeName = attributeNameToSearch.Substring(0, firstDotIndex);

                        var attributeId = classInfo.GetAttributeId(singleAttributeName);
                        cai = classInfo.GetAttributeInfo(attributeId, attributeNameToSearch);

                        // Gets the identification (id or position from the object
                        // info) for the attribute with the id of the class
                        // attribute info
                        attributeIdentification = objectInfoHeader.GetAttributeIdentificationFromId(cai.GetId());
                        // When object is non native, then attribute identification
                        // is the oid of the object. It is stored as negative, so we
                        // must do *-1
                        if (!cai.IsNative())
                        {
                            // Relations can be null
                            if (attributeIdentification == StorageEngineConstant.NullObjectIdId)
                            {
                                map.Add(attributeNameToSearch, null);
                                continue;
                            }
                            attributeOid = OIDFactory.BuildObjectOID(-attributeIdentification);
                            attributePosition = GetObjectPositionFromItsOid(attributeOid, useCache, false);
                            IOdbList<string> list1 = new OdbList<string>(1);
                            list1.Add(relationAttributeName);
                            IOdbList<string> list2 = new OdbList<string>(1);
                            list2.Add(attributeNameToSearch);
                            map.PutAll(ReadObjectInfoValuesFromPosition(cai.GetClassInfo(), attributeOid,
                                                                        attributePosition, useCache, list1, list2,
                                                                        recursionLevel + 1));
                        }
                        else
                        {
                            throw new OdbRuntimeException(
                                NDatabaseError.CriteriaQueryUnknownAttribute.AddParameter(attributeNameToSearch).
                                    AddParameter(classInfo.FullClassName));
                        }
                    }
                    else
                    {
                        var attributeId = classInfo.GetAttributeId(attributeNameToSearch);

                        cai = classInfo.GetAttributeInfo(attributeId, attributeNameToSearch);

                        // Gets the identification (id or position from the object
                        // info) for the attribute with the id of the class
                        // attribute info
                        attributeIdentification = objectInfoHeader.GetAttributeIdentificationFromId(cai.GetId());
                        // When object is non native, then attribute identification
                        // is the oid of the object. It is stored as negative, so we
                        // must do *-1
                        if (cai.IsNonNative())
                            attributeOid = OIDFactory.BuildObjectOID(-attributeIdentification);
                        // For non native object, the identification is the oid,
                        // which is stored as negative long
                        // TODO The attributeIdentification <0 clause should not be necessary
                        // But there is a case (found by Jeremias) where even for non
                        // native the attribute is a position and not an id! identification
                        if (cai.IsNonNative() && attributeIdentification < 0)
                            attributePosition = GetObjectPositionFromItsOid(attributeOid, useCache, false);
                        else
                            attributePosition = attributeIdentification;
                        if (attributePosition == StorageEngineConstant.DeletedObjectPosition ||
                            attributePosition == StorageEngineConstant.NullObjectPosition ||
                            attributePosition == StorageEngineConstant.FieldDoesNotExist)
                        {
                            // TODO is this correct?
                            continue;
                        }
                        _fileSystemInterface.SetReadPosition(attributePosition);
                        object @object;
                        if (cai.IsNative())
                        {
                            var aoi = ReadNativeObjectInfo(cai.GetAttributeType().Id, attributePosition, useCache,
                                                           true, true);
                            @object = aoi.GetObject();
                            map.Add(relationNameToSearch, @object);
                        }
                        else
                        {
                            var nnoi = ReadNonNativeObjectInfoFromOid(cai.GetClassInfo(), attributeOid, true, false);
                            @object = nnoi.GetObject();
                            if (@object == null)
                            {
                            }
                            //object = instanceBuilder.buildOneInstance(nnoi);
                            map.Add(relationNameToSearch, nnoi.GetOid());
                        }
                    }
                }
                return map;
            }
            finally
            {
                _currentDepth--;
            }
        }

        private ObjectInfoHeader GetObjectInfoHeader(OID oid, long position, bool useCache, IOdbCache cache)
        {
            // first check if the object info pointers exist in the cache
            ObjectInfoHeader objectInfoHeader = null;
            if (useCache && oid != null)
                objectInfoHeader = cache.GetObjectInfoHeaderByOid(oid, false);
            if (objectInfoHeader == null)
            {
                // Here we read by position because it is possible to have the
                // oid == null. And it is faster by position than by oid
                objectInfoHeader = ReadObjectInfoHeaderFromPosition(oid, position, false);
                var oidWasNull = oid == null;
                oid = objectInfoHeader.GetOid();
                if (useCache)
                {
                    var needToUpdateCache = true;
                    if (oidWasNull)
                    {
                        // The oid was null, now we have it, check the cache again !
                        var cachedOih = cache.GetObjectInfoHeaderByOid(oid, false);
                        if (cachedOih != null)
                        {
                            // Then use the one from the cache
                            objectInfoHeader = cachedOih;
                            // In this case the cache is up to date , no need to
                            // update
                            needToUpdateCache = false;
                        }
                    }
                    if (needToUpdateCache)
                        cache.AddObjectInfoOfNonCommitedObject(objectInfoHeader);
                }
            }
            return objectInfoHeader;
        }

        /// <summary>
        ///   Read the header of a native attribute <pre>The header contains
        ///                                           - The block size = int
        ///                                           - The block type = byte
        ///                                           - The OdbType ID = int
        ///                                           - A boolean to indicate if object is nulls.</pre>
        /// </summary>
        /// <remarks>
        ///   Read the header of a native attribute <pre>The header contains
        ///                                           - The block size = int
        ///                                           - The block type = byte
        ///                                           - The OdbType ID = int
        ///                                           - A boolean to indicate if object is nulls.
        ///                                           This method reads all the bytes and then convert the byte array to the values</pre>
        /// </remarks>
        private NativeAttributeHeader ReadNativeAttributeHeader()
        {
            var nah = new NativeAttributeHeader();
            var size = OdbType.Integer.Size + OdbType.Byte.Size + OdbType.Integer.Size +
                       OdbType.Boolean.Size;
            var bytes = _fileSystemInterface.ReadBytes(size);
            //TODO: block size and type not used
            ByteArrayConverter.ByteArrayToInt(bytes);

            var odbTypeId = ByteArrayConverter.ByteArrayToInt(bytes, 5);
            var isNull = ByteArrayConverter.ByteArrayToBoolean(bytes, 9);

            nah.SetOdbTypeId(odbTypeId);
            nah.SetNull(isNull);
            return nah;
        }

        /// <summary>
        ///   Reads a meta representation of a native object
        /// </summary>
        /// <param name="odbDeclaredTypeId"> The type of attribute declared in the ClassInfo. May be different from actual attribute type in caso of OID and OdbObjectId </param>
        /// <param name="position"> </param>
        /// <param name="useCache"> </param>
        /// <param name="returnObject"> </param>
        /// <param name="readHeader"> </param>
        /// <returns> The native object representation @ </returns>
        private AbstractObjectInfo ReadNativeObjectInfo(int odbDeclaredTypeId, long position, bool useCache,
                                                        bool returnObject, bool readHeader)
        {
            var positionAsString = position.ToString();
            Log4NetHelper.Instance.LogDebugMessage(OdbString.DepthToSpaces(_currentDepth) + "ObjectReader: Reading native object of type " +
                          OdbType.GetNameFromId(odbDeclaredTypeId) + " at position " + positionAsString);
            // The realType is initialized with the declared type
            var realTypeId = odbDeclaredTypeId;
            if (readHeader)
            {
                var nah = ReadNativeAttributeHeader();
                // since version 3 of ODB File Format, the native object header has
                // an info to indicate
                // if object is null!
                if (nah.IsNull())
                    return new NullNativeObjectInfo(odbDeclaredTypeId);
                realTypeId = nah.GetOdbTypeId();
            }
            
            if (OdbType.IsNull(realTypeId)) { return new NullNativeObjectInfo(realTypeId); }
            




            if (OdbType.IsAtomicNative(realTypeId)) { return ReadAtomicNativeObjectInfo(realTypeId); }
            if (OdbType.IsArray(realTypeId)) { return ReadArrayFromDatabaseFile(position, useCache, returnObject); }
            if (OdbType.IsEnum(realTypeId)) { return ReadEnumeration(); }


            throw new OdbRuntimeException(NDatabaseError.NativeTypeNotSupported.AddParameter(realTypeId));
        }










        
    }
}