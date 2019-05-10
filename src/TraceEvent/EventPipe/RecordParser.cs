using FastSerialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    // RecordType and RecordField have circular references in their static fields so we need to
    // set that up explicitly
    internal static class RecordTypeAndFieldInitializer
    {
        public static void Init()
        {
            lock (typeof(RecordTypeAndFieldInitializer))
            {
                if (RecordType.Type != null)
                {
                    return;
                }
                RecordType.Type = new RecordType(100, "Type", typeof(RecordType));
                RecordType.Field = new RecordType(101, "Field", typeof(RecordField));
                RecordType.Stream = new RecordType(102, "Stream", typeof(RecordStream));
                RecordType.Table = new RecordType(103, "Table", typeof(RecordTable));
                RecordType.Type.AddField(RecordField.TypeName = new RecordField(1, "Name", RecordType.Type, RecordType.String));
                RecordType.Type.AddField(RecordField.TypeId = new RecordField(2, "Id", RecordType.Type, RecordType.Int32));
                RecordType.Field.AddField(RecordField.FieldName = new RecordField(3, "Name", RecordType.Field, RecordType.String));
                RecordType.Field.AddField(RecordField.FieldId = new RecordField(4, "Id", RecordType.Field, RecordType.Int32));
                RecordType.Field.AddField(RecordField.FieldContainingType = new RecordField(5, "ContainingType", RecordType.Field, RecordType.Type));
                RecordType.Field.AddField(RecordField.FieldFieldType = new RecordField(6, "FieldType", RecordType.Field, RecordType.Type));
                RecordType.Stream.AddField(RecordField.StreamName = new RecordField(7, "Name", RecordType.Stream, RecordType.String));
                RecordType.Stream.AddField(RecordField.StreamId = new RecordField(8, "Id", RecordType.Stream, RecordType.Int32));
                RecordType.Stream.AddField(RecordField.StreamItemType = new RecordField(9, "ItemType", RecordType.Stream, RecordType.Type));
                RecordType.Table.AddField(RecordField.TableName = new RecordField(10, "Name", RecordType.Table, RecordType.String));
                RecordType.Table.AddField(RecordField.TypeId = new RecordField(11, "Id", RecordType.Table, RecordType.Int32));
                RecordType.Table.AddField(RecordField.TableItemType = new RecordField(12, "ItemType", RecordType.Table, RecordType.Type));
                RecordType.Table.AddField(RecordField.TablePrimaryKeyField = new RecordField(13, "PrimaryKeyField", RecordType.Table, RecordType.Field));
            }
        }
    }

    internal class RecordType : Record
    {
        // Well-known primitive types
        public static RecordType Boolean = new RecordType((int)TypeCode.Boolean, "Boolean", typeof(bool));
        public static RecordType UInt8 = new RecordType((int)TypeCode.Byte, "UInt8", typeof(byte));
        public static RecordType Int16 = new RecordType((int)TypeCode.Int16, "Int16", typeof(short));
        public static RecordType Int32 = new RecordType((int)TypeCode.Int32, "Int32", typeof(int));
        public static RecordType Int64 = new RecordType((int)TypeCode.Int64, "Int64", typeof(long));
        public static RecordType String = new RecordType((int)TypeCode.String, "String", typeof(string));
        public static RecordType Guid = new RecordType(19, "Guid", typeof(Guid));

        // Well-known record types
        public static RecordType Type;
        public static RecordType Field;
        public static RecordType Stream;
        public static RecordType Table;

        static RecordType()
        {
            RecordTypeAndFieldInitializer.Init();
        }

        public int Id;
        public string Name;
        public Type ReflectionType;

        class WeakFieldTypeInfo
        {
            public Type CanonFieldSlotType;
            public int TypeIndex;
            public int FieldCount;
        }
        int _countWeakFieldTypes;
        Dictionary<int, WeakFieldTypeInfo> _weakFieldTypesByIndex = new Dictionary<int, WeakFieldTypeInfo>();
        Dictionary<Type, WeakFieldTypeInfo> _weakFieldTypes = new Dictionary<Type, WeakFieldTypeInfo>();
        List<RecordField> _fields = new List<RecordField>();

        class FieldPathGetterSetter
        {
            public RecordField[] FieldPath;
            public Expression FieldExpression;
            public Delegate Getter;
            public Delegate Setter;
        }
        List<FieldPathGetterSetter> _fieldDelegates = new List<FieldPathGetterSetter>();
        Action<Record> _initRecord;
        ParameterExpression _recordParameterExpression;

        public RecordType()
        {
            _recordParameterExpression = Expression.Parameter(typeof(Record), "record");
        }

        public RecordType(int id, string name, Type accessType, params RecordField[] fields) : this()
        {
            Id = id;
            Name = name;
            ReflectionType = accessType;
            if(fields != null)
            {
                foreach (RecordField f in fields)
                    AddField(f);
            }
        }

        public void AddField(RecordField field)
        {
            if (null == GetStrongBackingField(field))
            {
                Type fieldSlotType = GetCanonFieldSlotType(field);
                WeakFieldTypeInfo typeFieldInfo;
                if (!_weakFieldTypes.TryGetValue(fieldSlotType, out typeFieldInfo))
                {
                    typeFieldInfo = new WeakFieldTypeInfo();
                    typeFieldInfo.TypeIndex = _countWeakFieldTypes++;
                    typeFieldInfo.CanonFieldSlotType = fieldSlotType;
                    _weakFieldTypes[fieldSlotType] = typeFieldInfo;
                    _weakFieldTypesByIndex[typeFieldInfo.TypeIndex] = typeFieldInfo;
                }
                field.DynamicFieldTypeIndex = typeFieldInfo.TypeIndex;
                field.DynamicFieldIndex = typeFieldInfo.FieldCount++;
            }
            _fields.Add(field);
            _initRecord = null;
        }

        public RecordField GetField(string fieldName)
        {
            return _fields.Where(f => f.Name == fieldName).FirstOrDefault();
        }

        private Type GetCanonFieldSlotType(RecordField field)
        {
            Type accessType = field.FieldType.ReflectionType;
            if (accessType.GetTypeInfo().IsValueType)
            {
                return accessType;
            }
            else
            {
                return typeof(object);
            }
        }

        public Delegate GetFieldReadDelegate(RecordField[] fieldPath)
        {
            FieldPathGetterSetter getterSetter = GetOrCreateFieldGetterSetter(fieldPath);
            return getterSetter.Getter;
        }

        public void InitRecord(Record record)
        {
            if (_initRecord == null)
            {
                Expression dynamicFieldsField = GetDynamicFieldsField(_recordParameterExpression);
                List<Expression> typedArrays = new List<Expression>();
                for (int i = 0; i < _countWeakFieldTypes; i++)
                {
                    WeakFieldTypeInfo weakFieldTypeInfo = _weakFieldTypesByIndex[i];
                    Type slotType = weakFieldTypeInfo.CanonFieldSlotType;
                    typedArrays.Add(Expression.NewArrayBounds(slotType, Expression.Constant(weakFieldTypeInfo.FieldCount)));
                }
                Expression weaklyTypeFields = Expression.NewArrayInit(typeof(object), typedArrays);
                Expression body = Expression.Assign(dynamicFieldsField, weaklyTypeFields);
                _initRecord = (Action<Record>)Expression.Lambda(typeof(Action<Record>), body, _recordParameterExpression).Compile();
            }
            _initRecord(record);
        }

        public Expression GetStoreFieldExpression(Expression record, Expression fieldVal, RecordField field)
        {
            Expression fieldRef = GetBackingFieldExpression(record, this, 0, new RecordField[] { field });
            Expression castedFieldVal = Expression.ConvertChecked(fieldVal, fieldRef.Type);
            var body = Expression.Assign(fieldRef, castedFieldVal);
            return body;
        }

        public Expression CreateFieldExpression(Expression record, RecordField[] fieldPath)
        {
            return GetBackingFieldExpression(record, this, 0, fieldPath);
        }

        public override string ToString()
        {
            return Name + "(" + Id + ")";
        }

        FieldPathGetterSetter GetOrCreateFieldGetterSetter(RecordField[] fieldPath)
        {
            foreach (FieldPathGetterSetter getterSetter in _fieldDelegates)
            {
                if (getterSetter.FieldPath.SequenceEqual(fieldPath))
                {
                    return getterSetter;
                }
            }
            FieldPathGetterSetter newGetterSetter = new FieldPathGetterSetter();
            Type fieldType = fieldPath[fieldPath.Length - 1].FieldType.ReflectionType;
            newGetterSetter.FieldPath = fieldPath;
            newGetterSetter.FieldExpression = CreateFieldExpression(_recordParameterExpression, fieldPath);
            newGetterSetter.Getter = CreateGetFieldDelegate(newGetterSetter.FieldExpression, fieldType, _recordParameterExpression);
            _fieldDelegates.Add(newGetterSetter);
            return newGetterSetter;
        }

        Delegate CreateGetFieldDelegate(Expression field, Type fieldType, ParameterExpression recordParameter)
        {
            var delegateType = typeof(Func<,>).MakeGenericType(ReflectionType, fieldType);
            if (field.Type != fieldType)
            {
                //field may be the canonical object type and we we need to cast it back to the precise type
                field = Expression.Convert(field, fieldType);
            }
            return Expression.Lambda(delegateType, field, recordParameter).Compile();
        }

        Expression GetBackingFieldExpression(Expression recordObj, RecordType recordType, int level, RecordField[] fieldPath)
        {
            RecordField derefField = fieldPath[level];
            Debug.Assert(typeof(Record).IsAssignableFrom(recordObj.Type));
            Debug.Assert(recordType.ReflectionType == recordObj.Type);
            Expression fieldValExpr = null;
            FieldInfo strongBackingField = GetStrongBackingField(derefField);
            if (strongBackingField != null)
            {
                fieldValExpr = Expression.Field(recordObj, strongBackingField);
            }
            else
            {
                Expression dynamicFieldsArray = GetDynamicFieldsField(recordObj);
                Expression dynamicFieldsTypedSlotArray = Expression.ArrayAccess(dynamicFieldsArray,
                                                                 Expression.Constant(derefField.DynamicFieldTypeIndex));
                Type fieldSlotArrayType = GetCanonFieldSlotType(derefField).MakeArrayType();
                fieldValExpr = Expression.ArrayAccess(Expression.Convert(dynamicFieldsTypedSlotArray, fieldSlotArrayType),
                                              Expression.Constant(derefField.DynamicFieldIndex));
            }

            if (level == fieldPath.Length - 1)
            {
                return fieldValExpr;
            }
            else
            {
                return GetBackingFieldExpression(fieldValExpr, derefField.FieldType, level + 1, fieldPath);
            }
        }

        Type GetStrongBackingType()
        {
            return (typeof(Record) != ReflectionType && typeof(Record).IsAssignableFrom(ReflectionType)) ? ReflectionType : null;
        }

        static Expression GetDynamicFieldsField(Expression recordObj)
        {
            FieldInfo dynamicFieldsField = recordObj.Type.GetField("DynamicFields", BindingFlags.Public | BindingFlags.Instance);
            return Expression.Field(recordObj, dynamicFieldsField);
        }

        static FieldInfo GetStrongBackingField(RecordField recordField)
        {
            FieldInfo reflectionField = recordField.ContainingType.GetStrongBackingType()?.GetField(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            FieldInfo baseReflectionField = typeof(Record).GetField(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            if (reflectionField == null ||
                baseReflectionField != null ||
                reflectionField.FieldType != recordField.FieldType.ReflectionType)
            {
                return null;
            }
            return reflectionField;
        }
    }

    internal class RecordField : Record
    {
        // well-known fields
        public static RecordField TypeName;
        public static RecordField TypeId;
        public static RecordField FieldName;
        public static RecordField FieldId;
        public static RecordField FieldContainingType; 
        public static RecordField FieldFieldType;      
        public static RecordField StreamName;
        public static RecordField StreamId;
        public static RecordField StreamItemType;
        public static RecordField TableName;
        public static RecordField TableId;
        public static RecordField TableItemType;
        public static RecordField TablePrimaryKeyField;

        static RecordField()
        {
            RecordTypeAndFieldInitializer.Init();
        }

        public int Id;
        public string Name;
        public RecordType ContainingType;
        public RecordType FieldType;

        public int DynamicFieldTypeIndex;
        public int DynamicFieldIndex;

        public RecordField() { }
        public RecordField(int id, string name, RecordType containingType, RecordType fieldType)
        {
            Id = id;
            Name = name;
            ContainingType = containingType;
            FieldType = fieldType;
        }

        public override string ToString()
        {
            return ContainingType.Name + "." + Name + "(" + Id + ")";
        }
    }

    internal enum ParseInstructionType
    {
        StoreConstant,
        StoreRead,
        StoreField,
        StoreFieldLookup,
        AddConstant,
        AddRead,
        AddField,
        Publish
    }

    internal class ParseInstruction
    {
        public static ParseInstruction StoreConstant(RecordField storeField, object constant)
        {
            return new ParseInstruction(ParseInstructionType.StoreConstant, storeField)
            {
                Constant = constant
            };
        }
        public static ParseInstruction StoreRead(RecordField storeField, ParseRule parseRule)
        {
            return new ParseInstruction(ParseInstructionType.StoreRead, storeField)
            {
                ParseRule = parseRule
            };
        }
        public static ParseInstruction StoreField(RecordField storeField, RecordField[] loadFieldPath)
        {
            return new ParseInstruction(ParseInstructionType.StoreField, storeField)
            {
                LoadFieldPath = loadFieldPath
            };
        }
        public static ParseInstruction StoreFieldLookup(RecordField storeField, RecordField[] loadFieldPath, RecordTable lookupTable)
        {
            return new ParseInstruction(ParseInstructionType.StoreFieldLookup, storeField)
            {
                LoadFieldPath = loadFieldPath,
                LookupTable = lookupTable
            };
        }
        public static ParseInstruction Publish(RecordStream publishStream)
        {
            return new ParseInstruction(ParseInstructionType.Publish, null)
            {
                PublishStream = publishStream
            };
        }

        public ParseInstructionType InstructionType { get; private set; }
        public object Constant { get; private set; }
        public RecordField DestinationField { get; private set; }
        public RecordField[] LoadFieldPath { get; private set; }
        public ParseRule ParseRule { get; private set; }
        public RecordTable LookupTable { get; private set; }
        public RecordStream PublishStream { get; private set; }

        private Action<IStreamReader, Record> _cachedExecuteAction;

        private ParseInstruction(ParseInstructionType instructionType, RecordField destinationField)
        {
            InstructionType = instructionType;
            DestinationField = destinationField;
            //TODO: validate instruction?
        }

        public void Execute(IStreamReader streamReader, Record record)
        {
            GetExecuteAction()(streamReader, record);
        }

        Action<IStreamReader, Record> GetExecuteAction()
        {
            if(_cachedExecuteAction == null)
            {
                switch (InstructionType)
                {
                    case (ParseInstructionType.StoreConstant):
                        Expression constantExpression = Expression.Constant(Constant);
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(DestinationField.ContainingType, constantExpression, DestinationField);
                        break;
                    case (ParseInstructionType.StoreRead):
                        Expression readExpression = RecordParseCodeGen.GetParseExpression(ParseRule);
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(DestinationField.ContainingType, readExpression, DestinationField);
                        break;
                    case ParseInstructionType.StoreField:
                        Expression readFieldExpression = RecordParseCodeGen.GetFieldReadExpression(RecordParseCodeGen.RecordParameter,
                            LoadFieldPath, DestinationField.FieldType.ReflectionType);
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(DestinationField.ContainingType, readFieldExpression, DestinationField);
                        break;
                    case ParseInstructionType.StoreFieldLookup:
                        Expression lookupExpression = RecordParseCodeGen.GetFieldReadLookupExpression(RecordParseCodeGen.RecordParameter,
                            LoadFieldPath, LookupTable, DestinationField.FieldType.ReflectionType);
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(DestinationField.ContainingType, lookupExpression, DestinationField);
                        break;
                    case ParseInstructionType.Publish:
                        _cachedExecuteAction = RecordParseCodeGen.GetPublishDelegate(RecordParseCodeGen.RecordParameter, PublishStream);
                        break;
                }
            }
            return _cachedExecuteAction;
        }
    }

    internal static class RecordParseCodeGen
    {
        public static Action<IStreamReader, Record> GetStoreFieldDelegate(RecordType recordType, Expression fieldVal, RecordField field)
        {
            Expression record = RecordParameter;
            if (record.Type != recordType.ReflectionType)
            {
                record = Expression.Convert(record, recordType.ReflectionType);
            }
            Expression body = recordType.GetStoreFieldExpression(record, fieldVal, field);
            var delegateType = typeof(Action<,>).MakeGenericType(typeof(IStreamReader), RecordParameter.Type);
            return (Action<IStreamReader, Record>)
                Expression.Lambda(delegateType, body, StreamReaderParameter, RecordParameter).Compile();
        }

        public static Func<T,U> GetRecordFieldDelegate<T,U>(RecordType recordType, RecordField field)
        {
            Debug.Assert(typeof(T) == recordType.ReflectionType);
            ParameterExpression record = Expression.Parameter(typeof(T), "record");
            Expression body = GetFieldReadExpression(record, new RecordField[] { field }, typeof(U));
            return Expression.Lambda<Func<T,U>>(body, record).Compile();
        }

        public static Action<IStreamReader, Record> GetPublishDelegate(ParameterExpression record, RecordStream stream)
        {
            RecordType recordType = stream.ItemType;
            Expression recordStrong = record;
            if (record.Type != recordType.ReflectionType)
            {
                // the record parameter may have a weaker static type that we need to cast off
                recordStrong = Expression.Convert(record, recordType.ReflectionType);
            }
            MethodInfo addMethod = stream.GetType().GetMethod("Add");
            Expression body = Expression.Call(Expression.Constant(stream), addMethod, recordStrong);
            return Expression.Lambda<Action<IStreamReader, Record>>(body, StreamReaderParameter, record).Compile();
        }

        public static Expression GetFieldReadExpression(Expression record, RecordField[] fieldPath, Type targetType)
        {
            RecordType recordType = fieldPath[0].ContainingType;
            if (record.Type != recordType.ReflectionType)
            {
                // the record parameter may have a weaker static type that we need to cast off
                record = Expression.Convert(record, recordType.ReflectionType);
            }
            Expression fieldRead = recordType.CreateFieldExpression(record, fieldPath);
            if (fieldRead.Type != targetType)
            {
                fieldRead = Expression.Convert(fieldRead, targetType);
            }
            return fieldRead;
        }

        public static Expression GetFieldReadLookupExpression(Expression record, RecordField[] fieldPath, RecordTable table, Type targetType)
        {
            Expression fieldRead = GetFieldReadExpression(record, fieldPath, typeof(int));
            Type tableType = typeof(RecordTable<>).MakeGenericType(table.ItemType.ReflectionType);
            Expression tableExpression = Expression.Constant(table, tableType);
            MethodInfo lookupMethod = tableType.GetMethod("Get");
            Expression lookupResult = Expression.Call(tableExpression, lookupMethod, fieldRead);
            if (lookupResult.Type != targetType)
            {
                lookupResult = Expression.Convert(lookupResult, targetType);
            }
            return lookupResult;
        }

        public static Expression GetParseExpression(ParseRule parseRule, Expression streamReader = null)
        {
            if(streamReader == null)
            {
                streamReader = StreamReaderParameter;
            }
            switch (parseRule.Id)
            {
                case (int)PrimitiveParseRuleId.Boolean:
                    MethodInfo readByte = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.NotEqual(Expression.Call(streamReader, readByte), Expression.Constant((byte)0));
                case (int)PrimitiveParseRuleId.FixedUInt8:
                    MethodInfo readByte2 = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readByte2);
                case (int)PrimitiveParseRuleId.FixedInt16:
                    MethodInfo readInt16 = typeof(IStreamReader).GetMethod("ReadInt16", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt16);
                case (int)PrimitiveParseRuleId.FixedInt32:
                    MethodInfo readInt32 = typeof(IStreamReader).GetMethod("ReadInt32", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt32);
                case (int)PrimitiveParseRuleId.FixedInt64:
                    MethodInfo readInt64 = typeof(IStreamReader).GetMethod("ReadInt64", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt64);
                case (int)PrimitiveParseRuleId.Guid:
                    MethodInfo readGuid = typeof(IStreamWriterExentions).GetMethod("ReadGuid", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readGuid, streamReader);
                case (int)PrimitiveParseRuleId.UTF8String:
                    MethodInfo readUtf8 = typeof(ParseFunctions).GetMethod("ReadUTF8String", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readUtf8, streamReader);
                default:
                    throw new ArgumentException("Parse rule id " + parseRule.Id + " not recognized");
            }
        }

        public static ParameterExpression StreamReaderParameter = Expression.Parameter(typeof(IStreamReader), "streamReader");
        public static ParameterExpression RecordParameter = Expression.Parameter(typeof(Record), "record");
    }

    internal class ParseFunctions
    {
        public static string ReadUTF8String(IStreamReader reader)
        {
            ushort numBytes = ReadVarUInt16(reader);
            byte[] bytes = new byte[numBytes];
            reader.Read(bytes, 0, numBytes);
            return Encoding.UTF8.GetString(bytes, 0, numBytes);
        }

        public static ushort ReadVarUInt16(IStreamReader reader)
        {
            if(!TryReadVarUInt(reader, out ulong val) || val > ushort.MaxValue)
            {
                throw new SerializationException("Invalid VarUInt16");
            }
            return (ushort)val;
        }

        public static uint ReadVarUInt32(IStreamReader reader)
        {
            if (!TryReadVarUInt(reader, out ulong val) || val > uint.MaxValue)
            {
                throw new SerializationException("Invalid VarUInt32");
            }
            return (uint)val;
        }

        public static ulong ReadVarUInt64(IStreamReader reader)
        {
            if (!TryReadVarUInt(reader, out ulong val))
            {
                throw new SerializationException("Invalid VarUInt64");
            }
            return val;
        }

        public static bool TryReadVarUInt(IStreamReader reader, out ulong val)
        {
            val = 0;
            int shift = 0;
            byte b;
            do
            {
                if(shift == 10*7)
                {
                    return false;
                }
                b = reader.ReadByte();
                val |= (uint)(b & 0x7f) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return true;
        }
    }

    /// <summary>
    /// Writes well known Record types in a format that can be deserialized using the well known ParseRules
    /// </summary>
    internal static class RecordWriter
    {
        public static void Write(BinaryWriter writer, RecordType recordType)
        {
            writer.Write((int)recordType.Id);
            WriteUTF8String(writer, recordType.Name);
        }

        public static void Write(BinaryWriter writer, RecordField recordField)
        {
            writer.Write((int)recordField.Id);
            //writer.Write((int)recordField.ContainingType.Id);
            //writer.Write((int)recordField.FieldType.Id);
            WriteUTF8String(writer, recordField.Name);
        }

        public static void WriteUTF8String(BinaryWriter writer, string val)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(val);
            if(bytes.Length > ushort.MaxValue)
            {
                throw new SerializationException("string is too long for this encoding");
            }
            WriteVarUInt(writer, (ulong)bytes.Length);
            writer.Write(bytes);
        }

        public static void WriteVarUInt(BinaryWriter writer, ulong val)
        {
            while(val >= 0x80)
            {
                writer.Write((byte)(val & 0x7F) | 0x80);
                val >>= 7;
            }
            writer.Write((byte)val);
        }
    }

    internal enum PrimitiveParseRuleId
    {
        Boolean = 0,
        FixedUInt8 = 1,
        FixedInt16 = 2,
        FixedInt32 = 3,
        FixedInt64 = 4,
        VarUInt16 =  5,     // UInt16 is divided into 7bit chunks from least significant chunk to most significant chunk
                            // Each 7 bit chunk is written as a byte where a 1 bit is prepended if there are more chunks
                            // to come and 0 if it is the last chunk. This encoding uses at most 3 bytes. Example:
                            // Decimal: 53,241
                            // Binary: 1100 1111 1111 1001
                            // 7 bit chunks ordered least->most significant: 1111001 0011111 0000011
                            // Byte encoding: 11111001 10011111 00000011
        VarUInt32 =  6,     
        VarUInt64 =  7,
        UTF8String = 8,     // Default string parser is a VarUInt32 count, followed by count bytes (not chars!) of UTF8 data
                            // Example: 0x3,        0x41, 0x42, 0x43
                            //          Len=3       'A'   'B'   'C'  = "ABC"
        Guid       = 9,

        // add new rules here and increase Count
        Count      = 10 
    }



    internal class ParseRule
    {
        public static ParseRule Boolean = new ParseRule((int)PrimitiveParseRuleId.Boolean, RecordType.Boolean);
        public static ParseRule FixedUInt8 = new ParseRule((int)PrimitiveParseRuleId.FixedUInt8, RecordType.UInt8);
        public static ParseRule FixedInt16 = new ParseRule((int)PrimitiveParseRuleId.FixedInt16, RecordType.Int16);
        public static ParseRule FixedInt32 = new ParseRule((int)PrimitiveParseRuleId.FixedInt32, RecordType.Int32);
        public static ParseRule FixedInt64 = new ParseRule((int)PrimitiveParseRuleId.FixedInt64, RecordType.Int64);
        public static ParseRule UTF8String = new ParseRule((int)PrimitiveParseRuleId.UTF8String, RecordType.String);
        public static ParseRule Guid = new ParseRule((int)PrimitiveParseRuleId.Guid, RecordType.Guid);

        public static ParseRule Type = new ParseRule(500, RecordType.Type,
            ParseInstruction.StoreRead(RecordType.Type.GetField("Id"), ParseRule.FixedInt32),
            ParseInstruction.StoreRead(RecordType.Type.GetField("Name"), ParseRule.UTF8String));
        public static ParseRule Field = new ParseRule(501, RecordType.Field,
            ParseInstruction.StoreRead(RecordType.Field.GetField("Id"), ParseRule.FixedInt32),
            //ParseInstruction.StoreRead(RecordType.Type.GetField("ContainingTypeId"), ParseRule.FixedInt32),
            //ParseInstruction.StoreRead(RecordType.Type.GetField("FieldTypeId"), ParseRule.FixedInt32),
            ParseInstruction.StoreRead(RecordType.Field.GetField("Name"), ParseRule.UTF8String));


        public int Id;
        public RecordType ParsedType;
        public ParseInstruction[] Instructions;

        public ParseRule(int id, RecordType parsedType, params ParseInstruction[] instructions)
        {
            Id = id;
            ParsedType = parsedType;
            Instructions = instructions;
            //TODO: validate instructions
        }

        public void Parse<T>(IStreamReader reader, T record) where T : Record
        {
            if(!ParsedType.ReflectionType.IsAssignableFrom(typeof(T)))
            {
                throw new ArgumentException("Expected record of type " + ParsedType.ReflectionType.FullName + ", actual type is " + typeof(T).FullName);
            }
            if(Id < (int)PrimitiveParseRuleId.Count)
            {
                // Use a StoreRead or AddRead instruction to do the parse indirectly
                // The primitive parsers produce types that don't derive from Record and may not be reference types which makes them 
                // more awkward to work with in the API.
                throw new InvalidOperationException("Parse not supported for primitive ParseRules");
            }
            record.Init(ParsedType);
            foreach(ParseInstruction instruction in Instructions)
            {
                instruction.Execute(reader, record);
            }
        }
    }

    internal class Record
    {
        public object[] DynamicFields;
        private RecordType _recordType;

        public void Init(RecordType recordType)
        {
            _recordType = recordType;
            _recordType.InitRecord(this);
        }

        public T GetFieldValue<T>(params RecordField[] fieldPath)
        {
            return ((Func<Record,T>)_recordType.GetFieldReadDelegate(fieldPath))(this);
        }
    }

    internal class RecordStream : Record
    {
        public string Name;
        public RecordType ItemType;
        public int Id;

    }
    internal class RecordStream<T> : RecordStream
    {   
        public RecordStream(string name, RecordType itemType)
        {
            Name = name;
            ItemType = itemType;
        }
        public virtual void Add(T item) { }
    }
    internal class RecordTable : RecordStream
    {
        public RecordField PrimaryKeyField;
    }
    internal class RecordTable<T> : RecordTable
    {
        Func<T, int> _getKeyDelegate;
        Dictionary<int, T> _lookupTable = new Dictionary<int, T>();

        public RecordTable(string name, RecordField primaryKeyField)
        {
            Name = name;
            ItemType = primaryKeyField.ContainingType;
            PrimaryKeyField = primaryKeyField;
            _getKeyDelegate = RecordParseCodeGen.GetRecordFieldDelegate<T, int>(ItemType, PrimaryKeyField);
        }

        public virtual T Add(T item)
        {
            int key = _getKeyDelegate(item);
            _lookupTable.Add(key, item);
            return item;
        }
        public bool ContainsKey(int key)
        {
            return _lookupTable.ContainsKey(key);
        }
        public T Get(int key)
        {
            return _lookupTable[key];
        }
    }

    internal class RecordParserContext
    {
        public RecordTypeTable Types { get; private set; } = new RecordTypeTable();
        public RecordFieldTable Fields { get; private set; } = new RecordFieldTable();
    }

    internal class RecordTypeTable : RecordTable<RecordType>
    {
        Dictionary<string, RecordType> _nameToType = new Dictionary<string, RecordType>();

        public RecordTypeTable() : base("Type", RecordType.Type.GetField("Id"))
        {
            Id = 1;
            Add(RecordType.Boolean);
            Add(RecordType.UInt8);
            Add(RecordType.Int16);
            Add(RecordType.Int32);
            Add(RecordType.Int64);
            Add(RecordType.String);
            Add(RecordType.Guid);
            Add(RecordType.Type);
            Add(RecordType.Field);
        }

        public override RecordType Add(RecordType item)
        {
            if(string.IsNullOrEmpty(item.Name))
            {
                throw new ArgumentException("RecordType Name must be non-empty");
            }
            if(ContainsKey(item.Id))
            {
                throw new ArgumentException("Can not add new type " + item.ToString() + " because the Id is already in use by " + Get(item.Id).ToString());
            }
            if(_nameToType.ContainsKey(item.Name))
            {
                throw new ArgumentException("Can not add new type " + item.ToString() + " because the Name is already in use by " + Get(item.Name).ToString());
            }
            _nameToType.Add(item.Name, item);
            return base.Add(item);
        }

        public RecordType Get(string name)
        {
            return _nameToType[name];
        }
    }

    internal class RecordFieldTable : RecordTable<RecordField>
    {
        public RecordFieldTable() : base("Field", RecordType.Field.GetField("Id"))
        {
            Id = 2;
            base.Add(RecordField.TypeName);
            base.Add(RecordField.TypeId);
            base.Add(RecordField.FieldName);
            base.Add(RecordField.FieldId);
            base.Add(RecordField.FieldContainingType);
            base.Add(RecordField.FieldFieldType);
        }

        public override RecordField Add(RecordField item)
        {
            if (string.IsNullOrEmpty(item.Name))
            {
                throw new ArgumentException("RecordField.Name must be non-empty");
            }
            if (item.ContainingType == null)
            {
                throw new ArgumentException("RecordField.ContainingType must be non-null");
            }
            if (item.FieldType == null)
            {
                throw new ArgumentException("RecordField.FieldType must be non-null");
            }
            if (ContainsKey(item.Id))
            {
                throw new ArgumentException("Can not add new field " + item.ToString() + " because the Id is already in use by " + Get(item.Id).ToString());
            }
            RecordField existingField = item.ContainingType.GetField(item.Name);
            if (existingField != null)
            {
                throw new ArgumentException("Can not add new field " + item.ToString() + " because the Name is already in use by " + existingField.ToString());
            }
            return base.Add(item);
        }
    }
    internal class RecordTableTable : RecordTable<RecordTable>
    {
        public RecordTableTable() : base("Table", RecordField.TableId)
        {
            Id = 3;
            base.Add(new RecordTypeTable());
            base.Add(new RecordFieldTable());
            base.Add(this);
            base.Add(new RecordStreamTable());
        }
    }
    internal class RecordStreamTable : RecordTable<RecordStream>
    {
        public RecordStreamTable() : base("Stream", RecordField.StreamId) {}
    }
}
