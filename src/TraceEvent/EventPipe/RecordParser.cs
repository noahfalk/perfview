using FastSerialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    internal enum RecordTypeId
    {
        Type = 500,
        Field = 501
    }

    internal class RecordType : Record
    {
        // Well-known primitive types
        public static RecordType Boolean = new RecordType((int)TypeCode.Boolean, "Boolean", typeof(bool));
        public static RecordType Byte = new RecordType((int)TypeCode.Byte, "UInt8", typeof(byte));
        public static RecordType Int16 = new RecordType((int)TypeCode.Int16, "Int16", typeof(short));
        public static RecordType Int32 = new RecordType((int)TypeCode.Int32, "Int32", typeof(int));
        public static RecordType Int64 = new RecordType((int)TypeCode.Int64, "Int64", typeof(long));
        public static RecordType String = new RecordType((int)TypeCode.String, "String", typeof(string));
        public static RecordType Guid = new RecordType(19, "Guid", typeof(Guid));

        // Well-known record types
        public static RecordType Type = new RecordType((int)RecordTypeId.Type, "Type", typeof(RecordType));
        public static RecordType Field = new RecordType((int)RecordTypeId.Field, "Field", typeof(RecordField));

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

        public RecordType(int id, string name, Type accessType)
        {
            Id = id;
            Name = name;
            ReflectionType = accessType;
            _recordParameterExpression = Expression.Parameter(typeof(Record), "record");
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

        Expression CreateFieldExpression(Expression record, RecordField[] fieldPath)
        {
            return GetBackingFieldExpression(record, this, 0, fieldPath);
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

    internal class RecordField
    {
        public int Id;
        public string Name;
        public RecordType ContainingType;
        public RecordType FieldType;

        public int DynamicFieldTypeIndex;
        public int DynamicFieldIndex;

        public RecordField(int id, string name, RecordType containingType, RecordType fieldType)
        {
            Id = id;
            Name = name;
            ContainingType = containingType;
            FieldType = fieldType;
        }
    }



    internal enum ParseInstructionType
    {
        StoreConstant,
        StoreRead,
        StoreField,
        AddConstant,
        AddRead,
        AddField
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

        public ParseInstructionType InstructionType { get; private set; }
        public object Constant { get; private set; }
        public RecordField StoreField { get; private set; }
        public RecordField[] LoadFieldPath { get; private set; }
        public ParseRule ParseRule { get; private set; }

        private Action<IStreamReader, Record> _cachedExecuteAction;

        private ParseInstruction(ParseInstructionType instructionType, RecordField storeField)
        {
            InstructionType = instructionType;
            StoreField = storeField;
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
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(StoreField.ContainingType, constantExpression, StoreField);
                        break;
                    case (ParseInstructionType.StoreRead):
                        Expression readExpression = RecordParseCodeGen.CreateParseExpression(ParseRule);
                        _cachedExecuteAction = RecordParseCodeGen.GetStoreFieldDelegate(StoreField.ContainingType, readExpression, StoreField);
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

        public static Expression CreateParseExpression(ParseRule parseRule, Expression streamReader = null)
        {
            if(streamReader == null)
            {
                streamReader = StreamReaderParameter;
            }
            switch (parseRule.Id)
            {
                case (int)PrimitiveParseRuleId.FixedUInt8:
                    MethodInfo readByte = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readByte);
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
                    MethodInfo readUtf8 = typeof(WellKnownParseFunctions).GetMethod("ReadUTF8String", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readUtf8, streamReader);
                default:
                    throw new ArgumentException("Parse rule id " + parseRule.Id + " not recognized");
            }
        }

        public static ParameterExpression StreamReaderParameter = Expression.Parameter(typeof(IStreamReader), "streamReader");
        public static ParameterExpression RecordParameter = Expression.Parameter(typeof(Record), "record");

        
    }

    internal class WellKnownParseFunctions
    {
        string ReadUTF8String(IStreamReader reader)
        {
            ushort numBytes = (ushort)reader.ReadInt16();
            byte[] bytes = new byte[numBytes];
            reader.Read(bytes, 0, numBytes);
            return Encoding.UTF8.GetString(bytes, 0, numBytes - 1); // numBytes includes a null terminator
        }
    }

    internal enum SerializationSize
    {
        Fixed8,
        Fixed16,
        Fixed32,
        Fixed64,
        Fixed128,
        //CompressedInt,
        //CountedArrayUInt32Fixed8,  // UInt32 is the leading count, followed by that many bytes
        //CountedArrayUInt32Fixed16, // UInt32 is the leading count, followed by that many words
        Complex                    // Described by a series of parse instructions in the corresponding layout 
    }

    internal enum PrimitiveParseRuleId
    {
        FixedUInt8 = 0,
        FixedInt16 = 1,
        FixedInt32 = 2,
        FixedInt64 = 3,
        UTF8String = 4,     // Default string parser is uint16 count, followed by count bytes (not chars!) of UTF8 data
                            // Example: 0x3, 0x0,   0x41, 0x42, 0x43
                            //           Len=3       'A'   'B'   'C'  = "ABC"
        Guid = 5,
        // add new rules here and increase Count

        Count = 6 
    }



    internal class ParseRule
    {
        //TODO: replace SerializationSize with instructions that advance the offset 
        public static ParseRule Guid = new ParseRule((int)PrimitiveParseRuleId.Guid, SerializationSize.Fixed128, RecordType.Guid);
        public static ParseRule FixedUInt8 = new ParseRule((int)PrimitiveParseRuleId.FixedUInt8, SerializationSize.Fixed8, RecordType.Byte);
        public static ParseRule FixedInt16 = new ParseRule((int)PrimitiveParseRuleId.FixedInt16, SerializationSize.Fixed16, RecordType.Int16);
        public static ParseRule FixedInt32 = new ParseRule((int)PrimitiveParseRuleId.FixedInt32, SerializationSize.Fixed32, RecordType.Int32);
        public static ParseRule FixedInt64 = new ParseRule((int)PrimitiveParseRuleId.FixedInt64, SerializationSize.Fixed64, RecordType.Int64);
        public static ParseRule UTF8String = new ParseRule((int)PrimitiveParseRuleId.UTF8String, SerializationSize.Complex, RecordType.String);

        public static ParseRule Type = new ParseRule((int)RecordTypeId.Type, SerializationSize.Complex, RecordType.Type,
            ParseInstruction.StoreRead(RecordType.Type.GetField("Id"), ParseRule.FixedInt32),
            ParseInstruction.StoreRead(RecordType.Type.GetField("Name"), ParseRule.UTF8String));
        public static ParseRule Field = new ParseRule((int)RecordTypeId.Field, SerializationSize.Complex, RecordType.Field,


        public int Id;
        public SerializationSize Size;
        public RecordType ParsedType;
        public ParseInstruction[] Instructions;

        public ParseRule(int id, SerializationSize size, RecordType parsedType, params ParseInstruction[] instructions)
        {
            Id = id;
            Size = size;
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

    internal class RecordParser<T>
    {
        
    }
}
