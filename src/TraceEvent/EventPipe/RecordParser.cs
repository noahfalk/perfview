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
    internal class RecordType
    {
        // Well-known types
        public static RecordType Boolean = new RecordType((int)TypeCode.Boolean, typeof(bool).FullName, typeof(bool));
        public static RecordType Byte = new RecordType((int)TypeCode.Byte, typeof(byte).FullName, typeof(byte));
        public static RecordType Int16 = new RecordType((int)TypeCode.Int16, typeof(short).FullName, typeof(short));
        public static RecordType Int32 = new RecordType((int)TypeCode.Int32, typeof(int).FullName, typeof(int));
        public static RecordType Int64 = new RecordType((int)TypeCode.Int64, typeof(long).FullName, typeof(long));
        public static RecordType String = new RecordType((int)TypeCode.String, typeof(string).FullName, typeof(string));
        public static RecordType Guid = new RecordType(19, typeof(Guid).FullName, typeof(Guid));


        public int Id;
        public string Name;
        public Type Type;

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
        ParameterExpression _recordParameterExpression;

        public RecordType(int id, string name, Type accessType)
        {
            Id = id;
            Name = name;
            Type = accessType;
            _recordParameterExpression = Expression.Parameter(typeof(Record), "record");
        }

        public void AddField(RecordField field)
        {
            if(null == GetStrongBackingField(field))
            {
                Type fieldSlotType = GetCanonFieldSlotType(field);
                WeakFieldTypeInfo typeFieldInfo;
                if(!_weakFieldTypes.TryGetValue(fieldSlotType, out typeFieldInfo))
                {
                    typeFieldInfo = new WeakFieldTypeInfo();
                    typeFieldInfo.TypeIndex = _countWeakFieldTypes++;
                    typeFieldInfo.CanonFieldSlotType = fieldSlotType;
                    _weakFieldTypes[fieldSlotType] = typeFieldInfo;
                    _weakFieldTypesByIndex[typeFieldInfo.TypeIndex] = typeFieldInfo;
                }
                field.WeakFieldTypeIndex = typeFieldInfo.TypeIndex;
                field.WeakFieldIndex = typeFieldInfo.FieldCount++;
            }
            _fields.Add(field);
        }

        private Type GetCanonFieldSlotType(RecordField field)
        {
            Type accessType = field.FieldType.Type;
            if(accessType.GetTypeInfo().IsValueType)
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

        public Action<T> GetStoreFieldDelegate<T>(Expression fieldVal, RecordField[] fieldPath)
        {
            Expression record = _recordParameterExpression;
            if (record.Type != Type)
            {
                record = Expression.Convert(record, Type);
            }
            Expression body = GetStoreFieldExpression(record, fieldVal, fieldPath);
            var delegateType = typeof(Action<>).MakeGenericType(_recordParameterExpression.Type);
            return (Action<T>)Expression.Lambda(delegateType, body, _recordParameterExpression).Compile();
        }

        public Action<Record> GetInitWeakFieldStorageDelegate()
        {
            FieldInfo weaklyTypedFieldsFieldInfo = Type.GetField("WeaklyTypedFields", BindingFlags.Public | BindingFlags.Instance);
            Expression weaklyTypedFieldsField = Expression.Field(_recordParameterExpression, weaklyTypedFieldsFieldInfo);
            List<Expression> typedArrays = new List<Expression>();
            for(int i = 0; i < _countWeakFieldTypes; i++)
            {
                WeakFieldTypeInfo weakFieldTypeInfo = _weakFieldTypesByIndex[i];
                Type slotType = weakFieldTypeInfo.CanonFieldSlotType;
                typedArrays.Add(Expression.NewArrayBounds(slotType, Expression.Constant(weakFieldTypeInfo.FieldCount)));
            }
            Expression weaklyTypeFields = Expression.NewArrayInit(typeof(object), typedArrays);
            Expression body = Expression.Assign(weaklyTypedFieldsField, weaklyTypeFields);
            return (Action<Record>)Expression.Lambda(typeof(Action<Record>), body, _recordParameterExpression).Compile();
        }

        FieldPathGetterSetter GetOrCreateFieldGetterSetter(RecordField[] fieldPath)
        {
            foreach(FieldPathGetterSetter getterSetter in _fieldDelegates)
            {
                if(getterSetter.FieldPath.SequenceEqual(fieldPath))
                {
                    return getterSetter;
                }
            }

            FieldPathGetterSetter newGetterSetter = new FieldPathGetterSetter();
            Type fieldType = fieldPath[fieldPath.Length - 1].FieldType.Type;
            newGetterSetter.FieldPath = fieldPath;
            newGetterSetter.FieldExpression = CreateFieldExpression(_recordParameterExpression, fieldPath);
            newGetterSetter.Getter = CreateGetFieldDelegate(newGetterSetter.FieldExpression, fieldType, _recordParameterExpression);
            _fieldDelegates.Add(newGetterSetter);
            return newGetterSetter;
        }

        Delegate CreateGetFieldDelegate(Expression field, Type fieldType, ParameterExpression recordParameter)
        {
            var delegateType = typeof(Func<,>).MakeGenericType(Type, fieldType);
            if (field.Type != fieldType)
            {
                //field may be the canonical object type and we we need to cast it back to the precise type
                field = Expression.Convert(field, fieldType);
            }
            return Expression.Lambda(delegateType, field, recordParameter).Compile();
        }

        /*
        Delegate CreateSetFieldDelegate(RecordField[] fieldPath)
        {
            Type fieldType = fieldPath[fieldPath.Length - 1].FieldType.Type;
            ParameterExpression recordExpression = Expression.Parameter(Type, "record");
            ParameterExpression newFieldValueExpression = Expression.Parameter(fieldType, "newFieldValue");
            BinaryExpression body = GetStoreFieldExpression(recordExpression, this, newFieldValueExpression, fieldPath);
            var delegateType = typeof(Func<,>).MakeGenericType(Type, fieldType);
            return Expression.Lambda(delegateType, body, recordExpression, newFieldValueExpression).Compile();
        }*/

        Expression CreateFieldExpression(Expression record, RecordField[] fieldPath)
        {
            return GetBackingFieldExpression(record, this, 0, fieldPath);
        }

        public Expression GetStoreFieldExpression(Expression record, Expression fieldVal, RecordField[] fieldPath)
        {
            Expression fieldRef = GetBackingFieldExpression(record, this, 0, fieldPath);
            Expression castedFieldVal = Expression.ConvertChecked(fieldVal, fieldRef.Type);
            var body = Expression.Assign(fieldRef, castedFieldVal);
            return body;
        }

        Expression GetBackingFieldExpression(Expression recordObj, RecordType recordType, int level, RecordField[] fieldPath)
        {
            RecordField derefField = fieldPath[level];
            Debug.Assert(typeof(Record).IsAssignableFrom(recordObj.Type));
            Debug.Assert(recordType.Type == recordObj.Type);
            Expression fieldValExpr = null;
            FieldInfo strongBackingField = GetStrongBackingField(derefField);
            if (strongBackingField != null)
            {
                FieldInfo stronglyTypedFieldsField = recordObj.Type.GetField("StronglyTypedFields", BindingFlags.Public | BindingFlags.Instance); 
                fieldValExpr = Expression.Field(Expression.Field(recordObj,stronglyTypedFieldsField), strongBackingField);
            }
            else
            {
                FieldInfo weaklyTypedFieldsField = recordObj.Type.GetField("WeaklyTypedFields", BindingFlags.Public | BindingFlags.Instance);
                Expression weaklyTypedFieldsArray = Expression.Field(recordObj, weaklyTypedFieldsField);
                Expression weaklyTypedFieldsTypedSlotArray = Expression.ArrayAccess(weaklyTypedFieldsArray,
                                                                 Expression.Constant(derefField.WeakFieldTypeIndex));
                Type fieldSlotArrayType = GetCanonFieldSlotType(derefField).MakeArrayType();
                fieldValExpr = Expression.ArrayAccess(Expression.Convert(weaklyTypedFieldsTypedSlotArray, fieldSlotArrayType),
                                              Expression.Constant(derefField.WeakFieldIndex));
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
            if (Type.IsConstructedGenericType && Type.GetGenericTypeDefinition() == typeof(Record<>))
            {
                return Type.GetGenericArguments()[0];
            }
            return null;
        }

        static FieldInfo GetStrongBackingField(RecordField recordField)
        {
            FieldInfo reflectionField = recordField.ContainingType.GetStrongBackingType()?.GetField(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            if (reflectionField == null || reflectionField.FieldType != recordField.FieldType.Type)
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

        public int WeakFieldTypeIndex;
        public int WeakFieldIndex;

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
        FieldStoreConstant,
        FieldStoreRead,
        FieldStoreField,
        FieldStoreZero,
        FieldAddConstant,
        FieldAddRead,
        FieldAddField
    }

    internal class ParseInstruction
    {
        public ParseInstructionType InstructionType;
        public object InlineConstant;
        public RecordField[] StoreFieldPath;
        public RecordField[] LoadFieldPath;
        public ParseRule ParseRule;

        public void Execute(IStreamReader streamReader, Record record)
        {
            switch(InstructionType)
            {
                case (ParseInstructionType.FieldStoreConstant):
                    record.RecordType.GetStoreFieldDelegate<Record>(Expression.Constant(InlineConstant), StoreFieldPath)(record);
                    break;
                case (ParseInstructionType.FieldStoreRead):
                    Expression readExpression = ParseRule.CreateParseExpression(_streamReaderParameterExpression);
                    Action<IStreamReader, Record> action =
                        GetStoreFieldDelegate(StoreFieldPath[0].ContainingType, readExpression, StoreFieldPath);
                    action(streamReader, record);
                    break;
            }
        }

        public Action<IStreamReader,Record> GetStoreFieldDelegate(RecordType recordType, Expression fieldVal, RecordField[] fieldPath)
        {
            Expression record = _recordParameterExpression;
            if (record.Type != recordType.Type)
            {
                record = Expression.Convert(record, recordType.Type);
            }
            Expression body = recordType.GetStoreFieldExpression(record, fieldVal, fieldPath);
            var delegateType = typeof(Action<,>).MakeGenericType(typeof(IStreamReader), _recordParameterExpression.Type);
            return (Action<IStreamReader,Record>)
                Expression.Lambda(delegateType, body, _streamReaderParameterExpression, _recordParameterExpression).Compile();
        }

        static ParameterExpression _streamReaderParameterExpression = Expression.Parameter(typeof(IStreamReader), "streamReader");
        static ParameterExpression _recordParameterExpression = Expression.Parameter(typeof(Record), "record");
    }

    internal enum SerializationSize
    {
        Fixed8,
        Fixed16,
        Fixed32,
        Fixed64,
        Fixed128,
        CompressedInt,
        CountedArrayUInt32Fixed8,  // UInt32 is the leading count, followed by that many bytes
        CountedArrayUInt32Fixed16, // UInt32 is the leading count, followed by that many words
        Complex                    // Described by a series of parse instructions in the corresponding layout 
    }

    internal enum WellKnownParseRule
    {
        FixedUInt8 = 1,
        FixedInt16 = 2,
        FixedInt32 = 3,
        FixedInt64 = 4,
        Guid = 5
    }

    internal class ParseRule
    {
        public static ParseRule Guid = new ParseRule((int)WellKnownParseRule.Guid, SerializationSize.Fixed128, RecordType.Guid);
        public static ParseRule FixedUInt8 = new ParseRule((int)WellKnownParseRule.FixedUInt8, SerializationSize.Fixed8, RecordType.Byte);
        public static ParseRule FixedInt16 = new ParseRule((int)WellKnownParseRule.FixedInt16, SerializationSize.Fixed16, RecordType.Int16);
        public static ParseRule FixedInt32 = new ParseRule((int)WellKnownParseRule.FixedInt32, SerializationSize.Fixed32, RecordType.Int32);
        public static ParseRule FixedInt64 = new ParseRule((int)WellKnownParseRule.FixedInt64, SerializationSize.Fixed64, RecordType.Int64);


        public int Id;
        public SerializationSize Size;
        public RecordType ParsedType;
        public ParseInstruction[] Instructions;

        public ParseRule(int id, SerializationSize size, RecordType parsedType)
        {
            Id = id;
            Size = size;
            ParsedType = parsedType;
        }

        public Expression CreateParseExpression(Expression streamReader)
        {
            switch(Id)
            {
                case (int)WellKnownParseRule.FixedUInt8:
                    MethodInfo readByte = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readByte);
                case (int)WellKnownParseRule.FixedInt16:
                    MethodInfo readInt16 = typeof(IStreamReader).GetMethod("ReadInt16", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt16);
                case (int)WellKnownParseRule.FixedInt32:
                    MethodInfo readInt32 = typeof(IStreamReader).GetMethod("ReadInt32", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt32);
                case (int)WellKnownParseRule.FixedInt64:
                    MethodInfo readInt64 = typeof(IStreamReader).GetMethod("ReadInt64", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(streamReader, readInt64);
                case (int)WellKnownParseRule.Guid:
                    MethodInfo readGuid = typeof(IStreamWriterExentions).GetMethod("ReadGuid", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readGuid, streamReader);
                default:
                    throw new SerializationException("Parse rule id " + Id + " not recognized");
            }
        }
    }

    internal class Record
    {
        public RecordType RecordType;
        public object[] WeaklyTypedFields;

        public Record(RecordType recordType)
        {
            RecordType = recordType;
            RecordType.GetInitWeakFieldStorageDelegate()(this);
        }

        public T GetFieldValue<T>(params RecordField[] fieldPath)
        {
            return ((Func<Record, T>)RecordType.GetFieldReadDelegate(fieldPath))(this);
        }
    }

    internal class Record<T> : Record
    {
        public Record(RecordType recordType) : base(recordType) { }
        public T StronglyTypedFields;
    }

    internal class RecordParser<T>
    {
        
    }
}
