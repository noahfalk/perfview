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
    internal class DynamicFieldInfo
    {
        public Type CanonFieldSlotType;
        public int TypeIndex;
        public int FieldCount;
    }

    internal class RecordType : Record
    {
        public static RecordType Object = new RecordType(1, "Object", typeof(object));
        // Well-known primitive types
        public static RecordType Boolean = new RecordType(2, "Boolean", typeof(bool));
        public static RecordType UInt8 = new RecordType(3, "UInt8", typeof(byte));
        public static RecordType Int16 = new RecordType(4, "Int16", typeof(short));
        public static RecordType Int32 = new RecordType(5, "Int32", typeof(int));
        public static RecordType Int64 = new RecordType(6, "Int64", typeof(long));
        public static RecordType String = new RecordType(7, "String", typeof(string));
        public static RecordType Guid = new RecordType(8, "Guid", typeof(Guid));
        // Well-known record types
        public static RecordType Type;
        public static RecordType Field;
        public static RecordType Table;
        public static RecordType ParseRule;
        public static RecordType ParseRuleLocalVars;
        public static RecordType RecordBlock;
        public static RecordType ParseInstruction;
        public static RecordType ParseInstructionArray;

        public static RecordType[] WellKnownTypes;

        static RecordType()
        {
            Type = new RecordType(100, "Type", typeof(RecordType));
            Field = new RecordType(101, "Field", typeof(RecordField));
            Table = new RecordType(102, "Table", typeof(RecordTable));
            ParseRule = new RecordType(103, "ParseRule", typeof(ParseRule));
            ParseRuleLocalVars = new RecordType(104, "ParseRuleLocalVars", typeof(ParseRuleLocalVars));
            RecordBlock = new RecordType(105, "RecordBlock", typeof(RecordBlock));
            ParseInstruction = new RecordType(106, "ParseInstruction", typeof(ParseInstruction));
            Type.AddField(new RecordField(1, "Name", Type, String));
            Type.AddField(new RecordField(2, "Id", Type, Int32));
            Type.FinishInit();
            Field.AddField(new RecordField(3, "Name", Field, String));
            Field.AddField(new RecordField(4, "Id", Field, Int32));
            Field.AddField(new RecordField(5, "ContainingType", Field, Type));
            Field.AddField(new RecordField(6, "FieldType", Field, Type));
            Field.FinishInit();
            Table.AddField(new RecordField(10, "Name", Table, String));
            Table.AddField(new RecordField(11, "Id", Table, Int32));
            Table.AddField(new RecordField(12, "ItemType", Table, Type));
            Table.AddField(new RecordField(13, "PrimaryKeyField", Table, Field));
            Table.FinishInit();
            ParseRule.AddField(new RecordField(14, "Id", ParseRule, Int32));
            ParseRule.AddField(new RecordField(15, "ParsedType", ParseRule, Type));
            //ParseInstruction[] Instructions
            ParseRule.FinishInit();
            ParseRuleLocalVars.AddField(new RecordField(16, "TempInt32", ParseRuleLocalVars, Int32));
            ParseRuleLocalVars.AddField(new RecordField(99000, "TempParseRule", ParseRuleLocalVars, ParseRule));
            ParseRuleLocalVars.AddField(new RecordField(99001, "This", ParseRuleLocalVars, null));
            ParseRuleLocalVars.AddField(new RecordField(99002, "Null", ParseRuleLocalVars, null));
            ParseRuleLocalVars.AddField(new RecordField(17, "CurrentOffset", ParseRuleLocalVars, Int32));
            ParseRuleLocalVars.FinishInit();
            RecordBlock.AddField(new RecordField(18, "EntryCount", RecordBlock, Int32));
            RecordBlock.FinishInit();
            ParseInstruction.AddField(new RecordField(19, "InstructionType", ParseInstruction, UInt8));
            ParseInstruction.AddField(new RecordField(20, "DestinationField", ParseInstruction, Field));
            ParseInstruction.AddField(new RecordField(21, "Constant", ParseInstruction, Object));
            ParseInstruction.AddField(new RecordField(8899, "ConstantType", ParseInstruction, Type));
            ParseInstruction.AddField(new RecordField(22, "ParseRule", ParseInstruction, ParseRule));
            ParseInstruction.AddField(new RecordField(8887, "ParsedType", ParseInstruction, Type));
            ParseInstruction.AddField(new RecordField(8888, "ParseRuleField", ParseInstruction, Field));
            ParseInstruction.AddField(new RecordField(23, "CountField", ParseInstruction, Field));
            ParseInstruction.AddField(new RecordField(24, "SourceField", ParseInstruction, Field));
            ParseInstruction.AddField(new RecordField(25, "LookupTable", ParseInstruction, Table));
            ParseInstruction.AddField(new RecordField(26, "PublishStream", ParseInstruction, Table));
            ParseInstruction.FinishInit();

            WellKnownTypes = new RecordType[] { Object, Boolean, UInt8, Int16, Int32, Int64, Guid, String,
                Type, Field, Table, ParseRule, ParseRuleLocalVars, RecordBlock, ParseInstruction};
        }

        public int Id;
        public string Name;
        public RecordType ArrayElement;
        public Type ReflectionType;
        public int CountWeakFieldTypes;
        public Dictionary<int, DynamicFieldInfo> WeakFieldTypesByIndex = new Dictionary<int, DynamicFieldInfo>();
        Dictionary<Type, DynamicFieldInfo> _weakFieldTypes = new Dictionary<Type, DynamicFieldInfo>();
        List<RecordField> _fields = new List<RecordField>();
        Delegate _initRecord;   // Action<T> where T = ReflectionType
        Delegate _createRecord; // Func<T> where T = ReflectionType

        public RecordType() { }

        public RecordType(int id, string name, RecordType arrayElement)
        {
            Id = id;
            Name = name;
            ArrayElement = arrayElement;
            ReflectionType = ArrayElement.ReflectionType.MakeArrayType();
        }

        public RecordType(int id, string name, Type accessType)
        {
            Id = id;
            Name = name;
            ReflectionType = accessType;
        }

        public void FinishInit()
        {
            if(ReflectionType == null)
            {
                if(ArrayElement != null)
                {
                    ReflectionType = ArrayElement.ReflectionType.MakeArrayType();
                }
                else
                {
                    ReflectionType = typeof(Record);
                }
            }
            if (typeof(Record).IsAssignableFrom(ReflectionType))
            {
                _initRecord = RecordParserCodeGen.GetInitRecordDelegate(this);
                _createRecord = RecordParserCodeGen.GetCreateInstanceDelegate(this);
            }
        }

        public Delegate GetCreateInstanceDelegate()   { FinishInit(); return _createRecord; }
        public Delegate GetInitInstanceDelegate()     { FinishInit(); return _initRecord; }
        public RecordField[] GetFields()              { return _fields.ToArray(); }
        public void InitInstance<T>(T record)         { ((Action<T>)GetInitInstanceDelegate())(record); }
        public T CreateInstance<T>()                  { return ((Func<T>)GetCreateInstanceDelegate())(); }
        public RecordField GetField(string fieldName) { return _fields.Where(f => f.Name == fieldName).FirstOrDefault(); }

        public void AddField(RecordField field)
        {
            if (field.FieldType != null && RecordParserCodeGen.GetStrongBackingField(field) == null)
            {
                Type fieldSlotType = RecordParserCodeGen.GetCanonFieldSlotType(field);
                DynamicFieldInfo typeFieldInfo;
                if (!_weakFieldTypes.TryGetValue(fieldSlotType, out typeFieldInfo))
                {
                    typeFieldInfo = new DynamicFieldInfo();
                    typeFieldInfo.TypeIndex = CountWeakFieldTypes++;
                    typeFieldInfo.CanonFieldSlotType = fieldSlotType;
                    _weakFieldTypes[fieldSlotType] = typeFieldInfo;
                    WeakFieldTypesByIndex[typeFieldInfo.TypeIndex] = typeFieldInfo;
                }
                field.DynamicFieldTypeIndex = typeFieldInfo.TypeIndex;
                field.DynamicFieldIndex = typeFieldInfo.FieldCount++;
            }
            _fields.Add(field);
            _initRecord = null;
            _createRecord = null;
        }

        public override string ToString()
        {
            return Name + "(" + Id + ")";
        }
    }

    internal class RecordField : Record
    {
        public int Id;
        public string Name;
        public RecordType ContainingType;
        public RecordType FieldType;
        public bool RequiresStorage;

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
        StoreReadLookup,
        StoreField,
        StoreFieldLookup,
        //AddConstant,
        //AddRead,
        //AddField,
        Publish,
        IterateRead
    }

    delegate void InstructionAction<T>(IStreamReader reader, ParseRuleLocalVars locals, ref T record);

    internal class ParseInstruction : Record
    {
        public ParseInstructionType InstructionType { get; protected set; }
        public RecordField DestinationField { get; protected set; }
        public object Constant { get; protected set; }
        public RecordType ConstantType { get { return ParsedType; } protected set { ParsedType = value; } }
        public ParseRule ParseRule { get; protected set; }
        public RecordType ParsedType { get; protected set; }
        public RecordField ParseRuleField { get; protected set; }
        public RecordField CountField { get; protected set; }
        public RecordField SourceField { get; protected set; }
        public RecordTable LookupTable { get; protected set; }
        public RecordTable PublishStream { get; protected set; }
        public RecordType ThisType { get; set; }

        private Delegate _cachedExecuteAction; // InstructionAction<T> where T = ThisType.ReflectionType

        public void Execute<T>(IStreamReader streamReader, ParseRuleLocalVars locals, ref T record)
        {
            if(_cachedExecuteAction == null)
            {
                _cachedExecuteAction = RecordParserCodeGen.GetInstructionExecuteDelegate(this);
            }
            ((InstructionAction<T>)_cachedExecuteAction)(streamReader, locals, ref record);
        }
    }

    internal abstract class ParseInstructionStore : ParseInstruction
    {
        public ParseInstructionStore(RecordType thisType, RecordField destinationField)
        {
            Debug.Assert(destinationField != null);
            ThisType = thisType;
            DestinationField = destinationField;
        }
    }

    internal class ParseInstructionStoreConstant : ParseInstructionStore
    {
        public ParseInstructionStoreConstant(RecordType thisType, RecordField destinationField, RecordType constantType, object constant) : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreConstant;
            ConstantType = constantType;
            Constant = constant;
        }
    }

    internal class ParseInstructionStoreRead : ParseInstructionStore
    {
        public ParseInstructionStoreRead(RecordType thisType, RecordField destinationField, ParseRule rule,
            RecordField countField = null) : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreRead;
            ParseRule = rule;
            CountField = countField;
            Debug.Assert(ParseRule != null);
        }

        public ParseInstructionStoreRead(RecordType thisType, RecordField destinationField, RecordType parsedType,
            RecordField parseRuleField = null, RecordField countField = null) : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreRead;
            ParsedType = parsedType;
            ParseRuleField = parseRuleField;
            CountField = countField;
            Debug.Assert(ParseRuleField != null);
        }
    }

    internal class ParseInstructionStoreReadLookup : ParseInstructionStore
    {
        public ParseInstructionStoreReadLookup(RecordType thisType, RecordField destinationField, ParseRule rule, RecordTable lookupTable)
            : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreReadLookup;
            ParseRule = rule;
            LookupTable = lookupTable;
        }
    }

    internal class ParseInstructionStoreField : ParseInstructionStore
    {
        public ParseInstructionStoreField(RecordType thisType, RecordField destinationField, RecordField sourceField ) : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreField;
            SourceField = sourceField;
        }
    }

    internal class ParseInstructionStoreFieldLookup : ParseInstructionStore
    {
        public ParseInstructionStoreFieldLookup(RecordType thisType, RecordField destinationField, RecordField sourceField, RecordTable lookupTable)
            : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreFieldLookup;
            SourceField = sourceField;
            LookupTable = lookupTable;
        }
    }

    internal class ParseInstructionPublish : ParseInstruction
    {
        public ParseInstructionPublish(RecordType thisType, RecordTable publishStream)
        {
            ThisType = thisType;
            InstructionType = ParseInstructionType.Publish;
            PublishStream = publishStream;
        }
    }

    internal class ParseInstructionIterateRead : ParseInstruction
    {
        public ParseInstructionIterateRead(RecordType thisType, ParseRule parseRule, RecordField countField)
        {
            ThisType = thisType;
            InstructionType = ParseInstructionType.IterateRead;
            ParseRule = parseRule;
            CountField = countField;
        }
    }

    internal static class RecordParserCodeGen
    {
        public static Delegate GetCreateInstanceDelegate(RecordType recordType)
        {
            Expression createExpression = Expression.New(recordType.ReflectionType);
            ParameterExpression recordVar = Expression.Variable(recordType.ReflectionType);
            List<Expression> statements = new List<Expression>();
            statements.Add(Expression.Assign(recordVar, createExpression));
            statements.AddRange(GetInitRecordStatements(recordVar, recordType));
            statements.Add(recordVar);
            Type delegateType = typeof(Func<>).MakeGenericType(recordType.ReflectionType);
            return Expression.Lambda(delegateType, Expression.Block(new ParameterExpression[] { recordVar }, statements)).Compile();
        }

        public static Delegate GetInitRecordDelegate(RecordType recordType)
        {
            Expression block = Expression.Block(GetInitRecordStatements(RecordParameter, recordType));
            Type delegateType = typeof(Action<>).MakeGenericType(recordType.ReflectionType);
            return Expression.Lambda(delegateType, block, RecordParameter).Compile();
        }

        public static IEnumerable<Expression> GetInitRecordStatements(ParameterExpression record, RecordType recordType)
        {
            if (typeof(Record).IsAssignableFrom(recordType.ReflectionType))
            {
                yield return Expression.Assign(Expression.Field(record, RecordRecordTypeField), Expression.Constant(recordType));
                if (recordType.CountWeakFieldTypes != 0)
                {
                    Expression dynamicFieldsField = Expression.Field(record, RecordDynamicFieldsField);
                    List<Expression> typedArrays = new List<Expression>();
                    for (int i = 0; i < recordType.CountWeakFieldTypes; i++)
                    {
                        DynamicFieldInfo weakFieldTypeInfo = recordType.WeakFieldTypesByIndex[i];
                        Type slotType = weakFieldTypeInfo.CanonFieldSlotType;
                        typedArrays.Add(Expression.NewArrayBounds(slotType, Expression.Constant(weakFieldTypeInfo.FieldCount)));
                    }
                    Expression weaklyTypeFields = Expression.NewArrayInit(typeof(object), typedArrays);
                    yield return Expression.Assign(dynamicFieldsField, weaklyTypeFields);
                }
            }
        }

        public static InstructionAction<T> GetStoreFieldDelegate<T>(ParameterExpression localsParam, RecordType recordType, ParameterExpression recordParam, RecordField field, RecordType fieldValType, Expression fieldVal)
        {
            RecordType fieldRootType = recordType;
            Expression fieldRoot = recordParam;
            //TODO: what if the record is also an instance of LocalVarsType?
            if(field.ContainingType.ReflectionType == localsParam.Type)
            {
                //TODO: get rid of this static access
                fieldRootType = RecordType.ParseRuleLocalVars;
                fieldRoot = localsParam;
            }
            else if (field.ContainingType.ReflectionType != recordParam.Type)
            {
                fieldRootType = field.ContainingType;
                fieldRoot = Expression.Convert(recordParam, field.ContainingType.ReflectionType);
            }
            Expression body = GetStoreFieldExpression(fieldRootType, recordType, fieldRoot, recordParam, field, fieldValType, fieldVal);
            return Expression.Lambda<InstructionAction<T>>(body, StreamReaderParameter, localsParam, recordParam).Compile();
        }

        public static Func<T,U> GetRecordFieldDelegate<T,U>(RecordType recordType, RecordField field)
        {
            Debug.Assert(typeof(T) == recordType.ReflectionType);
            Debug.Assert(typeof(U) == field.FieldType.ReflectionType);
            ParameterExpression record = Expression.Parameter(typeof(T), "record");
            Expression body = GetFieldReadExpression(recordType, null, record, field);
            if(body.Type != typeof(U))
            {
                body = Expression.Convert(body, typeof(U));
            }
            return Expression.Lambda<Func<T,U>>(body, record).Compile();
        }

        public static Expression GetPublishExpression(Expression record, RecordTable stream)
        {
            Expression streamItem = record;
            if (record.Type != stream.ItemType.ReflectionType)
            {
                streamItem = Expression.Convert(record, stream.ItemType.ReflectionType);
            }
            MethodInfo addMethod = stream.GetType().GetMethod("Add");
            Expression addResult = Expression.Call(Expression.Constant(stream), addMethod, streamItem);
            if(record.Type != addResult.Type)
            {
                addResult = Expression.Convert(addResult, record.Type);
            }
            return Expression.Block(Expression.Assign(record, addResult), record);
        }

        public static Expression GetIterateReadExpression(RecordType thisType, Expression record,  ParseRule rule, RecordField countField)
        {
            Expression count = GetFieldReadExpression(thisType, LocalVarsParameter, record, countField);
            if(countField.FieldType.ReflectionType != typeof(int))
            {
                count = Expression.ConvertChecked(count, typeof(int));
            }
            ParameterExpression countVar = Expression.Variable(typeof(int), "count");
            Expression initCountVar = Expression.Assign(countVar, count);
            ParameterExpression indexVar = Expression.Variable(typeof(int), "index");
            Expression initIndexVar = Expression.Assign(indexVar, Expression.Constant(0));
            LabelTarget exitLoop = Expression.Label();
            Expression initRecordVal = Expression.Constant(rule.ParsedType.CreateInstance<Record>());
            ParameterExpression recordVar = Expression.Variable(rule.ParsedType.ReflectionType);
            Expression initRecordVar = Expression.Assign(recordVar, initRecordVal);
            Expression loop = Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(indexVar, countVar),
                    Expression.Block(
                        Expression.Assign(recordVar, GetParseExpression(rule, recordVar)),
                        Expression.Assign(indexVar, Expression.Increment(indexVar))),
                    Expression.Break(exitLoop)),
                exitLoop);
            return Expression.Block(new ParameterExpression[] { countVar, indexVar, recordVar },
                initCountVar, initIndexVar, initRecordVar, loop);
        }

        public static Delegate GetInstructionExecuteDelegate(ParseInstruction instruction)
        {
            Type thisType = instruction.ThisType.ReflectionType;
            Type delegateType = typeof(InstructionAction<>).MakeGenericType(thisType);
            ParameterExpression recordParam = Expression.Parameter(thisType.MakeByRefType(), "record");
            Expression body = GetInstructionExecuteExpression(recordParam, instruction);
            return Expression.Lambda(delegateType, body, StreamReaderParameter, LocalVarsParameter, recordParam).Compile();
        }

        static Expression GetInstructionExecuteExpression(Expression recordParam, ParseInstruction instruction)
        {
            switch (instruction.InstructionType)
            {
                case ParseInstructionType.StoreConstant:
                    return GetInstructionStoreExpression(recordParam, instruction, instruction.ConstantType, Expression.Constant(instruction.Constant));
                case ParseInstructionType.StoreRead:
                    RecordType parsedType = instruction.ParsedType ?? instruction.ParseRule.ParsedType;

                    //TODO: the previous field value may not be convertible to the current parsed type
                    //consider array of long, parsed using dynamic parse rules, where some parse rules only handle int
                    //replacing records with different sub-types during publish could have similar effect - nah that is OK, parsedType would refer to the base type that was
                    //originally parsed which would match
                    //
                    // If this conversion isn't a no-op we didn't want to provide the previous val
                    Expression previousFieldVal = Expression.Default(parsedType.ReflectionType);
                    RecordType thisType = instruction.ThisType;
                    RecordType resolvedFieldType = ResolveFieldType(thisType, instruction.DestinationField);
                    if (resolvedFieldType == parsedType)
                    {
                        previousFieldVal = GetFieldReadExpression(thisType, LocalVarsParameter, recordParam, instruction.DestinationField);
                    }
                    else
                    {
                        Debug.Assert(!typeof(Record).IsAssignableFrom(parsedType.ReflectionType));
                        Debug.Assert(!parsedType.ReflectionType.IsArray);
                    }
                    
                    if (instruction.ParseRuleField != null)
                    {
                        return GetInstructionStoreExpression(recordParam, instruction, instruction.ParsedType,
                            GetParseExpression(instruction.ThisType, recordParam, instruction.ParsedType, instruction.ParseRuleField, previousFieldVal));
                    }
                    else
                    {
                        return GetInstructionStoreExpression(recordParam, instruction, instruction.ParseRule.ParsedType,
                            // not a no-op for example parse int and store into long field
                            GetParseExpression(instruction.ParseRule, previousFieldVal));
                    }
                case ParseInstructionType.StoreReadLookup:
                    return GetInstructionStoreExpression(recordParam, instruction, instruction.LookupTable.ItemType,
                        GetReadLookupExpression(instruction.ParseRule, instruction.LookupTable));
                case ParseInstructionType.StoreField:
                    // not a no-op
                    return GetInstructionStoreExpression(recordParam, instruction, instruction.SourceField.FieldType, GetFieldReadExpression(
                        instruction.ThisType, LocalVarsParameter, recordParam, instruction.SourceField));
                case ParseInstructionType.StoreFieldLookup:
                    return GetInstructionStoreExpression(recordParam, instruction, instruction.LookupTable.ItemType, 
                        GetFieldReadLookupExpression(instruction.ThisType, recordParam, instruction.SourceField, instruction.LookupTable));
                case ParseInstructionType.Publish:
                    return GetPublishExpression(recordParam, instruction.PublishStream);
                case ParseInstructionType.IterateRead:
                    return GetIterateReadExpression(instruction.ThisType, recordParam, instruction.ParseRule, instruction.CountField);
                default:
                    throw new SerializationException("Invalid ParseInstructionType");
            }
        }

        static Expression GetInstructionStoreExpression(Expression recordParam, ParseInstruction instruction, RecordType storeValType, Expression storeVal)
        {
            RecordType fieldRootType = instruction.ThisType;
            Expression fieldRoot = recordParam;
            if (instruction.DestinationField.ContainingType.ReflectionType == LocalVarsParameter.Type)
            {
                //TODO: don't hardcode static type
                fieldRootType = RecordType.ParseRuleLocalVars;
                fieldRoot = LocalVarsParameter;
            }
            return GetStoreFieldExpression(fieldRootType, instruction.ThisType, fieldRoot, recordParam, instruction.DestinationField, storeValType, storeVal);
        }

        public static Expression GetFieldReadExpression(RecordType thisType, Expression localVars, Expression thisObj, RecordField field)
        {
            RecordType fieldRootType = field.ContainingType;
            Expression fieldRoot = thisObj;
            if(localVars != null && fieldRootType.ReflectionType == localVars.Type)
            {
                //TODO: don't hardcode the static
                fieldRootType = RecordType.ParseRuleLocalVars;
                fieldRoot = localVars;
            }
            else if (fieldRootType.ReflectionType != thisObj.Type)
            {
                fieldRoot = Expression.Convert(thisObj, fieldRootType.ReflectionType);
            }
            Expression fieldRead = GetFieldExpression(thisType, fieldRoot, thisObj, field);
            if(fieldRead == null)
            {
                throw new SerializationException("Unable to read from field " + field.ToString());
            }
            MethodInfo debug = typeof(RecordParserCodeGen).GetMethod("DebugFieldRead", BindingFlags.Static | BindingFlags.Public);
            fieldRead = Expression.Block(
                Expression.Call(debug, Expression.Convert(fieldRoot,typeof(object)), Expression.Constant(field), Expression.Convert(fieldRead, typeof(object))),
                fieldRead);
            return fieldRead;
        }

        public static void DebugFieldRead(object fieldContainer, RecordField field, object val)
        {
            Debug.WriteLine("Read: " + fieldContainer.ToString() + " . " + field.ToString() + " = " + (val == null ? "(null)" : val.ToString()));
        }

        public static Expression GetFieldReadLookupExpression(RecordType thisType, Expression record, RecordField field, RecordTable table)
        {
            Expression fieldRead = GetFieldReadExpression(thisType, LocalVarsParameter, record, field);
            return GetLookupExpression(table, field.FieldType, fieldRead);
        }

        public static Expression GetReadLookupExpression(ParseRule rule, RecordTable table)
        {
            Expression fieldRead = GetParseExpression(rule, Expression.Default(rule.ParsedType.ReflectionType));
            return GetLookupExpression(table, rule.ParsedType, fieldRead);
        }

        private static Expression GetLookupExpression(RecordTable table, RecordType lookupKeyType, Expression lookupKey)
        {
            Type tableType = typeof(RecordTable<>).MakeGenericType(table.ItemType.ReflectionType);
            Expression tableExpression = Expression.Constant(table, tableType);
            MethodInfo lookupMethod = tableType.GetMethod("Get");
            if(lookupKeyType.ReflectionType != typeof(int))
            {
                lookupKey = Expression.Convert(lookupKey, typeof(int));
            }
            Expression lookupResult = Expression.Call(tableExpression, lookupMethod, lookupKey);
            return lookupResult;
        }

        public static Expression GetParseExpression(ParseRule parseRule, Expression initialRecord)
        {
            return GetParseExpression(Expression.Constant(parseRule), parseRule.ParsedType, initialRecord);
        }

        public static Expression GetParseExpression(RecordType thisType, Expression recordParam, RecordType parsedType, RecordField parseRuleField, Expression initialRecord)
        {
            Expression parseRule = GetFieldReadExpression(thisType, LocalVarsParameter, recordParam, parseRuleField);
            return GetParseExpression(parseRule, parsedType, initialRecord);
        }

        public static Expression GetParseExpression(Expression parseRule, RecordType parsedType, Expression initialRecord)
        {
            //TODO: improve error handling when dynamicly loaded parseRule doesn't parse parsedType
            MethodInfo parseMethod = typeof(ParseRule).GetMethod("Parse").MakeGenericMethod(parsedType.ReflectionType);
            if(initialRecord.Type != parsedType.ReflectionType)
            {
                //TODO: reading 'this' as initialRecord from a location typed object means we have to do casting.
                initialRecord = Expression.Convert(initialRecord, parsedType.ReflectionType);
            }
            Expression result = Expression.Call(parseRule, parseMethod, StreamReaderParameter, initialRecord);
            if (parseRule.NodeType == ExpressionType.Constant)
            {
                result = GetBuiltinParseRule((ParseRule)((ConstantExpression)parseRule).Value) ?? result;
            }
            return result;
        }

        public static Expression GetBuiltinParseRule(ParseRule parseRule)
        {
            switch (parseRule.Id)
            {
                case (int)PrimitiveParseRuleId.Boolean:
                    MethodInfo readByte = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.NotEqual(Expression.Call(StreamReaderParameter, readByte), Expression.Constant((byte)0));
                case (int)PrimitiveParseRuleId.FixedUInt8:
                    MethodInfo readByte2 = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readByte2);
                case (int)PrimitiveParseRuleId.FixedInt16:
                    MethodInfo readInt16 = typeof(IStreamReader).GetMethod("ReadInt16", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt16);
                case (int)PrimitiveParseRuleId.FixedInt32:
                    MethodInfo readInt32 = typeof(IStreamReader).GetMethod("ReadInt32", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt32);
                case (int)PrimitiveParseRuleId.FixedInt64:
                    MethodInfo readInt64 = typeof(IStreamReader).GetMethod("ReadInt64", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt64);
                case (int)PrimitiveParseRuleId.Guid:
                    MethodInfo readGuid = typeof(IStreamWriterExentions).GetMethod("ReadGuid", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readGuid, StreamReaderParameter);
                case (int)PrimitiveParseRuleId.UTF8String:
                    MethodInfo readUtf8 = typeof(ParseFunctions).GetMethod("ReadUTF8String", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readUtf8, StreamReaderParameter);
                default:
                    return null;
            }
        }

        public static MemberInfo GetStrongBackingField(RecordField recordField)
        {
            Debug.Assert(recordField.FieldType != null);
            //TODO: restrict access with attribute
            PropertyInfo reflectionProp = GetStrongBackingType(recordField.ContainingType)?.GetProperty(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            //TODO: stronger typed checking?
            if (reflectionProp != null)
            {
                return reflectionProp;
            }
            FieldInfo reflectionField = GetStrongBackingType(recordField.ContainingType)?.GetField(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            FieldInfo baseReflectionField = typeof(Record).GetField(recordField.Name, BindingFlags.Public | BindingFlags.Instance);
            if (reflectionField == null ||
                baseReflectionField != null ||
                reflectionField.FieldType != recordField.FieldType.ReflectionType)
            {
                return null;
            }

            return reflectionField;
        }

        public static Type GetCanonFieldSlotType(RecordField field)
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

        public static Expression GetStoreFieldExpression(RecordType fieldContainerType, RecordType thisType, Expression fieldObj, Expression thisObj, RecordField field, RecordType fieldValType, Expression fieldVal)
        {
            Expression fieldRef = GetBackingFieldExpression(fieldContainerType, thisType, fieldObj, thisObj, 0, new RecordField[] { field });
            if(fieldRef == null)
            {
                return fieldVal;
            }
            if(fieldRef.Type != fieldVal.Type)
            {
                fieldVal = Expression.ConvertChecked(fieldVal, fieldRef.Type);
            }
            var body = Expression.Assign(fieldRef, fieldVal);
            return body;
        }

        public static Expression GetFieldExpression(RecordType thisType, Expression fieldRootObj, Expression thisObj, params RecordField[] fieldPath)
        {
            return GetBackingFieldExpression(fieldPath[0].ContainingType, thisType, fieldRootObj, thisObj,  0, fieldPath);
        }

        static Expression GetBackingFieldExpression(RecordType fieldContainerType, RecordType thisType, Expression fieldContainerObj, Expression thisObj, int level, RecordField[] fieldPath)
        {
            RecordField derefField = fieldPath[level];
            Debug.Assert(typeof(Record).IsAssignableFrom(fieldContainerObj.Type));
            Debug.Assert(fieldContainerType.ReflectionType == fieldContainerObj.Type);
            RecordType resolvedFieldType = ResolveFieldType(thisType, derefField);
            Expression fieldValExpr = null;
            if (IsThisField(derefField))
            {
                fieldValExpr = thisObj;
            }
            else if(IsNullField(derefField))
            {
                fieldValExpr = null;
                if (level != fieldPath.Length)
                {
                    throw new SerializationException("Unable to dereference through field " + derefField.ToString());
                }
            }
            else
            {
                MemberInfo strongBackingField = GetStrongBackingField(derefField);
                if (strongBackingField != null)
                {
                    if (strongBackingField.MemberType == MemberTypes.Field)
                    {
                        fieldValExpr = Expression.Field(fieldContainerObj, (FieldInfo)strongBackingField);
                    }
                    else
                    {
                        fieldValExpr = Expression.Property(fieldContainerObj, (PropertyInfo)strongBackingField);
                    }
                }
                else
                {
                    Expression dynamicFieldsArray = Expression.Field(fieldContainerObj, RecordDynamicFieldsField);
                    Expression dynamicFieldsTypedSlotArray = Expression.ArrayAccess(dynamicFieldsArray,
                                                                     Expression.Constant(derefField.DynamicFieldTypeIndex));
                    Type fieldSlotArrayType = GetCanonFieldSlotType(derefField).MakeArrayType();
                    fieldValExpr = Expression.ArrayAccess(Expression.Convert(dynamicFieldsTypedSlotArray, fieldSlotArrayType),
                                                  Expression.Constant(derefField.DynamicFieldIndex));
                }
            }

            if (level == fieldPath.Length - 1)
            {
                return fieldValExpr;
            }
            else
            {
                if(fieldValExpr.Type != resolvedFieldType.ReflectionType)
                {
                    fieldValExpr = Expression.Convert(fieldValExpr, resolvedFieldType.ReflectionType);
                }
                return GetBackingFieldExpression(resolvedFieldType, thisType, thisObj, fieldValExpr, level + 1, fieldPath);
            }
        }

        static bool IsThisField(RecordField field) => field.ContainingType.ReflectionType == typeof(ParseRuleLocalVars) && field.Name == "This";
        static bool IsNullField(RecordField field) => field.ContainingType.ReflectionType == typeof(ParseRuleLocalVars) && field.Name == "Null";
        static RecordType ResolveFieldType(RecordType thisType, RecordField field) => IsThisField(field) ? thisType : field.FieldType;

        static Type GetStrongBackingType(RecordType recordType)
        {
            Type reflectionType = recordType.ReflectionType;
            return (typeof(Record) != reflectionType && typeof(Record).IsAssignableFrom(reflectionType)) ? reflectionType : null;
        }

        public static ParameterExpression StreamReaderParameter = Expression.Parameter(typeof(IStreamReader), "streamReader");
        public static ParameterExpression LocalVarsParameter = Expression.Parameter(typeof(ParseRuleLocalVars), "localVars");
        public static ParameterExpression RecordParameter = Expression.Parameter(typeof(Record), "record_cg");
        public static ParameterExpression RecordRefParameter = Expression.Parameter(typeof(Record).MakeByRefType(), "record_ref_cg");
        public static FieldInfo RecordDynamicFieldsField = typeof(Record).GetField("DynamicFields", BindingFlags.Public | BindingFlags.Instance);
        public static FieldInfo RecordRecordTypeField = typeof(Record).GetField("_recordType", BindingFlags.NonPublic | BindingFlags.Instance);
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

    internal class ParseRule : Record
    {
        public static ParseRule Boolean = new ParseRule((int)PrimitiveParseRuleId.Boolean, "Builtin.Boolean", RecordType.Boolean);
        public static ParseRule FixedUInt8 = new ParseRule((int)PrimitiveParseRuleId.FixedUInt8, "Builtin.FixedUInt8", RecordType.UInt8);
        public static ParseRule FixedInt16 = new ParseRule((int)PrimitiveParseRuleId.FixedInt16, "Builtin.FixedInt16", RecordType.Int16);
        public static ParseRule FixedInt32 = new ParseRule((int)PrimitiveParseRuleId.FixedInt32, "Builtin.FixedInt32", RecordType.Int32);
        public static ParseRule FixedInt64 = new ParseRule((int)PrimitiveParseRuleId.FixedInt64, "Builtin.FixedInt64", RecordType.Int64);
        public static ParseRule UTF8String = new ParseRule((int)PrimitiveParseRuleId.UTF8String, "Builtin.UTF8String", RecordType.String);
        public static ParseRule Guid = new ParseRule((int)PrimitiveParseRuleId.Guid, "Builtin.Guid", RecordType.Guid);

        public int Id;
        public string Name;
        public RecordType ParsedType;
        public ParseInstruction[] Instructions;

        //Delegate _cachedParseFunc; //Func<IStreamReader, T, T> where T = ParsedType.ReflectionType

        public ParseRule() { }
        public ParseRule(int id, string name, RecordType parsedType, params ParseInstruction[] instructions)
        {
            Id = id;
            Name = name;
            ParsedType = parsedType;
            Instructions = instructions;
            //TODO: validate instructions
            //_cachedParseFunc = RecordParserCodeGen.GetParseDelegate(this);
            OnParseComplete();
        }
        public void OnParseComplete()
        {
            foreach(ParseInstruction instr in Instructions)
            {
                instr.ThisType = ParsedType;
            }
        }

        public T Parse<T>(IStreamReader reader, T record)
        {
            Debug.WriteLine("Reading " + Name.ToString() + " at: " + reader.Current);
            if(ParsedType.ReflectionType != typeof(T))
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
            ParsedType.InitInstance(record);
            ParseRuleLocalVars vars = RecordType.ParseRuleLocalVars.CreateInstance<ParseRuleLocalVars>();
            vars.InitReader(reader);
            //return ((Func<IStreamReader,T,T>)_cachedParseFunc)(reader, record);
            
            foreach (ParseInstruction instruction in Instructions)
            {
                instruction.Execute(reader, vars, ref record);
            }
            return record;
        }
    }

    internal class ParseRuleLocalVars : Record
    {
        IStreamReader _reader;
        StreamLabel _positionStart;
        public void InitReader(IStreamReader reader)
        {
            _reader = reader;
            _positionStart = _reader.Current;
        }

        public int TempInt32;
        public ParseRule TempParseRule;
        public object This;
        public int CurrentOffset
        {
            get { return _reader.Current.Sub(_positionStart); }
            set
            {
                int cur = CurrentOffset;
                if (value < cur)
                {
                    throw new InvalidOperationException("CurrentOffset can not be decreased");
                }
                _reader.Skip(value - cur);
            }
        }

    }

    internal class Record
    {
        public object[] DynamicFields;
        private RecordType _recordType;

        public T GetFieldValue<T>(RecordField field)
        {
            return RecordParserCodeGen.GetRecordFieldDelegate<Record, T>(_recordType, field)(this);
        }
        public virtual Record Clone<T>() where T : Record, new()
        {
            if(_recordType == null)
            {
                return new T();
            }
            T copy = _recordType.CreateInstance<T>();
            if(DynamicFields != null)
            {
                for(int i = 0; i < DynamicFields.Length; i++)
                {
                    Array.Copy((Array)DynamicFields[i], (Array)copy.DynamicFields[i], ((Array)DynamicFields[i]).Length);
                }
            }
            return copy;
        }
    }

    internal class RecordTable : Record
    {
        public string Name;
        public RecordType ItemType;
        public int Id;
        public RecordField PrimaryKeyField;
        public int CacheSize;
    }
    internal class RecordTable<T> : RecordTable
    {
        Func<T, int> _getKeyDelegate;
        Dictionary<int, T> _lookupTable = new Dictionary<int, T>();

        public RecordTable(string name, RecordType itemType, RecordField primaryKeyField = null)
        {
            Name = name;
            ItemType = itemType;
            PrimaryKeyField = primaryKeyField;
            if (PrimaryKeyField != null)
            {
                _getKeyDelegate = RecordParserCodeGen.GetRecordFieldDelegate<T, int>(ItemType, PrimaryKeyField);
            }
        }

        public virtual T Add(T item)
        {
            if (_getKeyDelegate != null)
            {
                int key = _getKeyDelegate(item);
                _lookupTable.Add(key, item);
            }
            return item;
        }
        public bool ContainsKey(int key) => _lookupTable.ContainsKey(key);
        public T Get(int key) => _lookupTable[key];
        public IEnumerable<T> Values => _lookupTable.Values;
    }

    internal class RecordBlock : Record
    {
        int RecordCount;
    }

    internal class RecordParserContext
    {
        public RecordTypeTable Types { get; private set; }
        public RecordFieldTable Fields { get; private set; }
        public RecordTableTable Tables { get; private set; }
        public ParseRuleTable ParseRules { get; private set; }
        public RecordParserContext()
        {
            Tables = new RecordTableTable();
            Types = Tables.Types;
            Fields = Tables.Fields;
            ParseRules = Tables.ParseRules;

        }
        public T Parse<T>(IStreamReader reader, ParseRule rule)
        {
            T record = rule.ParsedType.CreateInstance<T>();
            return rule.Parse(reader, record);
        }
    }

    internal class RecordTypeTable : RecordTable<RecordType>
    {
        Dictionary<string, RecordType> _nameToType = new Dictionary<string, RecordType>();

        public RecordTypeTable() : base("Type", RecordType.Type, RecordType.Type.GetField("Id"))
        {
            Id = 1;
            foreach (RecordType t in RecordType.WellKnownTypes)
            {
                Add(t);
            }
        }

        public override RecordType Add(RecordType item)
        {
            if(string.IsNullOrEmpty(item.Name))
            {
                throw new ArgumentException("RecordType Name must be non-empty");
            }
            if(ContainsKey(item.Id))
            {
                RecordType matchingIdType = Get(item.Id);
                if(item.Name == matchingIdType.Name)
                {
                    return matchingIdType;
                }
                throw new ArgumentException("Can not add new type " + item.ToString() + " because the Id is already in use by " + matchingIdType.ToString());
            }
            if(_nameToType.TryGetValue(item.Name, out RecordType existingType))
            {
                if (existingType.Id == 0)
                {
                    existingType.Id = item.Id;
                    item = existingType;
                }
                else
                {
                    throw new ArgumentException("Can not add new type " + item.ToString() + " because the Name is already in use by " + Get(item.Name).ToString());
                }
            }
            else
            {
                _nameToType.Add(item.Name, item);
            }
            return base.Add(item);
        }

        public RecordType Get(string name)
        {
            return _nameToType[name];
        }

        public RecordType GetOrCreate(string name)
        {
            return GetOrCreate(new RecordType(0, name, typeof(Record)));
        }

        public RecordType GetOrCreate(RecordType type)
        {
            if (!_nameToType.TryGetValue(type.Name, out RecordType tableType))
            {
                tableType = _nameToType[type.Name] = type;
            }
            return tableType;
        }
    }

    internal class RecordFieldTable : RecordTable<RecordField>
    {
        public RecordFieldTable() : base("Field", RecordType.Field, RecordType.Field.GetField("Id"))
        {
            Id = 2;
            foreach (RecordType type in RecordType.WellKnownTypes)
                foreach(RecordField field in type.GetFields())
                base.Add(field);
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
        public RecordTableTable() : base("Table", RecordType.Table, RecordType.Table.GetField("Id"))
        {
            Id = 3;
            base.Add(Types = new RecordTypeTable());
            base.Add(Fields = new RecordFieldTable());
            base.Add(ParseRules = new ParseRuleTable(Types, Fields, this));
            base.Add(this);
        }
        public RecordTypeTable Types { get; private set; }
        public RecordFieldTable Fields { get; private set; }
        public ParseRuleTable ParseRules { get; private set; }

        public override RecordTable Add(RecordTable item)
        {
            return base.Add(item);
        }
    }

    internal class ParseRuleTable : RecordTable<ParseRule>
    {
        public ParseRule Boolean { get; private set; }
        public ParseRule FixedUInt8 { get; private set; }
        public ParseRule FixedInt16 { get; private set; }
        public ParseRule FixedInt32 { get; private set; }
        public ParseRule FixedInt64 { get; private set; }
        public ParseRule VarUInt16 { get; private set; }
        public ParseRule VarUInt32 { get; private set; }
        public ParseRule VarUInt64 { get; private set; }
        public ParseRule UTF8String { get; private set; }
        public ParseRule Guid { get; private set; }
        public ParseRule Type { get; private set; }
        public ParseRule Field { get; private set; }
        public ParseRule Table { get; private set; }
        public ParseRule ParseRule { get; private set; }
        public ParseRule TypeBlock { get; private set; }
        public ParseRule FieldBlock { get; private set; }
        public ParseRule TableBlock { get; private set; }
        public ParseRule ParseRuleBlock { get; private set; }
        public ParseRule ParseInstruction { get; private set; }
        public ParseRule ParseInstructionStoreConstant { get; private set; }
        public ParseRule ParseInstructionStoreRead { get; private set; }
        public ParseRule ParseInstructionStoreReadDynamic { get; private set; }
        public ParseRule ParseInstructionStoreReadLookup { get; private set; }
        public ParseRule ParseInstructionStoreField { get; private set; }
        public ParseRule ParseInstructionStoreFieldLookup { get; private set; }
        public ParseRule ParseInstructionPublish { get; private set; }
        public ParseRule ParseInstructionIterateRead { get; private set; }

        public ParseRuleTable(RecordTypeTable types, RecordFieldTable fields, RecordTableTable tables) : 
            base("ParseRule", RecordType.ParseRule, RecordType.ParseRule.GetField("Id"))
        {
            Id = 4;
            Add(FixedInt32 = new ParseRule((int)PrimitiveParseRuleId.FixedInt32, "Builtin.FixedInt32", types.Get("Int32")));
            Add(UTF8String = new ParseRule((int)PrimitiveParseRuleId.UTF8String, "Builtin.UTF8String", types.Get("String")));
            Add(Type = new ParseRule(100, "Builtin.Type", RecordType.Type,
                new ParseInstructionStoreRead(RecordType.Type, RecordType.Type.GetField("Id"), ParseRule.FixedInt32),
                new ParseInstructionStoreRead(RecordType.Type, RecordType.Type.GetField("Name"), ParseRule.UTF8String),
                new ParseInstructionPublish(RecordType.Type, types)));
            Add(Field = new ParseRule(101, "Builtin.Field", RecordType.Field,
                new ParseInstructionStoreRead(RecordType.Field, RecordType.Field.GetField("Id"), ParseRule.FixedInt32),
                new ParseInstructionStoreReadLookup(RecordType.Field, RecordType.Field.GetField("ContainingType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreReadLookup(RecordType.Field, RecordType.Field.GetField("FieldType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreRead(RecordType.Field, RecordType.Field.GetField("Name"), ParseRule.UTF8String),
                new ParseInstructionPublish(RecordType.Field, fields)));
            Add(Table = new ParseRule(102, "Builtin.Table", RecordType.Table,
                new ParseInstructionStoreRead(RecordType.Table, RecordType.Table.GetField("Id"), ParseRule.FixedInt32),
                new ParseInstructionStoreReadLookup(RecordType.Table, RecordType.Table.GetField("ItemType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreReadLookup(RecordType.Table, RecordType.Table.GetField("PrimaryKeyField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreRead(RecordType.Table, RecordType.Table.GetField("Name"), ParseRule.UTF8String),
                new ParseInstructionPublish(RecordType.Table,tables)));
            Add(ParseRule = new ParseRule(103, "Builtin.ParseRule", RecordType.ParseRule,
                new ParseInstructionStoreRead(RecordType.ParseRule, RecordType.ParseRule.GetField("Id"), ParseRule.FixedInt32),
                new ParseInstructionStoreReadLookup(RecordType.ParseRule, RecordType.ParseRule.GetField("ParsedType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreRead(RecordType.ParseRule, RecordType.ParseRuleLocalVars.GetField("TempInt32"), ParseRule.FixedInt32)/*,
            Add( new ParseInstructionStoreRead(RecordType.Table.GetField("Instructions"), ParseRule.FixedInt32)*/));
            Add(ParseInstruction = new ParseRule(9000, "Builtin.ParseInstruction", RecordType.ParseInstruction,
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseRuleLocalVars.GetField("TempParseRule"), ParseRule.FixedInt32, this),
                new ParseInstructionStoreRead(RecordType.ParseInstruction, RecordType.ParseRuleLocalVars.GetField("This"), RecordType.ParseInstruction, RecordType.ParseRuleLocalVars.GetField("TempParseRule"))));
            Add(ParseInstructionStoreConstant = new ParseRule(9001, "Builtin.ParseInstructionStoreConstant", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreConstant),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ConstantType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseRuleLocalVars.GetField("TempParseRule"), ParseRule.FixedInt32, this),
                new ParseInstructionStoreRead(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("Constant"), RecordType.Object, RecordType.ParseRuleLocalVars.GetField("TempParseRule"))));
            Add(ParseInstructionStoreRead = new ParseRule(105, "Builtin.ParseInstructionStoreRead", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreRead),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ParseRule"), ParseRule.FixedInt32, this)));
            Add(ParseInstructionStoreReadDynamic = new ParseRule(7777, "Builtin.ParseInstructionStoreReadDynamic", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreRead),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ParsedType"), ParseRule.FixedInt32, types),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ParseRuleField"), ParseRule.FixedInt32, fields)));
            Add(ParseInstructionStoreReadLookup = new ParseRule(106, "Builtin.ParseInstructionStoreReadLookup", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreReadLookup),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ParseRule"), ParseRule.FixedInt32, this),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("LookupTable"), ParseRule.FixedInt32, tables)));
            Add(ParseInstructionStoreField = new ParseRule(9002, "Builtin.ParseInstructionStoreField", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreField),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("SourceField"), ParseRule.FixedInt32, fields)));
            Add(ParseInstructionStoreFieldLookup = new ParseRule(107, "Builtin.ParseInstructionStoreFieldLookup", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.StoreFieldLookup),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("DestinationField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("SourceField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("LookupTable"), ParseRule.FixedInt32, tables)));
            Add(ParseInstructionPublish = new ParseRule(108, "Builtin.ParseInstructionPublish", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.Publish),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("PublishStream"), ParseRule.FixedInt32, tables)));
            Add(ParseInstructionIterateRead = new ParseRule(109, "Builtin.ParseInstructionIterateRead", RecordType.ParseInstruction,
                new ParseInstructionStoreConstant(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("InstructionType"), RecordType.Int32, ParseInstructionType.IterateRead),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("CountField"), ParseRule.FixedInt32, fields),
                new ParseInstructionStoreReadLookup(RecordType.ParseInstruction, RecordType.ParseInstruction.GetField("ParseRule"), ParseRule.FixedInt32, this)));
            Add(TypeBlock = new ParseRule(110, "Builtin.TypeBlock", RecordType.RecordBlock,
                new ParseInstructionStoreRead(RecordType.ParseInstruction, RecordType.RecordBlock.GetField("EntryCount"), ParseRule.FixedInt32),
                new ParseInstructionIterateRead(RecordType.RecordBlock,Type, RecordType.RecordBlock.GetField("EntryCount"))));
            Add(FieldBlock = new ParseRule(111, "Builtin.FieldBlock", RecordType.RecordBlock,
                new ParseInstructionStoreRead(RecordType.RecordBlock, RecordType.RecordBlock.GetField("EntryCount"), ParseRule.FixedInt32),
                new ParseInstructionIterateRead(RecordType.RecordBlock, Field, RecordType.RecordBlock.GetField("EntryCount"))));
            //TableBlock
            //ParseRuleBlock
        }
    }
}
