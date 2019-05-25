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
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    internal class RecordFieldAttribute : Attribute {}

    [AttributeUsage(AttributeTargets.Class)]
    internal class RecordAttribute : Attribute
    {
        public RecordAttribute(string name) { Name = name; }
        public string Name { get; }
    }

    internal class DynamicFieldInfo
    {
        public Type CanonFieldSlotType;
        public int TypeIndex;
        public int FieldCount;
    }

    [Record("Type")]
    internal class RecordType : BindableRecord
    {
        [RecordField] public RecordType ArrayElement;
        public Type ReflectionType;
        public int CountWeakFieldTypes;
        public Dictionary<int, DynamicFieldInfo> WeakFieldTypesByIndex = new Dictionary<int, DynamicFieldInfo>();
        Dictionary<Type, DynamicFieldInfo> _weakFieldTypes = new Dictionary<Type, DynamicFieldInfo>();
        List<RecordField> _fields = new List<RecordField>();
        Delegate _initRecord;   // Action<T> where T = ReflectionType
        Delegate _createRecord; // Func<T> where T = ReflectionType
        Delegate _copyRecord; // Action<T,T> where T = ReflectionType

        public RecordType() { }

        public RecordType(int id, string name, RecordType arrayElement)
        {
            Id = id;
            Name = name;
            ArrayElement = arrayElement;
            ReflectionType = ArrayElement.ReflectionType.MakeArrayType();
            Freeze();
        }

        public RecordType(int id, string name, Type accessType)
        {
            Id = id;
            Name = name;
            ReflectionType = accessType;
            Freeze();
        }

        protected override void OnFreeze()
        {
            if (ReflectionType == null)
            {
                if (ArrayElement != null)
                {
                    ReflectionType = ArrayElement.ReflectionType.MakeArrayType();
                }
                else
                {
                    ReflectionType = typeof(Record);
                }
            }
        }

        public void EnsureDelegates()
        {
            Freeze();
            if (typeof(Record).IsAssignableFrom(ReflectionType))
            {
                _initRecord = RecordParserCodeGen.GetInitRecordDelegate(this);
                _createRecord = RecordParserCodeGen.GetCreateInstanceDelegate(this);
                _copyRecord = RecordParserCodeGen.GetCopyDelegate(this);
            }
        }

        public Delegate GetCreateInstanceDelegate()   { EnsureDelegates(); return _createRecord; }
        public Delegate GetInitInstanceDelegate()     { EnsureDelegates(); return _initRecord; }
        public Delegate GetCopyDelegate()             { EnsureDelegates(); return _copyRecord; }
        public RecordField[] GetFields()              { return _fields.ToArray(); }
        public void InitInstance<T>(T record)         { ((Action<T>)GetInitInstanceDelegate())(record); }
        public T CreateInstance<T>()                  { return ((Func<T>)GetCreateInstanceDelegate())(); }
        public void Copy<T>(T source, T dest)         { ((Action<T,T>)GetCopyDelegate())(source, dest); }
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

    [Record("Field")]
    internal class RecordField : BindableRecord
    {
        [RecordField] public RecordType ContainingType;
        [RecordField] public RecordType FieldType;
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

        public override object GetBindingKey()
        {
            if (string.IsNullOrEmpty(Name)) throw new SerializationException("Name must be non-empty");
            if (ContainingType == null)     throw new SerializationException("ContainingType must be non-empty");
            return Tuple.Create(Name,ContainingType);
        }

        public override string ToString()
        {
            return ContainingType?.Name + "." + Name + "(" + Id + ")";
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
        [RecordField] public ParseInstructionType InstructionType { get; protected set; }
        [RecordField] public RecordField DestinationField { get; protected set; }
        [RecordField] public object Constant { get; protected set; }
        [RecordField] public RecordType ConstantType { get { return ParsedType; } protected set { ParsedType = value; } }
        [RecordField] public ParseRule ParseRule { get; protected set; }
        [RecordField] public RecordType ParsedType { get; protected set; }
        [RecordField] public RecordField ParseRuleField { get; protected set; }
        [RecordField] public RecordField CountField { get; protected set; }
        [RecordField] public RecordField SourceField { get; protected set; }
        [RecordField] public RecordTable LookupTable { get; protected set; }
        [RecordField] public RecordTable PublishStream { get; protected set; }
        [RecordField] public RecordType ThisType { get; set; }

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
            RecordType parsedType = null, RecordField countField = null) : base(thisType, destinationField)
        {
            InstructionType = ParseInstructionType.StoreRead;
            ParseRule = rule;
            ParsedType = parsedType;
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
            Type delegateType = typeof(Func<>).MakeGenericType(recordType.ReflectionType);
            return Expression.Lambda(delegateType, GetCreateAndInitExpression(recordType)).Compile();
        }

        static Expression GetCreateAndInitExpression(RecordType recordType)
        {
            if(recordType == null || !typeof(Record).IsAssignableFrom(recordType.ReflectionType))
            {
                return Expression.Default(recordType.ReflectionType);
            }
            Expression createExpression = Expression.New(recordType.ReflectionType);
            ParameterExpression recordVar = Expression.Variable(recordType.ReflectionType);
            List<Expression> statements = new List<Expression>();
            statements.Add(Expression.Assign(recordVar, createExpression));
            statements.AddRange(GetInitRecordStatements(recordVar, recordType));
            statements.Add(recordVar);
            return Expression.Block(new ParameterExpression[] { recordVar }, statements);
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

        public static Func<T,U> GetRecordFieldDelegate<T,U>(RecordType recordType, RecordField field)
        {
            Debug.Assert(typeof(T) == recordType.ReflectionType);
            ParameterExpression record = Expression.Parameter(typeof(T), "record");
            Expression body = GetFieldReadExpression(recordType, null, record, field);
            if(body.Type != typeof(U))
            {
                body = Expression.Convert(body, typeof(U));
            }
            return Expression.Lambda<Func<T,U>>(body, record).Compile();
        }

        public static Func<IStreamReader,object> GetParseDelegate(ParseRule parseRule)
        {
            if(parseRule.ParsedType == null)
            {
                //TODO: separate ParseRuleRef from ParseRuleDef
                return null;
            }
            Expression initialThis = GetCreateAndInitExpression(parseRule.ParsedType);
            Expression body = GetParseRuleExpression(parseRule, null, initialThis);
            if(body.Type != typeof(object))
            {
                body = Expression.Convert(body, typeof(object));
            }
            return Expression.Lambda<Func<IStreamReader,object>>(body, StreamReaderParameter).Compile();
        }

        static Expression PushFrame(Expression parseRule)
        {
            return Expression.Call(typeof(ParseRuleLocalVars).GetMethod("PushFrame", BindingFlags.Static | BindingFlags.Public),
                parseRule);
        }

        static Expression PopFrame(Expression parseRule)
        {
            return Expression.Call(typeof(ParseRuleLocalVars).GetMethod("PopFrame", BindingFlags.Static | BindingFlags.Public),
                parseRule);
        }

        public static Expression GetPublishExpression(Expression record, RecordTable stream)
        {
            Expression streamItem = record;
            if (record.Type != stream.ItemType.ReflectionType)
            {
                streamItem = Expression.Convert(record, stream.ItemType.ReflectionType);
            }
            MethodInfo addMethod = stream.GetType().GetMethod("OnPublish", BindingFlags.FlattenHierarchy | BindingFlags.NonPublic | BindingFlags.Instance);
            Expression addResult = Expression.Call(Expression.Constant(stream), addMethod, streamItem);
            if(record.Type != addResult.Type)
            {
                addResult = Expression.Convert(addResult, record.Type);
            }
            return Expression.Block(Expression.Assign(record, addResult), record);
        }

        public static Expression GetIterateReadExpression(RecordType thisType, Expression record,  ParseRule rule, RecordField countField)
        {
            return RunParseRule(Expression.Constant(rule), childLocals =>
            {
                Expression count = GetFieldReadExpression(thisType, LocalVarsParameter, record, countField);
                if (countField.FieldType.ReflectionType != typeof(int))
                {
                    count = Expression.ConvertChecked(count, typeof(int));
                }
                ParameterExpression countVar = Expression.Variable(typeof(int), "count");
                Expression initCountVar = Expression.Assign(countVar, count);
                ParameterExpression indexVar = Expression.Variable(typeof(int), "index");
                Expression initIndexVar = Expression.Assign(indexVar, Expression.Constant(0));
                LabelTarget exitLoop = Expression.Label();
                //TODO: need strong type constructor
                Expression initRecordVal = Expression.Constant(rule.ParsedType.CreateInstance<Record>());
                ParameterExpression recordVar = Expression.Variable(rule.ParsedType.ReflectionType);
                Expression initRecordVar = Expression.Assign(recordVar, initRecordVal);
                Expression loop = Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(indexVar, countVar),
                        Expression.Block(
                            Expression.Assign(recordVar, GetParseExpression(rule, childLocals, recordVar)),
                            Expression.Assign(indexVar, Expression.Increment(indexVar))),
                        Expression.Break(exitLoop)),
                    exitLoop);
                return Expression.Block(new ParameterExpression[] { countVar, indexVar, recordVar },
                    initCountVar, initIndexVar, initRecordVar, loop);
            });
            
        }

        public static Delegate GetCopyDelegate(RecordType objType)
        {
            ParameterExpression source = Expression.Parameter(objType.ReflectionType, "source");
            ParameterExpression dest = Expression.Parameter(objType.ReflectionType, "dest");
            Type delegateType = typeof(Action<,>).MakeGenericType(objType.ReflectionType, objType.ReflectionType);
            return Expression.Lambda(delegateType, GetCopyExpression(objType, source, dest), source, dest).Compile();
        }

        static Expression GetCopyExpression(RecordType objType, Expression sourceObj, Expression destObj)
        {
            if (objType.GetFields().Any())
            {
                return Expression.Block(objType.GetFields().Select(f =>
                    GetStoreFieldExpression(objType, objType, destObj, destObj, f, f.FieldType, GetFieldReadExpression(objType, null, sourceObj, f))));
            }
            else
            {
                return destObj;
            }
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
                    Expression previousFieldVal = null;
                    RecordType thisType = instruction.ThisType;
                    RecordType resolvedFieldType = ResolveFieldType(thisType, instruction.DestinationField);
                    if (resolvedFieldType == parsedType && parsedType.ArrayElement == null)
                    {
                        //TODO: refine the cases where previous value gets loaded and how it gets passed to the parse rule
                        previousFieldVal = GetFieldReadExpression(thisType, LocalVarsParameter, recordParam, instruction.DestinationField);
                    }
                    else
                    {
                        Debug.Assert(!typeof(Record).IsAssignableFrom(parsedType.ReflectionType));
                        //Debug.Assert(!parsedType.ReflectionType.IsArray);
                    }
                    
                    if (instruction.ParseRuleField != null)
                    {
                        return GetInstructionStoreExpression(recordParam, instruction, parsedType,
                            GetReadRuleAndParseExpression(instruction.ThisType, recordParam, instruction.ParsedType, instruction.ParseRuleField, previousFieldVal));
                    }
                    else if(parsedType.ArrayElement != null)
                    {
                        Expression count = GetFieldReadExpression(instruction.ThisType, LocalVarsParameter, recordParam, instruction.CountField);
                        return GetInstructionStoreExpression(recordParam, instruction, parsedType,
                            GetArrayParseExpression(parsedType, instruction.ParseRule, count));
                    }
                    else
                    {
                        return GetInstructionStoreExpression(recordParam, instruction, parsedType,
                            GetParseExpression(instruction.ParseRule, null, previousFieldVal));
                    }
                case ParseInstructionType.StoreReadLookup:
                    return GetInstructionStoreExpression(recordParam, instruction, instruction.LookupTable.ItemType,
                        GetReadLookupExpression(instruction.ParseRule, instruction.LookupTable));
                case ParseInstructionType.StoreField:
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
                fieldRootType = instruction.DestinationField.ContainingType;
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
            ParseRuleLocalVars.DebugWriteLine("Read: " + fieldContainer.ToString() + " . " + field.ToString() + " = " + (val == null ? "(null)" : val.ToString()));
        }

        public static Expression GetFieldReadLookupExpression(RecordType thisType, Expression record, RecordField field, RecordTable table)
        {
            Expression fieldRead = GetFieldReadExpression(thisType, LocalVarsParameter, record, field);
            return GetLookupExpression(table, field.FieldType, fieldRead);
        }

        public static Expression GetReadLookupExpression(ParseRule rule, RecordTable table)
        {
            Expression fieldRead = GetParseExpression(rule);
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

        public static Expression GetParseExpression(ParseRule parseRule, Expression localVars = null, Expression initialThis = null)
        {
            Debug.Assert(localVars == null || localVars.Type == typeof(ParseRuleLocalVars));
            return GetParseExpression(Expression.Constant(parseRule), parseRule.ParsedType, localVars, initialThis);
        }

        public static Expression GetReadRuleAndParseExpression(RecordType thisType, Expression recordParam, RecordType parsedType, RecordField parseRuleField, Expression previousFieldVal)
        {
            Expression parseRule = GetFieldReadExpression(thisType, LocalVarsParameter, recordParam, parseRuleField);
            return GetParseExpression(parseRule, parsedType, null, previousFieldVal);
        }

        public static Expression GetArrayParseExpression(RecordType parsedType, ParseRule elementParseRule, Expression count)
        {
            return RunParseRule(Expression.Constant(elementParseRule), childLocals =>
            {
                if (count.Type != typeof(int))
                {
                    count = Expression.ConvertChecked(count, typeof(int));
                }
                Expression arrayObj = Expression.NewArrayBounds(parsedType.ReflectionType.GetElementType(), count);
                ParameterExpression arrayVar = Expression.Variable(arrayObj.Type, "array");
                Expression initiArrayVar = Expression.Assign(arrayVar, arrayObj);
                ParameterExpression countVar = Expression.Variable(typeof(int), "count");
                Expression initCountVar = Expression.Assign(countVar, count);
                ParameterExpression indexVar = Expression.Variable(typeof(int), "index");
                Expression initIndexVar = Expression.Assign(indexVar, Expression.Constant(0));
                LabelTarget exitLoop = Expression.Label();
                MethodInfo debugMethod = typeof(RecordParserCodeGen).GetMethod("DebugStoreArrayIndex",
                    BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(parsedType.ReflectionType.GetElementType());
                Expression loop = Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(indexVar, countVar),
                        Expression.Block(
                            Expression.Assign(
                                Expression.ArrayAccess(arrayVar, indexVar),
                                GetParseExpression(elementParseRule, childLocals, null)),
                            Expression.Call(debugMethod, arrayVar, indexVar),
                            Expression.Assign(indexVar, Expression.Increment(indexVar))),
                            Expression.Break(exitLoop)),
                        exitLoop);
                return Expression.Block(new ParameterExpression[] { arrayVar, countVar, indexVar },
                    initiArrayVar, initCountVar, initIndexVar, loop, arrayVar);
            });
        }

        static void DebugStoreArrayIndex<T>(T[] array, int index)
        {
            ParseRuleLocalVars.DebugWriteLine($"ret[{index}] = {array[index].ToString()}");
        }


        static Expression RunParseRule(Expression parseRule, Func<Expression, Expression> bodyFunc)
        {
            Expression locals = PushFrame(parseRule);
            ParameterExpression localsVar = Expression.Variable(typeof(ParseRuleLocalVars), "locals");
            Expression localsVarInit = Expression.Assign(localsVar, locals);
            Expression nestedBody = bodyFunc(localsVar);
            return Expression.Block(new ParameterExpression[] { localsVar },
                localsVarInit,
                Expression.TryFinally(
                    nestedBody,
                    PopFrame(parseRule)));
        }

        public static Expression GetParseExpression(Expression parseRule, RecordType parsedType, Expression localVars = null, Expression initialThis = null)
        {
            if (parsedType.ReflectionType == typeof(object))
            {
                initialThis = Expression.Default(typeof(object));
            }
            else if (initialThis == null)
            {
                initialThis = GetCreateAndInitExpression(parsedType);
            }
            //TODO: improve error handling when dynamicly loaded parseRule doesn't parse parsedType
            MethodInfo parseMethod = typeof(ParseRule).GetMethod("Parse").MakeGenericMethod(parsedType.ReflectionType);
            
            //TODO: handle case where parsedType is known but exact parseRule is not known
            Expression result = Expression.Call(parseRule, parseMethod, StreamReaderParameter);
            if (parseRule.NodeType == ExpressionType.Constant)
            {
                result = GetParseRuleExpression((ParseRule)((ConstantExpression)parseRule).Value, localVars, initialThis);
            }
            return result;
        }

        public static Expression GetParseRuleExpression(ParseRule parseRule, Expression localVars, Expression initialThis)
        {
            switch (parseRule.Name)
            {
                case "Null":
                    return Expression.Default(typeof(object));
                case "Boolean":
                    MethodInfo readByte = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.NotEqual(Expression.Call(StreamReaderParameter, readByte), Expression.Constant((byte)0));
                case "FixedByte":
                    MethodInfo readByte2 = typeof(IStreamReader).GetMethod("ReadByte", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readByte2);
                case "FixedInt16":
                    MethodInfo readInt16 = typeof(IStreamReader).GetMethod("ReadInt16", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt16);
                case "FixedInt32":
                    MethodInfo readInt32 = typeof(IStreamReader).GetMethod("ReadInt32", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt32);
                case "FixedInt64":
                    MethodInfo readInt64 = typeof(IStreamReader).GetMethod("ReadInt64", BindingFlags.Public | BindingFlags.Instance);
                    return Expression.Call(StreamReaderParameter, readInt64);
                case "Guid":
                    MethodInfo readGuid = typeof(IStreamWriterExentions).GetMethod("ReadGuid", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readGuid, StreamReaderParameter);
                case "UTF8String":
                    MethodInfo readUtf8 = typeof(ParseFunctions).GetMethod("ReadUTF8String", BindingFlags.Public | BindingFlags.Static);
                    return Expression.Call(readUtf8, StreamReaderParameter);
                default:
                    if(localVars == null)
                    {
                        return RunParseRule(Expression.Constant(parseRule),
                            locals => GetParseWithInstructionsExpression(parseRule, locals, initialThis));
                    }
                    return GetParseWithInstructionsExpression(parseRule, localVars, initialThis);
            }
        }

        private static Expression GetParseWithInstructionsExpression(ParseRule parseRule, Expression localVars, Expression initialThis)
        {
            MethodInfo initFrame = typeof(ParseRuleLocalVars).GetMethod("InitFrame", BindingFlags.Public | BindingFlags.Instance);
            localVars = Expression.Call(localVars, initFrame, StreamReaderParameter);
            MethodInfo parseWithInstructions = typeof(ParseRule).GetMethod("ParseWithInstructions", BindingFlags.NonPublic | BindingFlags.Instance)
                                                .MakeGenericMethod(parseRule.ParsedType.ReflectionType);
            return Expression.Call(Expression.Constant(parseRule), parseWithInstructions, localVars, StreamReaderParameter, initialThis);
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
            if(numBytes == 0)
            {
                return null;
            }
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

    internal class ParseRule : BindableRecord
    {
        [RecordField] public RecordType ParsedType;
        [RecordField] public ParseInstruction[] Instructions;

        Func<IStreamReader, object> _cachedParseFunc;

        public ParseRule() { }
        public ParseRule(int id, string name, RecordType parsedType, params ParseInstruction[] instructions)
        {
            Id = id;
            Name = name;
            ParsedType = parsedType;
            Instructions = instructions;
            //TODO: validate instructions
            Freeze();
        }
        protected override void OnFreeze()
        {
            if(Instructions != null)
            {
                foreach (ParseInstruction instr in Instructions)
                {
                    instr.ThisType = ParsedType;
                }
            }
            _cachedParseFunc = RecordParserCodeGen.GetParseDelegate(this);
        }

        private T ParseWithInstructions<T>(ParseRuleLocalVars locals, IStreamReader reader, T initialThis)
        {
            //ParseRuleLocalVars.DebugWriteLine("Reading " + Name.ToString() + " at: " + reader.Current);
            Debug.Assert(ParsedType.ReflectionType == typeof(T));
            foreach (ParseInstruction instruction in Instructions)
            {
                instruction.Execute(reader, locals, ref initialThis);
            }
            return initialThis;
        }

        public T Parse<T>(IStreamReader reader)
        {
            return (T) _cachedParseFunc(reader);
        }
        public override string ToString()
        {
            return Name + "(" + Id + ")";
        }
    }

    internal class ParseRuleLocalVars : Record
    {
        [ThreadStatic] static Stack<ParseRuleLocalVars> _stack = new Stack<ParseRuleLocalVars>();
        public static ParseRuleLocalVars PushFrame(ParseRule rule)
        {
            
            DebugWriteLine(rule.ToString());
            DebugWriteLine("{");
            ParseRuleLocalVars frame = new ParseRuleLocalVars(rule);
            _stack.Push(frame);
            return frame;
        }
        public static void PopFrame(ParseRule rule)
        {
            ParseRuleLocalVars locals = _stack.Pop();
            DebugWriteLine("}");
            Debug.Assert(rule == locals._rule);
        }

        public static void DebugWriteLine(string line)
        {
            //string tab = new string(' ', _stack.Count * 2);
            //Debug.WriteLine(tab + line);
        }

        static ParseRuleLocalVars CurrentFrame { get { return _stack.Peek(); } }

        ParseRule _rule;
        IStreamReader _reader;
        StreamLabel _positionStart;
        public ParseRuleLocalVars(ParseRule rule) => _rule = rule;

        public ParseRuleLocalVars InitFrame(IStreamReader reader)
        {
            _reader = reader;
            _positionStart = _reader.Current;
            return this;
        }

        [RecordField] public int TempInt32;
        [RecordField] public ParseRule TempParseRule;
        [RecordField] public int CurrentOffset
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
        RecordType _recordType;
        bool _frozen;

        public T GetFieldValue<T>(RecordField field)
        {
            return RecordParserCodeGen.GetRecordFieldDelegate<Record, T>(_recordType, field)(this);
        }

        public void Freeze()
        {
            if(!_frozen)
            {
                OnFreeze();
                _frozen = true;
            }
        }

        protected virtual void OnFreeze() { }
    }

    internal class BindableRecord : Record
    {
        [RecordField] public int Id;
        [RecordField] public string Name;

        public virtual object GetBindingKey()
        {
            if (string.IsNullOrEmpty(Name))
            {
                throw new SerializationException("Name must be non-empty");
            }
            return Name;
        }
    }

    internal class RecordBlock : Record
    {
        [RecordField]public int RecordCount;
    }

    internal class RecordParserContext
    {
        public RecordTypeTable Types { get; private set; }
        public RecordFieldTable Fields { get; private set; }
        public RecordTableTable Tables { get; private set; }
        public ParseRuleTable ParseRules { get; private set; }
        public RecordParserContext()
        {
            RecordType table = new RecordType(0, "Table", typeof(RecordTable));
            RecordType int32 = new RecordType(0, "Int32", typeof(int));
            RecordField tableId = new RecordField(0, "Id", table, int32);
            Tables = new RecordTableTable(table, tableId);
            Types = Tables.Types;
            Fields = Tables.Fields;
            ParseRules = Tables.ParseRules;

        }
        public T Parse<T>(IStreamReader reader, ParseRule rule)
        {
            return rule.Parse<T>(reader);
        }
    }

    [Record("Table")]
    internal class RecordTable : BindableRecord
    {
        [RecordField] public RecordType ItemType;
        [RecordField] public RecordField PrimaryKeyField;
        [RecordField] public int CacheSize;
        protected virtual void OnParseComplete() { }
    }

    internal class RecordTable<T> : RecordTable where T : Record
    {
        Func<T, int> _getKeyDelegate;
        Dictionary<int, T> _lookupTable = new Dictionary<int, T>();

        public RecordTable(string name, RecordType itemType, RecordField primaryKeyField = null)
        {
            Name = name;
            ItemType = itemType;
            PrimaryKeyField = primaryKeyField;
            OnParseComplete();
        }

        protected override void OnParseComplete()
        {
            if (PrimaryKeyField != null)
            {
                _getKeyDelegate = RecordParserCodeGen.GetRecordFieldDelegate<T, int>(ItemType, PrimaryKeyField);
            }
        }

        protected virtual T OnPublish(T item)
        {
            T itemCopy = ItemType.CreateInstance<T>();
            ItemType.Copy(item, itemCopy);
            itemCopy.Freeze();
            Add(itemCopy);
            return item;
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
        public T Get(int key) { _lookupTable.TryGetValue(key, out T val); return val; }
        public virtual IEnumerable<T> Values => _lookupTable.Values;
        public virtual int Count => _lookupTable.Count;
    }

    internal class BindableRecordTable<Key, T> : RecordTable<T> where T : BindableRecord
    {
        Dictionary<object, T> _bindingKeyLookupTable = new Dictionary<object, T>();

        public BindableRecordTable(string name, RecordType itemType, RecordField primaryKeyField) : base(name, itemType, primaryKeyField) { }

        public override T Add(T item)
        {
            object key = item.GetBindingKey();
            if (!_bindingKeyLookupTable.TryGetValue(key, out T existingItem))
            {
                _bindingKeyLookupTable[key] = existingItem = item;
                if(item.Id != 0)
                {
                    base.Add(item);
                }
            }
            else if(item.Id != 0 && existingItem.Id == 0)
            {
                Bind(existingItem, item.Id);
            }
            else if(item.Id != 0 && existingItem.Id != item.Id)
            {
                throw new SerializationException(Name + " table can not add new item " + item.ToString() + " because the key is already in use by " + existingItem.ToString());
            }
            return existingItem;
        }

        public void Bind(T item, int id)
        {
            Debug.Assert(item.Id == 0);
            item.Id = id;
            base.Add(item);
        }

        public T Get(Key key) { _bindingKeyLookupTable.TryGetValue(key, out T ret); return ret; }
        public override IEnumerable<T> Values => _bindingKeyLookupTable.Values;
        public override int Count => _bindingKeyLookupTable.Count;
    }

    internal class RecordTypeTable : BindableRecordTable<string, RecordType>
    {
        public RecordType Object { get; private set; }
        public RecordType Boolean { get; private set; }
        public RecordType Byte { get; private set; }
        public RecordType Int16 { get; private set; }
        public RecordType Int32 { get; private set; }
        public RecordType Int64 { get; private set; }
        public RecordType UInt16 { get; private set; }
        public RecordType UInt32 { get; private set; }
        public RecordType UInt64 { get; private set; }
        public RecordType String { get; private set; }
        public RecordType Guid { get; private set; }
        public RecordType Type { get; private set; }
        public RecordType Field { get; private set; }
        public RecordType Table { get; private set; }
        public RecordType ParseRule { get; private set; }
        public RecordType ParseRuleLocalVars { get; private set; }
        public RecordType RecordBlock { get; private set; }
        public RecordType ParseInstruction { get; private set; }
        public RecordType ParseInstructionArray { get; private set; }

        RecordFieldTable _fields;
        Dictionary<string, RecordType> _nameToType = new Dictionary<string, RecordType>();

        public RecordTypeTable(RecordFieldTable fields, RecordType type, RecordField typeId, RecordType table, RecordType field) : base("Type", type, typeId)
        {
            _fields = fields;
            Object = GetOrCreate(typeof(object));
            Boolean = GetOrCreate(typeof(bool));
            Byte = GetOrCreate(typeof(byte));
            Int16 = GetOrCreate(typeof(short));
            Int32 = Add(typeId.FieldType);
            Int64 = GetOrCreate(typeof(long));
            UInt16 = GetOrCreate(typeof(ushort));
            UInt32 = GetOrCreate(typeof(uint));
            UInt64 = GetOrCreate(typeof(ulong));
            String = GetOrCreate(typeof(string));
            Guid = GetOrCreate(typeof(Guid));
            Type = Add(type);
            Field = Add(field);
            Table = Add(table);
            ParseRule = GetOrCreate(typeof(ParseRule));
            ParseRuleLocalVars = GetOrCreate(typeof(ParseRuleLocalVars));
            _fields.Add(new RecordField(0, "This", ParseRuleLocalVars, null));
            _fields.Add(new RecordField(0, "Null", ParseRuleLocalVars, null));
            RecordBlock = GetOrCreate(typeof(RecordBlock));
            ParseInstruction = GetOrCreate(typeof(ParseInstruction));
            ParseInstructionArray = GetOrCreate(typeof(ParseInstruction[]));
            OnParseComplete();
        }

        public override RecordType Add(RecordType item)
        {
            if(Get(item.Name) == null)
            {
                base.Add(item);
                AddFields(item);
            }
            return base.Add(item);
        }

        public RecordType GetOrCreate(Type reflectionType)
        {
            string name = reflectionType.GetTypeInfo().GetCustomAttribute<RecordAttribute>()?.Name ?? reflectionType.Name;
            if (reflectionType.IsArray)
            {
                RecordType elementType = GetOrCreate(reflectionType.GetElementType());
                return Add(new RecordType(0, name, elementType));
            }
            else
            {
                return Add(new RecordType(0, name, reflectionType));
            }
        }

        private void AddFields(RecordType type)
        {
            foreach(RecordField field in type.GetFields())
            {
                _fields.Add(field);
            }
            if (type.ReflectionType != null)
            {
                foreach (FieldInfo field in type.ReflectionType.GetFields().Where(f => f.GetCustomAttributes(typeof(RecordFieldAttribute)).Any()))
                {
                    RecordType fieldType = GetOrCreate(field.FieldType);
                    _fields.Add(type.GetField(field.Name) ?? new RecordField(0, field.Name, type, fieldType));
                }
                foreach (PropertyInfo prop in type.ReflectionType.GetProperties().Where(f => f.GetCustomAttributes(typeof(RecordFieldAttribute)).Any()))
                {
                    RecordType propType = GetOrCreate(prop.PropertyType);
                    _fields.Add(type.GetField(prop.Name) ?? new RecordField(0, prop.Name, type, propType));
                }
            }
        }
    }

    internal class RecordFieldTable : BindableRecordTable<Tuple<string,RecordType>, RecordField>
    {
        public RecordFieldTable(RecordType field, RecordField fieldId) : base("Field", field, fieldId) {}

        public override RecordField Add(RecordField field)
        {
            field = base.Add(field);
            RecordField existingField = field.ContainingType.GetField(field.Name);
            if(existingField == null)
            {
                field.ContainingType.AddField(field);
            }
            return field;
        }
    }
    internal class RecordTableTable : BindableRecordTable<string, RecordTable>
    {
        public RecordTableTable(RecordType tableType, RecordField tableId) : base("Table", tableType, tableId)
        {
            RecordType type = new RecordType(0, "Type", typeof(RecordType));
            RecordField typeId = new RecordField(0, "Id", type, tableId.FieldType);
            RecordType field = new RecordType(0, "Field", typeof(RecordField));
            RecordField fieldId = new RecordField(0, "Id", field, tableId.FieldType);
            base.Add(Fields = new RecordFieldTable(field, fieldId));
            base.Add(Types = new RecordTypeTable(Fields, type, typeId, tableType, field));
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

    internal class ParseRuleTable : BindableRecordTable<string, ParseRule>
    {
        public ParseRule Null { get; private set; }
        public ParseRule Object { get; private set; }
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
        public ParseRule ParseRuleBinding { get; private set; }
        public ParseRule TypeBlock { get; private set; }
        public ParseRule FieldBlock { get; private set; }
        public ParseRule TableBlock { get; private set; }
        public ParseRule ParseRuleBlock { get; private set; }
        public ParseRule ParseRuleBindingBlock { get; private set; }
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
            base("ParseRule", types.ParseRule, types.ParseRule.GetField("Id"))
        {
            Add(Null = new ParseRule(0, "Null", types.Object));
            Add(Boolean = new ParseRule(0, "Boolean", types.Boolean));
            Add(FixedUInt8 = new ParseRule(0, "FixedByte", types.Byte));
            Add(FixedInt16 = new ParseRule(0, "FixedInt16", types.Int16));
            Add(FixedInt32 = new ParseRule(0, "FixedInt32", types.Int32));
            Add(FixedInt64 = new ParseRule(0, "FixedInt64", types.Int64));
            Add(VarUInt16 = new ParseRule(0, "VarUInt16", types.UInt16));
            Add(VarUInt32 = new ParseRule(0, "VarUInt32", types.UInt32));
            Add(VarUInt64 = new ParseRule(0, "VarUInt64", types.UInt64));
            Add(UTF8String = new ParseRule(0, "UTF8String", types.String));
            Add(Guid = new ParseRule(0, "Guid", types.Guid));
            Add(Object = new ParseRule(0, "Object", types.Object,
                new ParseInstructionStoreReadLookup(types.Object, types.ParseRuleLocalVars.GetField("TempParseRule"), FixedInt32, this),
                new ParseInstructionStoreRead(types.Object, types.ParseRuleLocalVars.GetField("This"), types.Object, types.ParseRuleLocalVars.GetField("TempParseRule"))));
            Add(Type = new ParseRule(0, "Type", types.Type,
                new ParseInstructionStoreRead(types.Type, types.Type.GetField("Id"), FixedInt32),
                new ParseInstructionStoreRead(types.Type, types.Type.GetField("Name"), UTF8String),
                new ParseInstructionPublish(types.Type, types)));
            Add(Field = new ParseRule(0, "Field", types.Field,
                new ParseInstructionStoreRead(types.Field, types.Field.GetField("Id"), FixedInt32),
                new ParseInstructionStoreReadLookup(types.Field, types.Field.GetField("ContainingType"), FixedInt32, types),
                new ParseInstructionStoreReadLookup(types.Field, types.Field.GetField("FieldType"), FixedInt32, types),
                new ParseInstructionStoreRead(types.Field, types.Field.GetField("Name"), UTF8String),
                new ParseInstructionPublish(types.Field, fields)));
            Add(Table = new ParseRule(0, "Table", types.Table,
                new ParseInstructionStoreRead(types.Table, types.Table.GetField("Id"), FixedInt32),
                new ParseInstructionStoreReadLookup(types.Table, types.Table.GetField("ItemType"), FixedInt32, types),
                new ParseInstructionStoreReadLookup(types.Table, types.Table.GetField("PrimaryKeyField"), FixedInt32, fields),
                new ParseInstructionStoreRead(types.Table, types.Table.GetField("Name"), UTF8String),
                new ParseInstructionPublish(types.Table,tables)));
            Add(ParseInstruction = new ParseRule(0, "ParseInstruction", types.ParseInstruction,
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseRuleLocalVars.GetField("TempParseRule"), FixedInt32, this),
                new ParseInstructionStoreRead(types.ParseInstruction, types.ParseRuleLocalVars.GetField("This"), types.ParseInstruction, types.ParseRuleLocalVars.GetField("TempParseRule"))));
            Add(ParseRule = new ParseRule(0, "ParseRule", types.ParseRule,
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRule.GetField("Id"), FixedInt32),
                new ParseInstructionStoreReadLookup(types.ParseRule, types.ParseRule.GetField("ParsedType"), FixedInt32, types),
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRule.GetField("Name"), UTF8String),
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRuleLocalVars.GetField("TempInt32"), FixedInt32),
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRule.GetField("Instructions"), ParseInstruction, types.ParseInstructionArray, types.ParseRuleLocalVars.GetField("TempInt32")),
                new ParseInstructionPublish(types.ParseRule, this)));
            Add(ParseRuleBinding = new ParseRule(0, "ParseRuleBinding", types.ParseRule,
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRule.GetField("Id"), FixedInt32),
                new ParseInstructionStoreRead(types.ParseRule, types.ParseRule.GetField("Name"), UTF8String),
                new ParseInstructionPublish(types.ParseRule, this)));
            Add(ParseInstructionStoreConstant = new ParseRule(0, "ParseInstructionStoreConstant", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreConstant),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ConstantType"), FixedInt32, types),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseRuleLocalVars.GetField("TempParseRule"), FixedInt32, this),
                new ParseInstructionStoreRead(types.ParseInstruction, types.ParseInstruction.GetField("Constant"), types.Object, types.ParseRuleLocalVars.GetField("TempParseRule")),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionStoreRead = new ParseRule(0, "ParseInstructionStoreRead", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreRead),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParseRule"), FixedInt32, this),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParsedType"), FixedInt32, types),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("CountField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionStoreReadDynamic = new ParseRule(0, "ParseInstructionStoreReadDynamic", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreRead),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParsedType"), FixedInt32, types),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParseRuleField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionStoreReadLookup = new ParseRule(0, "ParseInstructionStoreReadLookup", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreReadLookup),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParseRule"), FixedInt32, this),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("LookupTable"), FixedInt32, tables),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionStoreField = new ParseRule(0, "ParseInstructionStoreField", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreField),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("SourceField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionStoreFieldLookup = new ParseRule(0, "ParseInstructionStoreFieldLookup", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.StoreFieldLookup),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("DestinationField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("SourceField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("LookupTable"), FixedInt32, tables),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionPublish = new ParseRule(0, "ParseInstructionPublish", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.Publish),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("PublishStream"), FixedInt32, tables),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(ParseInstructionIterateRead = new ParseRule(0, "ParseInstructionIterateRead", types.ParseInstruction,
                new ParseInstructionStoreConstant(types.ParseInstruction, types.ParseInstruction.GetField("InstructionType"), types.Int32, (int)ParseInstructionType.IterateRead),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("CountField"), FixedInt32, fields),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ParseRule"), FixedInt32, this),
                new ParseInstructionStoreReadLookup(types.ParseInstruction, types.ParseInstruction.GetField("ThisType"), FixedInt32, types)));
            Add(TypeBlock = new ParseRule(0, "TypeBlock", types.RecordBlock,
                new ParseInstructionStoreRead(types.ParseInstruction, types.RecordBlock.GetField("RecordCount"), FixedInt32),
                new ParseInstructionIterateRead(types.RecordBlock,Type, types.RecordBlock.GetField("RecordCount"))));
            Add(FieldBlock = new ParseRule(0, "FieldBlock", types.RecordBlock,
                new ParseInstructionStoreRead(types.RecordBlock, types.RecordBlock.GetField("RecordCount"), FixedInt32),
                new ParseInstructionIterateRead(types.RecordBlock, Field, types.RecordBlock.GetField("RecordCount"))));
            Add(TableBlock = new ParseRule(0, "TableBlock", types.RecordBlock,
                new ParseInstructionStoreRead(types.RecordBlock, types.RecordBlock.GetField("RecordCount"), FixedInt32),
                new ParseInstructionIterateRead(types.RecordBlock, Table, types.RecordBlock.GetField("RecordCount"))));
            Add(ParseRuleBlock = new ParseRule(0, "ParseRuleBlock", types.RecordBlock,
                new ParseInstructionStoreRead(types.RecordBlock, types.RecordBlock.GetField("RecordCount"), FixedInt32),
                new ParseInstructionIterateRead(types.RecordBlock, ParseRule, types.RecordBlock.GetField("RecordCount"))));
            Add(ParseRuleBindingBlock = new ParseRule(0, "ParseRuleBindingBlock", types.RecordBlock,
                new ParseInstructionStoreRead(types.RecordBlock, types.RecordBlock.GetField("RecordCount"), FixedInt32),
                new ParseInstructionIterateRead(types.RecordBlock, ParseRuleBinding, types.RecordBlock.GetField("RecordCount"))));
            

        }
    }
}
