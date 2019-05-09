using Microsoft.Diagnostics.Tracing.EventPipe;
using EventPipe = Microsoft.Diagnostics.Tracing.EventPipe;
using Record = Microsoft.Diagnostics.Tracing.EventPipe.Record;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using FastSerialization;
using System.IO;

namespace TraceEventTests
{
    public class EventPipeRecordParser
    {

       
        class SimpleRecord : Record
        {
            public Guid Guid;
            public byte Byte;
            public short Number16;
            public int Number32;
            public long Number64;
            public string String;
            public bool Bool;
        }

        class ComplexRecordA : Record
        {
            public string String;
            public SimpleRecord SimpleOne;
            public SimpleRecord SimpleTwo;
        }

        class ComplexRecordB : Record
        {
            public bool Bool;
            public string String;
            public ComplexRecordA ComplexAOne;
            public Guid Guid;
            public SimpleRecord SimpleOne;
            public ComplexRecordA ComplexATwo;
        }

        private RecordType GetSimpleRecordType<T>()
        {
            RecordType simpleType = new RecordType(1000, "SimpleRecord", typeof(T));
            simpleType.AddField(new RecordField(1000, "Guid", simpleType, RecordType.Guid));
            simpleType.AddField(new RecordField(1001, "Byte", simpleType, RecordType.Byte));
            simpleType.AddField(new RecordField(1002, "Number16", simpleType, RecordType.Int16));
            simpleType.AddField(new RecordField(1003, "Number32", simpleType, RecordType.Int32));
            simpleType.AddField(new RecordField(1004, "Number64", simpleType, RecordType.Int64));
            simpleType.AddField(new RecordField(1005, "String", simpleType, RecordType.String));
            simpleType.AddField(new RecordField(1006, "Bool", simpleType, RecordType.Boolean));
            return simpleType;
        }

        private Guid ExampleGuid = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
        private byte ExampleByte = 0xab;
        private short ExampleShort = 0x1234;
        private int ExampleInt = 0x1234567;
        private long ExampleLong = 0xabcdef012345;
        private string ExampleString = "hi there";
        private bool ExampleBool = true;
        private SimpleRecord GetSimpleRecord()
        {
            return new SimpleRecord()
            {
                Guid = ExampleGuid,
                Byte = ExampleByte,
                Number16 = ExampleShort,
                Number32 = ExampleInt,
                Number64 = ExampleLong,
                String = ExampleString,
                Bool = ExampleBool
            };
        }
        private IStreamReader GetSimpleTypeStreamReader(SimpleRecord record = null)
        {
            if (record == null)
            {
                record = GetSimpleRecord();
            }
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms, Encoding.UTF8);

            writer.Write(record.Guid.ToByteArray());
            writer.Write(record.Byte);
            writer.Write(record.Number16);
            writer.Write(record.Number32);
            writer.Write(record.Number64);
            writer.Write(record.String);
            writer.Write(record.Bool);
            ms.Position = 0;
            return new PinnedStreamReader(ms);
        }



        [Fact]
        public void GetSetStaticFields()
        {
            // Confirm that we can write to strongly typed fields
            RecordType simpleType = GetSimpleRecordType<SimpleRecord>();
            SimpleRecord record = new SimpleRecord();
            record.Init(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            Action<IStreamReader, Record> storeGuid = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(g), simpleType.GetField("Guid"));
            storeGuid(null, record);
            Assert.Equal(g, record.Guid);

            Action<IStreamReader, Record> storeByte = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant((byte)0xab), simpleType.GetField("Byte"));
            storeByte(null, record);
            Assert.Equal(0xab, record.Byte);

            Action<IStreamReader, Record> storeNumber16 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant((short)17003), simpleType.GetField("Number16"));
            storeNumber16(null, record);
            Assert.Equal(17003, record.Number16);

            Action<IStreamReader, Record> storeNumber32 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(-19004001), simpleType.GetField("Number32"));
            storeNumber32(null, record);
            Assert.Equal(-19004001, record.Number32);

            Action<IStreamReader, Record> storeNumber64 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(0x123456789abcdef), simpleType.GetField("Number64"));
            storeNumber64(null, record);
            Assert.Equal(0x123456789abcdef, record.Number64);

            Action<IStreamReader, Record> storeString = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant("hello"), simpleType.GetField("String"));
            storeString(null, record);
            Assert.Equal("hello", record.String);

            Action<IStreamReader, Record> storeBool = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(true), simpleType.GetField("Bool"));
            storeBool(null, record);
            Assert.True(record.Bool);
            
        }

        [Fact]
        public void GetSetDynamicFields()
        {
            // Confirm that we can read and write to weakly typed fields
            RecordType simpleType = GetSimpleRecordType<Record>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            Record record = new Record();
            record.Init(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            Action<IStreamReader, Record> storeGuid = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(g), guidField);
            storeGuid(null, record);
            Assert.Equal(g, ((Guid[])record.DynamicFields[guidField.DynamicFieldTypeIndex])[guidField.DynamicFieldIndex]);
            Assert.Equal(g, record.GetFieldValue<Guid>(guidField));

            Action<IStreamReader, Record> storeByte = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant((byte)0xab),byteField);
            storeByte(null, record);
            Assert.Equal(0xab, ((byte[])record.DynamicFields[byteField.DynamicFieldTypeIndex])[byteField.DynamicFieldIndex]);
            Assert.Equal(0xab, record.GetFieldValue<byte>(byteField));

            Action<IStreamReader, Record> storeNumber16 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant((short)17003), number16Field);
            storeNumber16(null, record);
            Assert.Equal(17003, ((short[])record.DynamicFields[number16Field.DynamicFieldTypeIndex])[number16Field.DynamicFieldIndex]);
            Assert.Equal(17003, record.GetFieldValue<short>(number16Field));

            Action<IStreamReader, Record> storeNumber32 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(-19004001), number32Field);
            storeNumber32(null, record);
            Assert.Equal(-19004001, ((int[])record.DynamicFields[number32Field.DynamicFieldTypeIndex])[number32Field.DynamicFieldIndex]);
            Assert.Equal(-19004001, record.GetFieldValue<int>(number32Field));

            Action<IStreamReader, Record> storeNumber64 = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(0x123456789abcdef), number64Field);
            storeNumber64(null, record);
            Assert.Equal(0x123456789abcdef, ((long[])record.DynamicFields[number64Field.DynamicFieldTypeIndex])[number64Field.DynamicFieldIndex]);
            Assert.Equal(0x123456789abcdef, record.GetFieldValue<long>(number64Field));

            Action<IStreamReader, Record> storeString = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant("hello"), stringField);
            storeString(null, record);
            Assert.Equal("hello", ((object[])record.DynamicFields[stringField.DynamicFieldTypeIndex])[stringField.DynamicFieldIndex]);
            Assert.Equal("hello", record.GetFieldValue<string>(stringField));

            Action<IStreamReader, Record> storeBool = RecordParseCodeGen.GetStoreFieldDelegate(simpleType,
                Expression.Constant(true),  boolField);
            storeBool(null, record);
            Assert.True(((bool[])record.DynamicFields[boolField.DynamicFieldTypeIndex])[boolField.DynamicFieldIndex]);
            Assert.True(record.GetFieldValue<bool>(boolField));

        }

        /*
             [Fact]
             public void NumericConversions()
             {
                 // Confirm that we can convert between numeric types if the data fits, and throw if it doesn't

                 RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(SimpleRecordType));
                 RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
                 RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
                 RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
                 SimpleRecordType record = new SimpleRecordType();

                 // converting up
                 ParseRecord<SimpleRecordType> storeNumber64 = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
                     Expression.Constant((short)0xab), new RecordField[] { number64Field });
                 storeNumber64(ref record);
                 Assert.Equal(0xab, record.Number64);

                 //Converting down
                 ParseRecord<SimpleRecordType> storeByte = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
                         Expression.Constant((short)3), new RecordField[] { byteField });
                 storeByte(ref record);
                 Assert.Equal(3, record.Byte);

                 Assert.Throws<OverflowException>(() =>
                 {
                     ParseRecord<SimpleRecordType> storeByteBig = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
                         Expression.Constant((short)300), new RecordField[] { byteField });
                     storeByteBig(ref record);
                 });

                 ParseRecord<SimpleRecordType> storeInt = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
                     Expression.Constant((long)-30982), new RecordField[] { number32Field });
                 storeInt(ref record);
                 Assert.Equal(-30982, record.Number32);

                 Assert.Throws<OverflowException>(() =>
                 {
                     ParseRecord<SimpleRecordType> storeIntBig = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
                         Expression.Constant(0x1234567890), new RecordField[] { number32Field });
                     storeIntBig(ref record);
                 });
             }
             */

        /*
    [Fact]
    public void NonNumericCoercions()
    {
        // Confirm that we cannot coerce between types that aren't compatible

        RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(SimpleRecordType));
        RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
        RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
        RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
        RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);

        // string -> int64
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeNumber64 = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant("hi"), new RecordField[] { number64Field });
        });

        // int64 -> bool
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeBool = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant(0x1), new RecordField[] { boolField });
        });

        // int64 -> Guid
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeGuid = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant(0x123456789), new RecordField[] { guidField });
        });

        // int64 -> string
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeString = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant(0x123456789), new RecordField[] { stringField });
        });

        // guid -> string
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeString = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant(new Guid()), new RecordField[] { stringField });
        });

        // string -> guid
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeGuid = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant("hi"), new RecordField[] { guidField });
        });

        // bool -> string
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeString = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant(true), new RecordField[] { stringField });
        });

        // string -> bool
        Assert.Throws<InvalidOperationException>(() =>
        {
            ParseRecord<SimpleRecordType> storeBool = simpleType.GetStoreFieldDelegate<SimpleRecordType>(
               Expression.Constant("true"), new RecordField[] { boolField });
        });
    }
    */

        /*
    [Fact]
    public void SetNestedBackedRecordField()
    {
        //Confirm we can set backed fields in nested types

        RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(SimpleRecordType));
        RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
        RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
        RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);

        RecordType complexTypeA = new RecordType(3, "ComplexRecordTypeA", typeof(ComplexRecordTypeA));
        RecordField cAsimpleOne = new RecordField(0, "SimpleOne", complexTypeA, simpleType);
        RecordField cAsimpleTwo = new RecordField(0, "SimpleTwo", complexTypeA, simpleType);

        RecordType complexTypeB = new RecordType(4, "ComplexRecordTypeB", typeof(ComplexRecordTypeB));
        RecordField cBcomplexAOne = new RecordField(0, "ComplexAOne", complexTypeB, complexTypeA);
        RecordField cBcomplexATwo = new RecordField(0, "ComplexATwo", complexTypeB, complexTypeA);
        RecordField cBsimpleOne = new RecordField(0, "SimpleOne", complexTypeB, simpleType);
        ComplexRecordTypeB record = new ComplexRecordTypeB();

        Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
        ParseRecord<ComplexRecordTypeB> storeGuid = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
            Expression.Constant(g), new RecordField[] { cBcomplexATwo, cAsimpleOne, guidField });
        storeGuid(ref record);
        Assert.Equal(g, record.ComplexATwo.SimpleOne.Guid);

        ParseRecord<ComplexRecordTypeB> storeByte = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
            Expression.Constant((byte)0xab), new RecordField[] { cBcomplexAOne, cAsimpleTwo, byteField });
        storeByte(ref record);
        Assert.Equal(0xab, record.ComplexAOne.SimpleTwo.Byte);

        ParseRecord<ComplexRecordTypeB> storeNumber16 = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
            Expression.Constant((short)17003), new RecordField[] { cBsimpleOne, number16Field });
        storeNumber16(ref record);
        Assert.Equal(17003, record.SimpleOne.Number16);
    }
    */
        /*
        [Fact]
        public void CopyNestedBackedFields()
        {
            //Confirm we can read fields backed in nested types and copy them

            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(SimpleRecordType));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);

            RecordType complexTypeA = new RecordType(3, "ComplexRecordTypeA", typeof(ComplexRecordTypeA));
            RecordField cAsimpleOne = new RecordField(0, "SimpleOne", complexTypeA, simpleType);
            RecordField cAsimpleTwo = new RecordField(0, "SimpleTwo", complexTypeA, simpleType);

            RecordType complexTypeB = new RecordType(4, "ComplexRecordTypeB", typeof(ComplexRecordTypeB));
            RecordField cBcomplexAOne = new RecordField(0, "ComplexAOne", complexTypeB, complexTypeA);
            RecordField cBcomplexATwo = new RecordField(0, "ComplexATwo", complexTypeB, complexTypeA);
            RecordField cBsimpleOne = new RecordField(0, "SimpleOne", complexTypeB, simpleType);
            ComplexRecordTypeB record = new ComplexRecordTypeB();

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            Expresion readGuid = complexTypeB.GetReadFieldExpression()
            ParseRecord<ComplexRecordTypeB> storeGuid = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
                Expression.Constant(g), new RecordField[] { cBsimpleOne, guidField });
            storeGuid(ref record);
            Assert.Equal(g, record.ComplexATwo.SimpleOne.Guid);

            ParseRecord<ComplexRecordTypeB> storeByte = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
                Expression.Constant((byte)0xab), new RecordField[] { cBsimpleOne, byteField });
            storeByte(ref record);
            Assert.Equal(0xab, record.ComplexAOne.SimpleTwo.Byte);

            ParseRecord<ComplexRecordTypeB> storeNumber16 = complexTypeB.GetStoreFieldDelegate<ComplexRecordTypeB>(
                Expression.Constant((short)17003), new RecordField[] { cBsimpleOne, number16Field });
            storeNumber16(ref record);
            Assert.Equal(17003, record.SimpleOne.Number16);
        }*/

        [Fact]
        public void StoreConstantInstructionDynamic()
        {
            RecordType simpleType = GetSimpleRecordType<Record>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            Record record = new Record();
            record.Init(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            ParseInstruction instr = ParseInstruction.StoreConstant(guidField, g);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<Guid>(guidField));

            instr = ParseInstruction.StoreConstant(byteField, (byte)0xab);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<byte>(byteField));

            instr = ParseInstruction.StoreConstant(number16Field, (short)0x3b12);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<short>(number16Field));

            instr = ParseInstruction.StoreConstant(number32Field, -1002003004);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<int>(number32Field));

            instr = ParseInstruction.StoreConstant(number64Field, 0xabcdef1234567);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<long>(number64Field));

            instr = ParseInstruction.StoreConstant(stringField, "hi");
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<string>(stringField));

            instr = ParseInstruction.StoreConstant(boolField, true);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.GetFieldValue<bool>(boolField));
        }

        [Fact]
        public void StoreConstantInstructionStatic()
        {
            //these field id numbers are arbitrary
            RecordType simpleType = GetSimpleRecordType<SimpleRecord>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            SimpleRecord record = new SimpleRecord();
            record.Init(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            ParseInstruction instr = ParseInstruction.StoreConstant(guidField, g);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Guid);

            instr = ParseInstruction.StoreConstant(byteField, (byte)0xab);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Byte);

            instr = ParseInstruction.StoreConstant(number16Field, (short)0x3b12);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Number16);

            instr = ParseInstruction.StoreConstant(number32Field, -1002003004);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Number32);

            instr = ParseInstruction.StoreConstant(number64Field, 0xabcdef1234567);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Number64);

            instr = ParseInstruction.StoreConstant(stringField, "hi");
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.String);

            instr = ParseInstruction.StoreConstant(boolField, true);
            instr.Execute(null, record);
            Assert.Equal(instr.Constant, record.Bool);
        }



        [Fact]
        public void StoreReadInstruction()
        {
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(expectedRecord);
            RecordType simpleType = GetSimpleRecordType<SimpleRecord>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            SimpleRecord record = new SimpleRecord();
            record.Init(simpleType);

            ParseInstruction instr = ParseInstruction.StoreRead(guidField, ParseRule.Guid);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Guid, record.Guid);

            instr = ParseInstruction.StoreRead(byteField, ParseRule.FixedUInt8);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Byte, record.Byte);

            instr = ParseInstruction.StoreRead(number16Field, ParseRule.FixedInt16);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Number16, record.Number16);

            instr = ParseInstruction.StoreRead(number32Field, ParseRule.FixedInt32);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Number32, record.Number32);

            instr = ParseInstruction.StoreRead(number64Field, ParseRule.FixedInt64);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Number64, record.Number64);

            instr = ParseInstruction.StoreRead(stringField, ParseRule.UTF8String);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.String, record.String);

            instr = ParseInstruction.StoreRead(boolField, ParseRule.Boolean);
            instr.Execute(reader, record);
            Assert.Equal(expectedRecord.Bool, record.Bool);
        }



        [Fact]
        public void ParseRecordWithParseRule()
        {
            RecordType simpleRecordType = GetSimpleRecordType<SimpleRecord>();
            ParseRule rule = new ParseRule(1000, simpleRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                ParseInstruction.StoreRead(simpleRecordType.GetField("Guid"), ParseRule.Guid),
                ParseInstruction.StoreRead(simpleRecordType.GetField("Byte"), ParseRule.FixedUInt8),
                ParseInstruction.StoreRead(simpleRecordType.GetField("Number16"), ParseRule.FixedInt16),
                ParseInstruction.StoreRead(simpleRecordType.GetField("Number32"), ParseRule.FixedInt32),
                ParseInstruction.StoreRead(simpleRecordType.GetField("Number64"), ParseRule.FixedInt64)
            };
            SimpleRecord record = new SimpleRecord();
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(expectedRecord);
            rule.Parse(reader, record);

            Assert.Equal(record.Guid, expectedRecord.Guid);
            Assert.Equal(record.Byte, expectedRecord.Byte);
            Assert.Equal(record.Number16, expectedRecord.Number16);
            Assert.Equal(record.Number32, expectedRecord.Number32);
            Assert.Equal(record.Number64, expectedRecord.Number64);
        }

        [Fact]
        void RecordTableLookup()
        {
            RecordType simpleRecordType = GetSimpleRecordType<SimpleRecord>();
            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>();
            table.ItemType = simpleRecordType;
            table.PrimaryKeyField = simpleRecordType.GetField("Number16");
            table.OnParseComplete();

            SimpleRecord record = new SimpleRecord();
            record.Number16 = 12;
            table.Add(record);
            Assert.Equal(record, table.Get(12));
        }

        [Fact]
        void ReadWriteWellKnownTypes()
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            RecordWriter.Write(writer, RecordType.Boolean);
            RecordWriter.Write(writer, RecordType.Byte);
            RecordWriter.Write(writer, RecordType.Type.GetField("Name"));
            RecordWriter.Write(writer, RecordType.Type.GetField("Id"));
            ms.Position = 0;

            IStreamReader reader = new PinnedStreamReader(ms);
            RecordType booleanType = new RecordType();
            ParseRule.Type.Parse(reader, booleanType);
            Assert.Equal(RecordType.Boolean.Name, booleanType.Name);
            Assert.Equal(RecordType.Boolean.Id, booleanType.Id);

            RecordType byteType = new RecordType();
            ParseRule.Type.Parse(reader, byteType);
            Assert.Equal(RecordType.Byte.Name, byteType.Name);
            Assert.Equal(RecordType.Byte.Id, byteType.Id);

            RecordField nameField = new RecordField();
            ParseRule.Field.Parse(reader, nameField);
            Assert.Equal(RecordType.Type.GetField("Name").Name, nameField.Name);
            Assert.Equal(RecordType.Type.GetField("Name").Id, nameField.Id);

            RecordField idField = new RecordField();
            ParseRule.Field.Parse(reader, idField);
            Assert.Equal(RecordType.Type.GetField("Id").Name, idField.Name);
            Assert.Equal(RecordType.Type.GetField("Id").Id, idField.Id);
        }
    }
}
