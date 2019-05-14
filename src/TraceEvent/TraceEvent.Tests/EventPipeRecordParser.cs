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
            [RecordField] public Guid Guid;
            [RecordField] public byte Byte { get; set; }
            [RecordField] public short Number16;
            [RecordField] public int Number32;
            [RecordField] public long Number64;
            [RecordField] public string String;
            [RecordField] public bool Bool;

            /*
            public override Record Clone<T>()
            {
                SimpleRecord r = (SimpleRecord)base.Clone<SimpleRecord>();
                r.Guid = Guid;
                r.Byte = Byte;
                r.Number16 = Number16;
                r.Number32 = Number32;
                r.Number64 = Number64;
                r.Bool = Bool;
                return r;
            }*/
        }

        class ComplexRecordA : Record
        {
            [RecordField] public string String;
            [RecordField] public SimpleRecord SimpleOne;
            [RecordField] public SimpleRecord SimpleTwo;
        }

        class ComplexRecordB : Record
        {
            [RecordField] public bool Bool;
            [RecordField] public string String;
            [RecordField] public ComplexRecordA ComplexAOne;
            [RecordField] public Guid Guid;
            [RecordField] public SimpleRecord SimpleOne;
            [RecordField] public ComplexRecordA ComplexATwo;
        }

        class SimpleRecordBlockHeader : Record
        {
            [RecordField] public short EntryCount;
        }

        class VariableSizeRecord : Record
        {
            [RecordField] public int Size;
            [RecordField] public byte Byte;
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
        private IStreamReader GetSimpleTypeStreamReader(int count = 1, SimpleRecord record = null, bool useNumber16Index = false)
        {
            if (record == null)
            {
                record = GetSimpleRecord();
            }
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms, Encoding.UTF8);

            for (int i = 0; i < count; i++)
            {
                writer.Write(record.Guid.ToByteArray());
                writer.Write(record.Byte);
                writer.Write(useNumber16Index ? (short)i : record.Number16);
                writer.Write(record.Number32);
                writer.Write(record.Number64 + i);
                writer.Write(record.String);
                writer.Write(record.Bool);
            }
            ms.Position = 0;
            return new PinnedStreamReader(ms);
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

            /*
        [Fact]
        public void StoreConstantInstructionDynamic()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleType = GetSimpleRecordType<Record>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            Record record = simpleType.CreateInstance<Record>();

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            ParseInstructionStoreConstant instr = new ParseInstructionStoreConstant(simpleType, guidField, guidField.FieldType, g);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<Guid>(guidField));

            instr = new ParseInstructionStoreConstant(simpleType, byteField, byteField.FieldType, (byte)0xab);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<byte>(byteField));

            instr = new ParseInstructionStoreConstant(simpleType, number16Field, number16Field.FieldType, (short)0x3b12);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<short>(number16Field));

            instr = new ParseInstructionStoreConstant(simpleType, number32Field, number32Field.FieldType, -1002003004);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<int>(number32Field));

            instr = new ParseInstructionStoreConstant(simpleType, number64Field, number64Field.FieldType, 0xabcdef1234567);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<long>(number64Field));

            instr = new ParseInstructionStoreConstant(simpleType, stringField, stringField.FieldType, "hi");
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<string>(stringField));

            instr = new ParseInstructionStoreConstant(simpleType, boolField, boolField.FieldType, true);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.GetFieldValue<bool>(boolField));
        }*/

        [Fact]
        public void StoreConstantInstructionStatic()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleType = context.Types.GetOrCreate(typeof(SimpleRecord));
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            SimpleRecord record = simpleType.CreateInstance<SimpleRecord>();

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            ParseInstructionStoreConstant instr = new ParseInstructionStoreConstant(simpleType, guidField, guidField.FieldType, g);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Guid);

            instr = new ParseInstructionStoreConstant(simpleType, byteField, byteField.FieldType, (byte)0xab);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Byte);

            instr = new ParseInstructionStoreConstant(simpleType, number16Field, number16Field.FieldType, (short)0x3b12);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Number16);

            instr = new ParseInstructionStoreConstant(simpleType, number32Field, number32Field.FieldType, -1002003004);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Number32);

            instr = new ParseInstructionStoreConstant(simpleType, number64Field, number64Field.FieldType, 0xabcdef1234567);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Number64);

            instr = new ParseInstructionStoreConstant(simpleType, stringField, stringField.FieldType, "hi");
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.String);

            instr = new ParseInstructionStoreConstant(simpleType, boolField, boolField.FieldType, true);
            instr.Execute(null, null, ref record);
            Assert.Equal(instr.Constant, record.Bool);
        }



        [Fact]
        public void StoreReadInstruction()
        {
            RecordParserContext context = new RecordParserContext();
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(1, expectedRecord);
            RecordType simpleType = context.Types.GetOrCreate(typeof(SimpleRecord));
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            SimpleRecord record = simpleType.CreateInstance<SimpleRecord>();

            ParseInstructionStoreRead instr = new ParseInstructionStoreRead(simpleType, guidField, context.ParseRules.Guid);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Guid, record.Guid);

            instr = new ParseInstructionStoreRead(simpleType, byteField, context.ParseRules.FixedUInt8);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Byte, record.Byte);

            instr = new ParseInstructionStoreRead(simpleType, number16Field, context.ParseRules.FixedInt16);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number16, record.Number16);

            instr = new ParseInstructionStoreRead(simpleType, number32Field, context.ParseRules.FixedInt32);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number32, record.Number32);

            instr = new ParseInstructionStoreRead(simpleType, number64Field, context.ParseRules.FixedInt64);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number64, record.Number64);

            instr = new ParseInstructionStoreRead(simpleType, stringField, context.ParseRules.UTF8String);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.String, record.String);

            instr = new ParseInstructionStoreRead(simpleType, boolField, context.ParseRules.Boolean);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Bool, record.Bool);
        }

        [Fact]
        public void StoreFieldInstruction()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType complexType = context.Tables.Types.GetOrCreate(typeof(ComplexRecordA));
            ComplexRecordA record = new ComplexRecordA();
            record.SimpleOne = GetSimpleRecord();

            RecordField simpleOneField = complexType.GetField("SimpleOne");
            RecordField simpleTwoField = complexType.GetField("SimpleTwo");

            ParseInstruction instr = new ParseInstructionStoreField(complexType, simpleTwoField, simpleOneField);
            instr.Execute(null, null, ref record);
            Assert.Equal(record.SimpleOne, record.SimpleTwo);
            Assert.Equal(record.SimpleOne.Guid, record.SimpleTwo.Guid);
        }

        /*
        [Fact]
        public void StoreFieldLookupInstruction()
        {
            RecordType complexType = GetComplexRecordAType<ComplexRecordA, SimpleRecord>();
            RecordType simpleType = complexType.GetField("SimpleOne").FieldType;
            RecordField simpleOneField = complexType.GetField("SimpleOne");
            RecordField simpleTwoField = complexType.GetField("SimpleTwo");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            ComplexRecordA record = new ComplexRecordA();
            record.SimpleOne = new SimpleRecord();
            record.SimpleOne.Byte = 19;
            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>("Foo", simpleType, number16Field);
            SimpleRecord entry = GetSimpleRecord();
            entry.Number16 = 19;
            table.Add(entry);

            ParseInstruction instr = new ParseInstructionStoreFieldLookup(simpleTwoField, new RecordField[] { simpleOneField, byteField }, table);
            instr.Execute(null, null, ref record);
            Assert.Equal(record.SimpleTwo, entry);
            Assert.Equal(record.SimpleTwo.Guid, entry.Guid);
        }*/

        class MySimpleStream : RecordTable<SimpleRecord>
        {
            public MySimpleStream(RecordType type) : base("SimpleRecords", type) { }

            public List<SimpleRecord> Items = new List<SimpleRecord>();

            protected override SimpleRecord OnPublish(SimpleRecord item)
            {
                Items.Add(item);
                return item;
            }
        }

        [Fact]
        public void PublishInstruction()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleRecordType = context.Types.GetOrCreate(typeof(SimpleRecord));
            MySimpleStream stream = new MySimpleStream(simpleRecordType);
            SimpleRecord record = new SimpleRecord();
            ParseInstruction publishInstr = new ParseInstructionPublish(simpleRecordType, stream);
            publishInstr.Execute(null, null, ref record);
            Assert.Equal(record, stream.Items[0]);


            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>("Fooz!", simpleRecordType, simpleRecordType.GetField("Byte"));
            SimpleRecord record2 = new SimpleRecord();
            record2.Byte = 198;
            record2.Number64 = ExampleLong;
            ParseInstruction publishInstr2 = new ParseInstructionPublish(simpleRecordType, table);
            publishInstr2.Execute(null, null, ref record2);
            Assert.NotEqual(record2, table.Get(198));
            Assert.Equal(record2.Number64, table.Get(198).Number64);
            Assert.Equal(record2.Number16, table.Get(198).Number16);
        }

        [Fact]
        public void ParseRecordWithParseRule()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleRecordType = context.Types.GetOrCreate(typeof(SimpleRecord));
            ParseRule rule = new ParseRule(1000, "SimpleRecord", simpleRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Guid"), context.ParseRules.Guid),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Byte"), context.ParseRules.FixedUInt8),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number16"), context.ParseRules.FixedInt16),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number32"), context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number64"), context.ParseRules.FixedInt64),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("String"), context.ParseRules.UTF8String),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Bool"), context.ParseRules.Boolean)
            };
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(1, expectedRecord);
            SimpleRecord record = rule.Parse<SimpleRecord>(reader);

            Assert.Equal(expectedRecord.Guid, record.Guid);
            Assert.Equal(expectedRecord.Byte, record.Byte);
            Assert.Equal(expectedRecord.Number16, record.Number16);
            Assert.Equal(expectedRecord.Number32, record.Number32 );
            Assert.Equal(expectedRecord.Number64, record.Number64 );
        }

        [Fact]
        public void ParseVariableSizedRecord()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType variableSizeRecordType = context.Types.GetOrCreate(typeof(VariableSizeRecord));
            ParseRule rule = new ParseRule(1000, "VariableSizedRecord", variableSizeRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(variableSizeRecordType, variableSizeRecordType.GetField("Size"), context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(variableSizeRecordType, variableSizeRecordType.GetField("Byte"), context.ParseRules.FixedUInt8),
                new ParseInstructionStoreField(variableSizeRecordType, context.Types.ParseRuleLocalVars.GetField("CurrentOffset"),
                                               variableSizeRecordType.GetField("Size"))
            };
            MemoryStream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);
            for (int i = 0; i < 3; i++)
            {
                writer.Write(100);
                writer.Write((byte)214);
                for (int j = 5; j < 100; j++)
                {
                    writer.Write((byte)j);
                }
            }
            stream.Position = 0;
            IStreamReader reader = new MemoryStreamReader(stream.ToArray());
            StreamLabel startLabel = reader.Current;

            VariableSizeRecord v = rule.Parse<VariableSizeRecord>(reader);
            Assert.Equal(100, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

            v = rule.Parse<VariableSizeRecord>(reader);
            Assert.Equal(200, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

            v = rule.Parse<VariableSizeRecord>(reader);
            Assert.Equal(300, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

        }

        [Fact]
        public void IterateInstruction()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleRecordType = context.Types.GetOrCreate(typeof(SimpleRecord));
            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>("SimpleRecordsName", simpleRecordType, simpleRecordType.GetField("Number16"));
            ParseRule rule = new ParseRule(1000, "SimpleRecord", simpleRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Guid"), context.ParseRules.Guid),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Byte"), context.ParseRules.FixedUInt8),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number16"), context.ParseRules.FixedInt16),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number32"), context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number64"), context.ParseRules.FixedInt64),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("String"), context.ParseRules.UTF8String),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Bool"), context.ParseRules.Boolean),
                new ParseInstructionPublish(simpleRecordType, table)
            };

            SimpleRecordBlockHeader header = new SimpleRecordBlockHeader();
            header.EntryCount = 10;
            RecordType blockHeaderType = context.Types.GetOrCreate(typeof(SimpleRecordBlockHeader));
            ParseInstruction iterateInstr = new ParseInstructionIterateRead(blockHeaderType, 
                rule, blockHeaderType.GetField("EntryCount"));

            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(10, expectedRecord, true);
            iterateInstr.Execute(reader, null, ref header);

            Assert.Equal(10, table.Count);
            foreach(SimpleRecord record in table.Values)
            {
                int i = record.Number16;
                Assert.Equal(expectedRecord.Guid, record.Guid);
                Assert.Equal(expectedRecord.Byte, record.Byte);
                Assert.Equal(i, record.Number16);
                Assert.Equal(expectedRecord.Number32, record.Number32);
                Assert.Equal(expectedRecord.Number64 + i, record.Number64);
                Assert.Equal(expectedRecord.String, record.String);
                Assert.Equal(expectedRecord.Bool, record.Bool);
            }
        }

        [Fact]
        void RecordTableLookup()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleRecordType = context.Types.GetOrCreate(typeof(SimpleRecord));
            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>("foo", simpleRecordType, simpleRecordType.GetField("Number16"));

            SimpleRecord record = new SimpleRecord();
            record.Number16 = 12;
            table.Add(record);
            Assert.Equal(record, table.Get(12));
        }

        [Fact]
        void ReadWriteWellKnownTypes()
        {
            RecordParserContext context = new RecordParserContext();
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            RecordType foo = context.Types.Add(new RecordType(1000, "foo", typeof(Record)));
            RecordType bar = context.Types.Add(new RecordType(1001, "bar", typeof(Record)));
            RecordField a = context.Fields.Add(new RecordField(1000, "a", foo, context.Types.Int32));
            RecordField b = context.Fields.Add(new RecordField(1001, "b", bar, context.Types.String));
            context.BindAllTables();
            RecordWriter.Write(writer, foo);
            RecordWriter.Write(writer, bar);
            RecordWriter.Write(writer, a);
            RecordWriter.Write(writer, b);
            ms.Position = 0;

            IStreamReader reader = new PinnedStreamReader(ms);
            RecordType fooType = context.ParseRules.Type.Parse<RecordType>(reader);
            Assert.Equal(foo.Name, fooType.Name);
            Assert.Equal(foo.Id, fooType.Id);

            RecordType barType = context.ParseRules.Type.Parse<RecordType>(reader);
            Assert.Equal(bar.Name, barType.Name);
            Assert.Equal(bar.Id, barType.Id);

            RecordField aField = context.ParseRules.Field.Parse<RecordField>(reader);
            Assert.Equal(a.Name, aField.Name);
            Assert.Equal(a.Id, aField.Id);

            RecordField bField = context.ParseRules.Field.Parse<RecordField>(reader);
            Assert.Equal(b.Name, bField.Name);
            Assert.Equal(b.Id, bField.Id);
        }

        [Fact]
        void RecordParserContextHasDefaultState()
        {
            RecordParserContext context = new RecordParserContext();

            Assert.Equal("Boolean", context.Types.Boolean.Name);
            Assert.Equal("Byte", context.Types.Byte.Name);
            Assert.Equal("Int16", context.Types.Int16.Name);
            Assert.Equal("Int32", context.Types.Int32.Name);
            Assert.Equal("Int64", context.Types.Int64.Name);
            Assert.Equal("String", context.Types.String.Name);
            Assert.Equal("Guid", context.Types.Guid.Name);
            Assert.Equal("Type", context.Types.Type.Name);
            Assert.Equal("Field", context.Types.Field.Name);
            Assert.Equal("Table", context.Types.Table.Name);
            Assert.Equal("ParseRule", context.Types.ParseRule.Name);
            Assert.Equal("ParseRuleLocalVars", context.Types.ParseRuleLocalVars.Name);
            Assert.Equal("RecordBlock", context.Types.RecordBlock.Name);
            Assert.Equal("ParseInstruction", context.Types.ParseInstruction.Name);

            foreach (RecordType type in context.Types.Values)
            {
                foreach (RecordField field in type.GetFields())
                {
                    Assert.Equal(type, field.ContainingType);
                    Assert.Equal(field, type.GetField(field.Name));
                    Assert.Equal(field, context.Fields.Get((Tuple<string,RecordType>)field.GetBindingKey()));
                }
            }

            foreach (RecordType type in new RecordType[] { context.Types.Type,
                                                          context.Types.Field,
                                                          context.Types.Table,
                                                          context.Types.ParseRule})
            {
                RecordField nameField = type.GetField("Name");
                Assert.Equal("Name", nameField.Name);
                Assert.Equal(context.Types.String, nameField.FieldType);

                RecordField idField = type.GetField("Id");
                Assert.Equal("Id", idField.Name);
                Assert.Equal(context.Types.Int32, idField.FieldType);
            }

            /*
        Field.AddField(new RecordField(5, "ContainingType", Field, Type));
        Field.AddField(new RecordField(6, "FieldType", Field, Type));
        Table.AddField(new RecordField(12, "ItemType", Table, Type));
        Table.AddField(new RecordField(13, "PrimaryKeyField", Table, Field));
        ParseRule.AddField(new RecordField(15, "ParsedType", ParseRule, Type));
        ParseRuleLocalVars.AddField(new RecordField(16, "TempInt32", ParseRuleLocalVars, Int32));
        ParseRuleLocalVars.AddField(new RecordField(99000, "TempParseRule", ParseRuleLocalVars, ParseRule));
        ParseRuleLocalVars.AddField(new RecordField(99001, "This", ParseRuleLocalVars, null));
        ParseRuleLocalVars.AddField(new RecordField(99002, "Null", ParseRuleLocalVars, null));
        ParseRuleLocalVars.AddField(new RecordField(17, "CurrentOffset", ParseRuleLocalVars, Int32));
        RecordBlock.AddField(new RecordField(18, "EntryCount", RecordBlock, Int32));
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
        ParseInstruction.AddField(new RecordField(26, "PublishStream", ParseInstruction, Table));*/
        }
        
        IStreamReader GetStreamOfTypes()
        {
            MemoryStream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);
            RecordWriter.Write(writer, new RecordType(1000, "Foo", typeof(Record)));
            RecordWriter.Write(writer, new RecordType(1001, "Bar", typeof(Record)));
            RecordWriter.Write(writer, new RecordType(1002, "Baz", typeof(Record)));
            stream.Position = 0;
            return new MemoryStreamReader(stream.ToArray());
        }

        [Fact]
        void SimpleParsing()
        {
            RecordParserContext context = new RecordParserContext();
            IStreamReader reader = GetStreamOfTypes();
            RecordType record1 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal("Foo", record1.Name);
            Assert.Equal(1000, record1.Id);
            RecordType record2 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal("Bar", record2.Name);
            Assert.Equal(1001, record2.Id);
            RecordType record3 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal("Baz", record3.Name);
            Assert.Equal(1002, record3.Id);
        }

        [Fact]
        void CopyRecord()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType simpleType = context.Types.GetOrCreate(typeof(SimpleRecord));
            SimpleRecord record = GetSimpleRecord();
            SimpleRecord record2 = simpleType.CreateInstance<SimpleRecord>();
            simpleType.Copy(record, record2);
            Assert.Equal(record.Byte, record2.Byte);
        }

        /*
        [Fact]
        void TypeBindingByName()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType fooType = context.Types.Add(new RecordType(0, "Foo", typeof(Record)));
            RecordType barType = context.Types.Add(new RecordType(0, "Bar", typeof(Record)));
            RecordType bazType = context.Types.Add(new RecordType(0, "Baz", typeof(Record)));
            IStreamReader reader = GetStreamOfTypes();

            RecordType record1 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record1, fooType);
            Assert.Equal("Foo", fooType.Name);
            Assert.Equal(1000, fooType.Id);

            RecordType record2 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record2, barType);
            Assert.Equal("Bar", barType.Name);
            Assert.Equal(1001, barType.Id);

            RecordType record3 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record3, bazType);
            Assert.Equal("Baz", bazType.Name);
            Assert.Equal(1002, bazType.Id);
        }*/
        
        [Fact]
        void SerializeInstructions()
        {
            RecordParserContext context = new RecordParserContext();
            context.BindAllTables();
            List<ParseInstruction> instructions = new List<ParseInstruction>();
            foreach (ParseRule rule in context.ParseRules.Values)
            {
                foreach(ParseInstruction instruction in rule.Instructions)
                {
                    instructions.Add(instruction);
                }
            }

            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            foreach (ParseInstruction instruction in instructions)
            {
                writer.WriteInstruction(instruction, context.ParseRules);
            }
            ms.Position = 0;
            MemoryStreamReader reader = new MemoryStreamReader(ms.ToArray());

            for(int i = 0; i < instructions.Count; i++)
            {
                var instr = context.Parse<ParseInstruction>(reader, context.ParseRules.ParseInstruction);
                var orig = instructions[i];
                Assert.Equal(orig.Constant, instr.Constant);
                Assert.Equal(orig.ConstantType, instr.ConstantType);
                Assert.Equal(orig.CountField, instr.CountField);
                Assert.Equal(orig.DestinationField, instr.DestinationField);
                Assert.Equal(orig.InstructionType, instr.InstructionType);
                Assert.Equal(orig.LookupTable, instr.LookupTable);
                Assert.Equal(orig.ParsedType, instr.ParsedType);
                Assert.Equal(orig.ParseRule, instr.ParseRule);
                Assert.Equal(orig.ParseRuleField, instr.ParseRuleField);
                Assert.Equal(orig.PublishStream, instr.PublishStream);
                Assert.Equal(orig.SourceField, instr.SourceField);
                Assert.Equal(orig.ThisType, instr.ThisType);
            }
        }

        [Fact]
        void SerializeParseRuleBindings()
        {
            RecordParserContext context = new RecordParserContext();
            context.BindAllTables();

            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            writer.WriteParseRuleBindingBlock(context);

            ms.Position = 0;
            MemoryStreamReader reader = new MemoryStreamReader(ms.ToArray());
            RecordParserContext context2 = new RecordParserContext();
            RecordBlock block = context2.Parse<RecordBlock>(reader, context2.ParseRules.ParseRuleBindingBlock);

            Assert.Equal(context.ParseRules.Count, block.RecordCount);
            foreach(ParseRule p in context2.ParseRules.Values)
            {
                Assert.True(p.Id > 0);
            }
        }

        [Fact]
        void SerializeTypeBlock()
        {
            RecordParserContext context = new RecordParserContext();
            context.Types.GetOrCreate(typeof(SimpleRecord));
            context.Types.GetOrCreate(typeof(ComplexRecordA));
            context.Types.GetOrCreate(typeof(ComplexRecordB));
            context.BindAllTables();

            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            writer.WriteParseRuleBindingBlock(context);
            writer.WriteDynamicTypeBlock(context.Types.Values.ToArray(), context.ParseRules);

            ms.Position = 0;
            MemoryStreamReader reader = new MemoryStreamReader(ms.ToArray());
            RecordParserContext context2 = new RecordParserContext();
            RecordBlock parseRuleBindingBlock = context2.Parse<RecordBlock>(reader, context2.ParseRules.ParseRuleBindingBlock);
            RecordBlock typeBlock = context2.Parse<RecordBlock>(reader, context2.ParseRules.Object);

            Assert.Equal(context.Types.Count, typeBlock.RecordCount);
            foreach(var t in context.Types.Values)
            {
                Assert.Equal(t.Id, context2.Types.Get(t.Id).Id);
                Assert.Equal(t.Name, context2.Types.Get(t.Id).Name);
            }
        }

        [Fact]
        void SerializeFieldBlock()
        {
            RecordParserContext context = new RecordParserContext();
            context.Types.GetOrCreate(typeof(SimpleRecord));
            context.Types.GetOrCreate(typeof(ComplexRecordA));
            context.Types.GetOrCreate(typeof(ComplexRecordB));
            context.BindAllTables();

            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            writer.WriteParseRuleBindingBlock(context);
            writer.WriteDynamicTypeBlock(context.Types.Values.ToArray(), context.ParseRules);
            writer.WriteDynamicFieldBlock(context.Fields.Values.ToArray(), context.ParseRules);

            ms.Position = 0;
            MemoryStreamReader reader = new MemoryStreamReader(ms.ToArray());
            RecordParserContext context2 = new RecordParserContext();
            RecordBlock parseRuleBindingBlock = context2.Parse<RecordBlock>(reader, context2.ParseRules.ParseRuleBindingBlock);
            RecordBlock typeBlock = context2.Parse<RecordBlock>(reader, context2.ParseRules.Object);
            RecordBlock fieldBlock = context2.Parse<RecordBlock>(reader, context2.ParseRules.Object);

            Assert.Equal(context.Fields.Count, typeBlock.RecordCount);
            foreach (var f in context.Fields.Values)
            {
                Assert.Equal(f.Id, context2.Fields.Get(f.Id).Id);
                Assert.Equal(f.Name, context2.Fields.Get(f.Id).Name);
                Assert.Equal(f.ContainingType.Id, context2.Fields.Get(f.Id).ContainingType.Id);
                Assert.Equal(f.FieldType.Id, context2.Fields.Get(f.Id).FieldType.Id);
            }
        }
    }
}
