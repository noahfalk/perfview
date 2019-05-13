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
            public byte Byte { get; set; }
            public short Number16;
            public int Number32;
            public long Number64;
            public string String;
            public bool Bool;

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
            }
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

        class SimpleRecordBlockHeader : Record
        {
            public short EntryCount;
        }

        class VariableSizeRecord : Record
        {
            public int Size;
            public byte Byte;
        }

        static class RecordTypes
        {
            public static RecordType SimpleRecord = GetSimpleRecordType<SimpleRecord>();
            public static RecordType SimpleRecordBlockHeader = GetSimpleRecordBlockHeaderType<SimpleRecordBlockHeader>();
            public static RecordType VariableSizeRecord = GetVariableSizeRecordType<VariableSizeRecord>();
        }

        private static RecordType GetSimpleRecordType<T>()
        {
            RecordType simpleType = new RecordType(1000, "SimpleRecord", typeof(T));
            simpleType.AddField(new RecordField(1000, "Guid", simpleType, RecordType.Guid));
            simpleType.AddField(new RecordField(1001, "Byte", simpleType, RecordType.UInt8));
            simpleType.AddField(new RecordField(1002, "Number16", simpleType, RecordType.Int16));
            simpleType.AddField(new RecordField(1003, "Number32", simpleType, RecordType.Int32));
            simpleType.AddField(new RecordField(1004, "Number64", simpleType, RecordType.Int64));
            simpleType.AddField(new RecordField(1005, "String", simpleType, RecordType.String));
            simpleType.AddField(new RecordField(1006, "Bool", simpleType, RecordType.Boolean));
            simpleType.FinishInit();
            return simpleType;
        }

        private RecordType GetComplexRecordAType<ComplexRecordType, SimpleRecordType>()
        {
            RecordType simpleType = GetSimpleRecordType<SimpleRecordType>();
            RecordType complexTypeA = new RecordType(1001, "ComplexRecordA", typeof(ComplexRecordType));
            complexTypeA.AddField(new RecordField(1007, "String", complexTypeA, RecordType.String));
            complexTypeA.AddField(new RecordField(1008, "SimpleOne", complexTypeA, simpleType));
            complexTypeA.AddField(new RecordField(1009, "SimpleTwo", complexTypeA, simpleType));
            return complexTypeA;
        }

        private static RecordType GetSimpleRecordBlockHeaderType<T>()
        {
            RecordType type = new RecordType(1002, "SimpleRecordBlockHeader", typeof(T));
            type.AddField(new RecordField(1010, "EntryCount", type, RecordType.Int16));
            type.FinishInit();
            return type;
        }

        private static RecordType GetVariableSizeRecordType<T>()
        {
            RecordType type = new RecordType(1003, "VariableSizedRecord", typeof(T));
            type.AddField(new RecordField(1011, "Size", type, RecordType.Int32));
            type.AddField(new RecordField(1012, "Byte", type, RecordType.UInt8));
            type.FinishInit();
            return type;
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
        private IStreamReader GetSimpleTypeStreamReader(int count = 1, SimpleRecord record = null)
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
                writer.Write(record.Number16);
                writer.Write(record.Number32);
                writer.Write(record.Number64 + i);
                writer.Write(record.String);
                writer.Write(record.Bool);
            }
            ms.Position = 0;
            return new PinnedStreamReader(ms);
        }



        [Fact]
        public void GetSetStaticFields()
        {
            // Confirm that we can write to strongly typed fields
            RecordType simpleType = GetSimpleRecordType<SimpleRecord>();
            SimpleRecord record = simpleType.CreateInstance<SimpleRecord>();
            ParameterExpression recordParam = Expression.Parameter(typeof(SimpleRecord).MakeByRefType(), "record");
            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            
            InstructionAction<SimpleRecord> storeGuid = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Guid"),
                simpleType.GetField("Guid").FieldType, Expression.Constant(g));
            storeGuid(null, null, ref record);
            Assert.Equal(g, record.Guid);

            InstructionAction<SimpleRecord> storeByte = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Byte"),
                simpleType.GetField("Byte").FieldType, Expression.Constant((byte)0xab));
            storeByte(null, null, ref record);
            Assert.Equal(0xab, record.Byte);

            InstructionAction<SimpleRecord> storeNumber16 = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Number16"),
                simpleType.GetField("Number16").FieldType, Expression.Constant((short)17003));
            storeNumber16(null, null, ref record);
            Assert.Equal(17003, record.Number16);

            InstructionAction<SimpleRecord> storeNumber32 = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Number32"),
                simpleType.GetField("Number32").FieldType, Expression.Constant(-19004001));
            storeNumber32(null, null, ref record);
            Assert.Equal(-19004001, record.Number32);

            InstructionAction<SimpleRecord> storeNumber64 = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Number64"),
                simpleType.GetField("Number64").FieldType, Expression.Constant(0x123456789abcdef));
            storeNumber64(null, null, ref record);
            Assert.Equal(0x123456789abcdef, record.Number64);

            InstructionAction<SimpleRecord> storeString = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("String"),
                simpleType.GetField("String").FieldType, Expression.Constant("hello"));
            storeString(null, null, ref record);
            Assert.Equal("hello", record.String);

            InstructionAction<SimpleRecord> storeBool = RecordParserCodeGen.GetStoreFieldDelegate<SimpleRecord>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, simpleType.GetField("Bool"),
                simpleType.GetField("Bool").FieldType, Expression.Constant(true) );
            storeBool(null, null, ref record);
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
            ParameterExpression recordParam = RecordParserCodeGen.RecordRefParameter;
            Record record = simpleType.CreateInstance<Record>();

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            InstructionAction<Record> storeGuid = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, guidField,
                guidField.FieldType, Expression.Constant(g));
            storeGuid(null, null, ref record);
            Assert.Equal(g, ((Guid[])record.DynamicFields[guidField.DynamicFieldTypeIndex])[guidField.DynamicFieldIndex]);
            Assert.Equal(g, record.GetFieldValue<Guid>(guidField));

            InstructionAction<Record> storeByte = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, byteField,
                byteField.FieldType, Expression.Constant((byte)0xab));
            storeByte(null, null, ref record);
            Assert.Equal(0xab, ((byte[])record.DynamicFields[byteField.DynamicFieldTypeIndex])[byteField.DynamicFieldIndex]);
            Assert.Equal(0xab, record.GetFieldValue<byte>(byteField));

            InstructionAction<Record> storeNumber16 = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, number16Field,
                number16Field.FieldType, Expression.Constant((short)17003) );
            storeNumber16(null, null, ref record);
            Assert.Equal(17003, ((short[])record.DynamicFields[number16Field.DynamicFieldTypeIndex])[number16Field.DynamicFieldIndex]);
            Assert.Equal(17003, record.GetFieldValue<short>(number16Field));

            InstructionAction<Record> storeNumber32 = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, number32Field,
                number32Field.FieldType, Expression.Constant(-19004001) );
            storeNumber32(null, null, ref record);
            Assert.Equal(-19004001, ((int[])record.DynamicFields[number32Field.DynamicFieldTypeIndex])[number32Field.DynamicFieldIndex]);
            Assert.Equal(-19004001, record.GetFieldValue<int>(number32Field));

            InstructionAction<Record> storeNumber64 = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, number64Field,
                number64Field.FieldType, Expression.Constant(0x123456789abcdef) );
            storeNumber64(null, null, ref record);
            Assert.Equal(0x123456789abcdef, ((long[])record.DynamicFields[number64Field.DynamicFieldTypeIndex])[number64Field.DynamicFieldIndex]);
            Assert.Equal(0x123456789abcdef, record.GetFieldValue<long>(number64Field));

            InstructionAction<Record> storeString = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, stringField,
                stringField.FieldType, Expression.Constant("hello") );
            storeString(null, null, ref record);
            Assert.Equal("hello", ((object[])record.DynamicFields[stringField.DynamicFieldTypeIndex])[stringField.DynamicFieldIndex]);
            Assert.Equal("hello", record.GetFieldValue<string>(stringField));

            InstructionAction<Record> storeBool = RecordParserCodeGen.GetStoreFieldDelegate<Record>(
                RecordParserCodeGen.LocalVarsParameter, simpleType, recordParam, boolField,
                boolField.FieldType, Expression.Constant(true)  );
            storeBool(null, null, ref record);
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
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(1, expectedRecord);
            RecordType simpleType = GetSimpleRecordType<SimpleRecord>();
            RecordField guidField = simpleType.GetField("Guid");
            RecordField byteField = simpleType.GetField("Byte");
            RecordField number16Field = simpleType.GetField("Number16");
            RecordField number32Field = simpleType.GetField("Number32");
            RecordField number64Field = simpleType.GetField("Number64");
            RecordField stringField = simpleType.GetField("String");
            RecordField boolField = simpleType.GetField("Bool");
            SimpleRecord record = simpleType.CreateInstance<SimpleRecord>();

            ParseInstructionStoreRead instr = new ParseInstructionStoreRead(simpleType, guidField, ParseRule.Guid);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Guid, record.Guid);

            instr = new ParseInstructionStoreRead(simpleType, byteField, ParseRule.FixedUInt8);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Byte, record.Byte);

            instr = new ParseInstructionStoreRead(simpleType, number16Field, ParseRule.FixedInt16);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number16, record.Number16);

            instr = new ParseInstructionStoreRead(simpleType, number32Field, ParseRule.FixedInt32);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number32, record.Number32);

            instr = new ParseInstructionStoreRead(simpleType, number64Field, ParseRule.FixedInt64);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Number64, record.Number64);

            instr = new ParseInstructionStoreRead(simpleType, stringField, ParseRule.UTF8String);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.String, record.String);

            instr = new ParseInstructionStoreRead(simpleType, boolField, ParseRule.Boolean);
            instr.Execute(reader, null, ref record);
            Assert.Equal(expectedRecord.Bool, record.Bool);
        }

        [Fact]
        public void StoreFieldInstruction()
        {
            RecordType complexType = GetComplexRecordAType<ComplexRecordA, SimpleRecord>();
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

        class MySimpleRecordStream : RecordTable<SimpleRecord>
        {
            public MySimpleRecordStream(bool copy = true) : base("SimpleRecords", RecordTypes.SimpleRecord)
            {
                this.copy = copy;
            }
            private bool copy;
            public List<SimpleRecord> Items = new List<SimpleRecord>();

            public override SimpleRecord Add(SimpleRecord item)
            {
                Items.Add(item);
                if (copy)
                    return (SimpleRecord)item.Clone<SimpleRecord>();
                else
                    return item;
            }
        }

        [Fact]
        public void PublishInstruction()
        {
            MySimpleRecordStream stream = new MySimpleRecordStream(false);
            SimpleRecord record = new SimpleRecord();
            ParseInstruction publishInstr = new ParseInstructionPublish(RecordTypes.SimpleRecord, stream);
            publishInstr.Execute(null, null, ref record);
            Assert.Equal(record, stream.Items[0]);


            RecordTable<SimpleRecord> table = new RecordTable<SimpleRecord>("Fooz!", RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Byte"));
            SimpleRecord record2 = new SimpleRecord();
            record2.Byte = 198;
            ParseInstruction publishInstr2 = new ParseInstructionPublish(RecordTypes.SimpleRecord, table);
            publishInstr2.Execute(null, null, ref record2);
            Assert.Equal(record2, table.Get(198));
        }

        [Fact]
        public void ParseRecordWithParseRule()
        {
            RecordType simpleRecordType = GetSimpleRecordType<SimpleRecord>();
            ParseRule rule = new ParseRule(1000, "SimpleRecord", simpleRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Guid"), ParseRule.Guid),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Byte"), ParseRule.FixedUInt8),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number16"), ParseRule.FixedInt16),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number32"), ParseRule.FixedInt32),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Number64"), ParseRule.FixedInt64),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("String"), ParseRule.UTF8String),
                new ParseInstructionStoreRead(simpleRecordType, simpleRecordType.GetField("Bool"), ParseRule.Boolean)
            };
            SimpleRecord record = new SimpleRecord();
            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(1, expectedRecord);
            record = rule.Parse(reader, record);

            Assert.Equal(record.Guid, expectedRecord.Guid);
            Assert.Equal(record.Byte, expectedRecord.Byte);
            Assert.Equal(record.Number16, expectedRecord.Number16);
            Assert.Equal(record.Number32, expectedRecord.Number32);
            Assert.Equal(record.Number64, expectedRecord.Number64);
        }

        [Fact]
        public void ParseVariableSizedRecord()
        {
            RecordType variableSizeRecordType = RecordTypes.VariableSizeRecord;
            ParseRule rule = new ParseRule(1000, "VariableSizedRecord", variableSizeRecordType);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(variableSizeRecordType, variableSizeRecordType.GetField("Size"), ParseRule.FixedInt32),
                new ParseInstructionStoreRead(variableSizeRecordType, variableSizeRecordType.GetField("Byte"), ParseRule.FixedUInt8),
                new ParseInstructionStoreField(variableSizeRecordType, RecordType.ParseRuleLocalVars.GetField("CurrentOffset"),
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
            VariableSizeRecord v = new VariableSizeRecord();

            rule.Parse(reader, v);
            Assert.Equal(100, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

            rule.Parse(reader, v);
            Assert.Equal(200, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

            rule.Parse(reader, v);
            Assert.Equal(300, reader.Current.Sub(startLabel));
            Assert.Equal(100, v.Size);
            Assert.Equal(214, v.Byte);

        }

        [Fact]
        public void IterateInstruction()
        {
            MySimpleRecordStream stream = new MySimpleRecordStream();
            ParseRule rule = new ParseRule(1000, "SimpleRecord", RecordTypes.SimpleRecord);
            rule.Instructions = new ParseInstruction[]
            {
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Guid"), ParseRule.Guid),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Byte"), ParseRule.FixedUInt8),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Number16"), ParseRule.FixedInt16),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Number32"), ParseRule.FixedInt32),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Number64"), ParseRule.FixedInt64),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("String"), ParseRule.UTF8String),
                new ParseInstructionStoreRead(RecordTypes.SimpleRecord, RecordTypes.SimpleRecord.GetField("Bool"), ParseRule.Boolean),
                new ParseInstructionPublish(RecordTypes.SimpleRecord, stream)
            };

            SimpleRecordBlockHeader header = new SimpleRecordBlockHeader();
            header.EntryCount = 10;
            ParseInstruction iterateInstr = new ParseInstructionIterateRead(RecordTypes.SimpleRecordBlockHeader, 
                rule, RecordTypes.SimpleRecordBlockHeader.GetField("EntryCount"));

            SimpleRecord expectedRecord = GetSimpleRecord();
            IStreamReader reader = GetSimpleTypeStreamReader(10, expectedRecord);
            iterateInstr.Execute(reader, null, ref header);

            Assert.Equal(10, stream.Items.Count);
            for(int i = 0; i < 10; i++)
            {
                Assert.Equal(expectedRecord.Guid, stream.Items[i].Guid);
                Assert.Equal(expectedRecord.Byte, stream.Items[i].Byte);
                Assert.Equal(expectedRecord.Number16, stream.Items[i].Number16);
                Assert.Equal(expectedRecord.Number32, stream.Items[i].Number32);
                Assert.Equal(expectedRecord.Number64 + i, stream.Items[i].Number64);
                Assert.Equal(expectedRecord.String, stream.Items[i].String);
                Assert.Equal(expectedRecord.Bool, stream.Items[i].Bool);
            }
        }

        [Fact]
        void RecordTableLookup()
        {
            RecordType simpleRecordType = GetSimpleRecordType<SimpleRecord>();
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
            RecordType foo = new RecordType(1000, "foo", typeof(Record));
            RecordType bar = new RecordType(1001, "bar", typeof(Record));
            RecordField a = new RecordField(1000, "a", foo, RecordType.Int32);
            RecordField b = new RecordField(1001, "b", bar, RecordType.String);
            RecordWriter.Write(writer, foo);
            RecordWriter.Write(writer, bar);
            RecordWriter.Write(writer, a);
            RecordWriter.Write(writer, b);
            ms.Position = 0;

            IStreamReader reader = new PinnedStreamReader(ms);
            RecordType fooType = new RecordType();
            fooType = context.ParseRules.Type.Parse(reader, fooType);
            Assert.Equal(foo.Name, fooType.Name);
            Assert.Equal(foo.Id, fooType.Id);

            RecordType barType = new RecordType();
            barType = context.ParseRules.Type.Parse(reader, barType);
            Assert.Equal(bar.Name, barType.Name);
            Assert.Equal(bar.Id, barType.Id);

            RecordField aField = new RecordField();
            aField = context.ParseRules.Field.Parse(reader, aField);
            Assert.Equal(a.Name, aField.Name);
            Assert.Equal(a.Id, aField.Id);

            RecordField bField = new RecordField();
            bField = context.ParseRules.Field.Parse(reader, bField);
            Assert.Equal(b.Name, bField.Name);
            Assert.Equal(b.Id, bField.Id);
        }

        [Fact]
        void RecordParserContextHasDefaultState()
        {
            RecordParserContext context = new RecordParserContext();
            Assert.Equal(RecordType.Boolean, context.Types.Get("Boolean"));
            Assert.Equal(RecordType.UInt8, context.Types.Get("UInt8"));
            Assert.Equal(RecordType.Int16, context.Types.Get("Int16"));
            Assert.Equal(RecordType.Int32, context.Types.Get("Int32"));
            Assert.Equal(RecordType.Int64, context.Types.Get("Int64"));
            Assert.Equal(RecordType.String, context.Types.Get("String"));
            Assert.Equal(RecordType.Guid, context.Types.Get("Guid"));
            Assert.Equal(RecordType.Type, context.Types.Get("Type"));
            Assert.Equal(RecordType.Field, context.Types.Get("Field"));

            Assert.Equal("Name", context.Fields.Get(1).Name);
            Assert.Equal("Id", context.Fields.Get(2).Name);
            Assert.Equal("Name", context.Fields.Get(3).Name);
            Assert.Equal("Id", context.Fields.Get(4).Name);
            Assert.Equal("ContainingType", context.Fields.Get(5).Name);
            Assert.Equal("FieldType", context.Fields.Get(6).Name);
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
        void TypeBindingByName()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType fooType = context.Types.GetOrCreate("Foo");
            RecordType barType = context.Types.GetOrCreate("Bar");
            RecordType bazType = context.Types.GetOrCreate("Baz");
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
        }

        [Fact]
        void TypeBindingByType()
        {
            RecordParserContext context = new RecordParserContext();
            RecordType fooType = context.Types.GetOrCreate(new RecordType(0, "Foo", typeof(SimpleRecord)));
            RecordType barType = context.Types.GetOrCreate(new RecordType(0, "Bar", typeof(SimpleRecord)));
            RecordType bazType = context.Types.GetOrCreate(new RecordType(0, "Baz", typeof(SimpleRecord)));
            IStreamReader reader = GetStreamOfTypes();

            RecordType record1 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record1, fooType);
            Assert.Equal("Foo", fooType.Name);
            Assert.Equal(1000, fooType.Id);
            Assert.Equal(typeof(SimpleRecord), fooType.ReflectionType);

            RecordType record2 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record2, barType);
            Assert.Equal("Bar", barType.Name);
            Assert.Equal(1001, barType.Id);
            Assert.Equal(typeof(SimpleRecord), barType.ReflectionType);

            RecordType record3 = context.Parse<RecordType>(reader, context.ParseRules.Type);
            Assert.Equal(record3, bazType);
            Assert.Equal("Baz", bazType.Name);
            Assert.Equal(1002, bazType.Id);
            Assert.Equal(typeof(SimpleRecord), bazType.ReflectionType);
        }

        
        [Fact]
        void SerializeInstructions()
        {
            RecordParserContext context = new RecordParserContext();
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
            }
        }
        
    }
}
