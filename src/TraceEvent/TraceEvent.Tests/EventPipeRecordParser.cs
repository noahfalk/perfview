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
        struct SimpleRecordType
        {
            public Guid Guid;
            public byte Byte;
            public short Number16;
            public int Number32;
            public long Number64;
            public string String;
            public bool Bool;
        }

        struct ComplexRecordTypeA
        {
            public string String;
            public SimpleRecordType SimpleOne;
            public SimpleRecordType SimpleTwo;
        }

        struct ComplexRecordTypeB
        {
            public bool Bool;
            public string String;
            public ComplexRecordTypeA ComplexAOne;
            public Guid Guid;
            public SimpleRecordType SimpleOne;
            public ComplexRecordTypeA ComplexATwo;
        }

        [Fact]
        public void GetSetStrongBackedRecordField()
        {
            // Confirm that we can write to strongly typed fields

            //these field id numbers are arbitrary
            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(Record<SimpleRecordType>));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);
            RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
            RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
            RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
            RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);
            Record<SimpleRecordType> record = new Record<SimpleRecordType>(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            Action<Record<SimpleRecordType>> storeGuid = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant(g), new RecordField[] { guidField });
            storeGuid(record);
            Assert.Equal(g, record.StronglyTypedFields.Guid);

            Action<Record<SimpleRecordType>> storeByte = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant((byte)0xab), new RecordField[] { byteField });
            storeByte(record);
            Assert.Equal(0xab, record.StronglyTypedFields.Byte);

            Action<Record<SimpleRecordType>> storeNumber16 = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant((short)17003), new RecordField[] { number16Field });
            storeNumber16(record);
            Assert.Equal(17003, record.StronglyTypedFields.Number16);

            Action<Record<SimpleRecordType>> storeNumber32 = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant(-19004001), new RecordField[] { number32Field });
            storeNumber32(record);
            Assert.Equal(-19004001, record.StronglyTypedFields.Number32);

            Action<Record<SimpleRecordType>> storeNumber64 = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant(0x123456789abcdef), new RecordField[] { number64Field });
            storeNumber64(record);
            Assert.Equal(0x123456789abcdef, record.StronglyTypedFields.Number64);

            Action<Record<SimpleRecordType>> storeString = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant("hello"), new RecordField[] { stringField });
            storeString(record);
            Assert.Equal("hello", record.StronglyTypedFields.String);

            Action<Record<SimpleRecordType>> storeBool = simpleType.GetStoreFieldDelegate<Record<SimpleRecordType>>(
                Expression.Constant(true), new RecordField[] { boolField });
            storeBool(record);
            Assert.True(record.StronglyTypedFields.Bool);
            
        }

        [Fact]
        public void GetSetWeakRecordFields()
        {
            // Confirm that we can read and write to weakly typed fields

            //these field id numbers are arbitrary
            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(Record));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            simpleType.AddField(guidField);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            simpleType.AddField(byteField);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);
            simpleType.AddField(number16Field);
            RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
            simpleType.AddField(number32Field);
            RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
            simpleType.AddField(number64Field);
            RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
            simpleType.AddField(stringField);
            RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);
            simpleType.AddField(boolField);
            Record record = new Record(simpleType);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            Action<Record> storeGuid = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant(g), new RecordField[] { guidField });
            storeGuid(record);
            Assert.Equal(g, ((Guid[])record.WeaklyTypedFields[guidField.WeakFieldTypeIndex])[guidField.WeakFieldIndex]);
            Assert.Equal(g, record.GetFieldValue<Guid>(guidField));

            Action<Record> storeByte = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant((byte)0xab), new RecordField[] { byteField });
            storeByte(record);
            Assert.Equal(0xab, ((byte[])record.WeaklyTypedFields[byteField.WeakFieldTypeIndex])[byteField.WeakFieldIndex]);
            Assert.Equal(0xab, record.GetFieldValue<byte>(byteField));

            Action<Record> storeNumber16 = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant((short)17003), new RecordField[] { number16Field });
            storeNumber16(record);
            Assert.Equal(17003, ((short[])record.WeaklyTypedFields[number16Field.WeakFieldTypeIndex])[number16Field.WeakFieldIndex]);
            Assert.Equal(17003, record.GetFieldValue<short>(number16Field));

            Action<Record> storeNumber32 = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant(-19004001), new RecordField[] { number32Field });
            storeNumber32(record);
            Assert.Equal(-19004001, ((int[])record.WeaklyTypedFields[number32Field.WeakFieldTypeIndex])[number32Field.WeakFieldIndex]);
            Assert.Equal(-19004001, record.GetFieldValue<int>(number32Field));

            Action<Record> storeNumber64 = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant(0x123456789abcdef), new RecordField[] { number64Field });
            storeNumber64(record);
            Assert.Equal(0x123456789abcdef, ((long[])record.WeaklyTypedFields[number64Field.WeakFieldTypeIndex])[number64Field.WeakFieldIndex]);
            Assert.Equal(0x123456789abcdef, record.GetFieldValue<long>(number64Field));

            Action<Record> storeString = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant("hello"), new RecordField[] { stringField });
            storeString(record);
            Assert.Equal("hello", ((object[])record.WeaklyTypedFields[stringField.WeakFieldTypeIndex])[stringField.WeakFieldIndex]);
            Assert.Equal("hello", record.GetFieldValue<string>(stringField));

            Action<Record> storeBool = simpleType.GetStoreFieldDelegate<Record>(
                Expression.Constant(true), new RecordField[] { boolField });
            storeBool(record);
            Assert.True(((bool[])record.WeaklyTypedFields[boolField.WeakFieldTypeIndex])[boolField.WeakFieldIndex]);
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
        public void StoreConstantInstructionWeak()
        {
            //these field id numbers are arbitrary
            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(Record));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            simpleType.AddField(guidField);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            simpleType.AddField(byteField);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);
            simpleType.AddField(number16Field);
            RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
            simpleType.AddField(number32Field);
            RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
            simpleType.AddField(number64Field);
            RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
            simpleType.AddField(stringField);
            RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);
            simpleType.AddField(boolField);
            Record record = new Record(simpleType);

            ParseInstruction instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            instr.StoreFieldPath = new RecordField[] { guidField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<Guid>(guidField));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = (byte)0xab;
            instr.StoreFieldPath = new RecordField[] { byteField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<byte>(byteField));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = (short)0x3b12;
            instr.StoreFieldPath = new RecordField[] { number16Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<short>(number16Field));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = -1002003004;
            instr.StoreFieldPath = new RecordField[] { number32Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<int>(number32Field));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = 0xabcdef1234567;
            instr.StoreFieldPath = new RecordField[] { number64Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<long>(number64Field));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = "hi";
            instr.StoreFieldPath = new RecordField[] { stringField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<string>(stringField));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = true;
            instr.StoreFieldPath = new RecordField[] { boolField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<bool>(boolField));
        }

        [Fact]
        public void StoreConstantInstructionStrong()
        {
            //these field id numbers are arbitrary
            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(Record<SimpleRecordType>));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            simpleType.AddField(guidField);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            simpleType.AddField(byteField);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);
            simpleType.AddField(number16Field);
            RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
            simpleType.AddField(number32Field);
            RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
            simpleType.AddField(number64Field);
            RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
            simpleType.AddField(stringField);
            RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);
            simpleType.AddField(boolField);
            Record<SimpleRecordType> record = new Record<SimpleRecordType>(simpleType);

            ParseInstruction instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            instr.StoreFieldPath = new RecordField[] { guidField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Guid);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = (byte)0xab;
            instr.StoreFieldPath = new RecordField[] { byteField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Byte);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = (short)0x3b12;
            instr.StoreFieldPath = new RecordField[] { number16Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Number16);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = -1002003004;
            instr.StoreFieldPath = new RecordField[] { number32Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Number32);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = 0xabcdef1234567;
            instr.StoreFieldPath = new RecordField[] { number64Field };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Number64);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = "hi";
            instr.StoreFieldPath = new RecordField[] { stringField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.String);

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreConstant;
            instr.InlineConstant = true;
            instr.StoreFieldPath = new RecordField[] { boolField };
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.StronglyTypedFields.Bool);
        }

        private IStreamReader GetSimpleTypeStreamReader()
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            Guid g = new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2);
            writer.Write(g.ToByteArray());
            writer.Write((byte)0xab);
            writer.Write((short)0x1234);
            writer.Write((int)0x1234567);
            writer.Write((long)0xabcedef012345);
            // string
            // bool
            ms.Position = 0;
            return new PinnedStreamReader(ms);
        }

        [Fact]
        public void StoreReadInstructionWeak()
        {
            IStreamReader reader = GetSimpleTypeStreamReader();
            //Guid g = reader.ReadGuid();
            //Assert.Equal(g, );
            //Assert.Equal(0xab, reader.ReadByte());
            //Assert.Equal(0x1234, reader.ReadInt16());

            //these field id numbers are arbitrary
            RecordType simpleType = new RecordType(2, "SimpleRecordType", typeof(Record));
            RecordField guidField = new RecordField(29, "Guid", simpleType, RecordType.Guid);
            simpleType.AddField(guidField);
            RecordField byteField = new RecordField(0, "Byte", simpleType, RecordType.Byte);
            simpleType.AddField(byteField);
            RecordField number16Field = new RecordField(71, "Number16", simpleType, RecordType.Int16);
            simpleType.AddField(number16Field);
            RecordField number32Field = new RecordField(72, "Number32", simpleType, RecordType.Int32);
            simpleType.AddField(number32Field);
            RecordField number64Field = new RecordField(73, "Number64", simpleType, RecordType.Int64);
            simpleType.AddField(number64Field);
            RecordField stringField = new RecordField(14, "String", simpleType, RecordType.String);
            simpleType.AddField(stringField);
            RecordField boolField = new RecordField(26, "Bool", simpleType, RecordType.Boolean);
            simpleType.AddField(boolField);
            Record record = new Record(simpleType);

            ParseInstruction instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { guidField };
            instr.ParseRule = ParseRule.Guid;
            instr.Execute(reader, record);
            Assert.Equal(new Guid(0x12345678, 0x9876, 0x5432, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x1, 0x2),
                record.GetFieldValue<Guid>(guidField));
            
            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { byteField };
            instr.ParseRule = ParseRule.FixedUInt8;
            instr.Execute(reader, record);
            Assert.Equal(0xab, record.GetFieldValue<byte>(byteField));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { number16Field };
            instr.ParseRule = ParseRule.FixedInt16;
            instr.Execute(reader, record);
            Assert.Equal(0x1234, record.GetFieldValue<short>(number16Field));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { number32Field };
            instr.ParseRule = ParseRule.FixedInt32;
            instr.Execute(reader, record);
            Assert.Equal(0x1234567, record.GetFieldValue<int>(number32Field));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { number64Field };
            instr.ParseRule = ParseRule.FixedInt64;
            instr.Execute(reader, record);
            Assert.Equal(0xabcedef012345, record.GetFieldValue<long>(number64Field));

            /*
            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { stringField };
            instr.ParseRule = ParseRule.Guid;
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<string>(stringField));

            instr = new ParseInstruction();
            instr.InstructionType = ParseInstructionType.FieldStoreRead;
            instr.StoreFieldPath = new RecordField[] { boolField };
            instr.ParseRule = ParseRule.Guid;
            instr.Execute(null, record);
            Assert.Equal(instr.InlineConstant, record.GetFieldValue<bool>(boolField));
            */
        }
    }
}
