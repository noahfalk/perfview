using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.EventPipe;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace TraceEventTests
{
    // Maybe someday we'll want a converter in TraceEvent proper, but for now this is 
    // just a test asset to help compare the Netperf and Nettrace formats
    public class NetTraceStreamWriter
    {
        EventPipeEventSource _source;
        BinaryWriter _writer;
        bool _headerWritten;

        public NetTraceStreamWriter(EventPipeEventSource input, Stream output)
        {
            _source = input;
            _writer = new BinaryWriter(output);
        }

        public void Convert()
        {
            _source.AllEvents += _source_AllEvents;
            _source.Process();
        }

        void _source_AllEvents(TraceEvent obj)
        {
            if(!_headerWritten)
            {
                WriteHeader();
                _headerWritten = true;
            }
        }

        void WriteHeader()
        {
            byte[] magic = Encoding.UTF8.GetBytes("nettrace");
            _writer.Write(magic);
            _writer.Write(1); // minimum reader version                           // [0  - 4)
            _writer.Write(1); // writer version                                   // [4  - 8)
            _writer.Write(48);  // header size starting from the min read version // [8  - 12)
            _writer.Write(_source.PointerSize);                                   // [12 - 16)
            _writer.Write(_source._processId);                                    // [16 - 20)
            _writer.Write(_source.NumberOfProcessors);                            // [20 - 24)
            _writer.Write(_source._syncTimeUTC.ToFileTimeUtc());                  // [24 - 32)   
            _writer.Write(_source._syncTimeQPC);                                  // [32 - 40)
            _writer.Write(_source._QPCFreq);                                      // [40 - 48)
        }
    }

    /// <summary>
    /// Writes well known Record types in a format that can be deserialized using the well known ParseRules
    /// </summary>
    internal static class RecordWriter
    {
        public static void BindAllTables(this RecordParserContext context)
        {
            int i = 0;
            foreach (RecordType t in context.Types.Values)
            {
                if(t.Id == 0)
                    context.Types.Bind(t, ++i);
            }
            i = 0;
            foreach (RecordField f in context.Fields.Values)
            {
                if(f.Id == 0)
                    context.Fields.Bind(f, ++i);
            }
            i = 0;
            foreach (RecordTable t in context.Tables.Values)
            {
                if(t.Id == 0)
                    context.Tables.Bind(t, ++i);
            }
            i = 0;
            foreach (ParseRule p in context.ParseRules.Values)
            {
                if(p.Id == 0)
                    context.ParseRules.Bind(p, ++i);
            }
        }

        public static void Write(this BinaryWriter writer, RecordType recordType)
        {
            writer.Write((int)recordType.Id);
            WriteUTF8String(writer, recordType.Name);
        }

        public static void Write(this BinaryWriter writer, RecordField recordField)
        {
            Debug.WriteLine("Field: " + recordField.ToString() + " Offset: " + writer.BaseStream.Position);
            writer.Write((int)recordField.Id);
            writer.Write((int)(recordField.ContainingType != null ? recordField.ContainingType.Id : 0));
            writer.Write((int)(recordField.FieldType != null ? recordField.FieldType.Id : 0));
            WriteUTF8String(writer, recordField.Name);
        }

        public static void WriteUTF8String(this BinaryWriter writer, string val)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(val);
            if (bytes.Length > ushort.MaxValue)
            {
                throw new SerializationException("string is too long for this encoding");
            }
            WriteVarUInt(writer, (ulong)bytes.Length);
            writer.Write(bytes);
        }

        public static void WriteVarUInt(this BinaryWriter writer, ulong val)
        {
            while (val >= 0x80)
            {
                writer.Write((byte)(val & 0x7F) | 0x80);
                val >>= 7;
            }
            writer.Write((byte)val);
        }

        public static void WriteInstruction(this BinaryWriter writer, ParseInstruction instruction, ParseRuleTable parseRules)
        {
            Debug.WriteLine("Instruction: " + instruction.ToString() + " Offset: " + writer.BaseStream.Position);
            switch (instruction.InstructionType)
            {
                case ParseInstructionType.StoreConstant:
                    writer.Write(parseRules.ParseInstructionStoreConstant.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.WriteConstant(instruction.ConstantType, instruction.Constant, parseRules);
                    writer.Write(instruction.ThisType.Id);
                    break;
                case ParseInstructionType.StoreRead:
                    if(instruction.ParseRule != null)
                    {
                        writer.Write(parseRules.ParseInstructionStoreRead.Id);
                        writer.Write(instruction.DestinationField.Id);
                        writer.Write(instruction.ParseRule.Id);
                        writer.Write(instruction.ThisType.Id);
                    }
                    else
                    {
                        writer.Write(parseRules.ParseInstructionStoreReadDynamic.Id);
                        writer.Write(instruction.DestinationField.Id);
                        writer.Write(instruction.ParsedType.Id);
                        writer.Write(instruction.ParseRuleField.Id);
                        writer.Write(instruction.ThisType.Id);
                    }
                    break;
                case ParseInstructionType.StoreReadLookup:
                    writer.Write(parseRules.ParseInstructionStoreReadLookup.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.ParseRule.Id);
                    writer.Write(instruction.LookupTable.Id);
                    writer.Write(instruction.ThisType.Id);
                    break;
                case ParseInstructionType.StoreField:
                    writer.Write(parseRules.ParseInstructionStoreField.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.SourceField.Id);
                    writer.Write(instruction.ThisType.Id);
                    break;
                case ParseInstructionType.StoreFieldLookup:
                    writer.Write(parseRules.ParseInstructionStoreFieldLookup.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.SourceField.Id);
                    writer.Write(instruction.LookupTable.Id);
                    writer.Write(instruction.ThisType.Id);
                    break;
                case ParseInstructionType.Publish:
                    writer.Write(parseRules.ParseInstructionPublish.Id);
                    writer.Write(instruction.PublishStream.Id);
                    writer.Write(instruction.ThisType.Id);
                    break;
                case ParseInstructionType.IterateRead:
                    writer.Write(parseRules.ParseInstructionIterateRead.Id);
                    writer.Write(instruction.CountField.Id);
                    writer.Write(instruction.ParseRule.Id);
                    writer.Write(instruction.ThisType.Id);
                    break;
                default:
                    throw new SerializationException("Invalid ParseInstructionType");
            }
        }

        public static void WriteConstant(this BinaryWriter writer, RecordType constantRecordType, object constant, ParseRuleTable parseRules)
        {
            Debug.WriteLine("Writing constant type: " + constantRecordType.Id + " at " + writer.BaseStream.Position);
            writer.Write(constantRecordType.Id);
            Type constantType = constant.GetType();
            if(constantType.IsEnum)
            {
                constantType = constantType.GetEnumUnderlyingType();
            }
            if (constantType == typeof(bool))
            {
                writer.Write(parseRules.Boolean.Id);
                writer.Write((byte)((bool)constant ? 1 : 0));
            }
            else if (constantType == typeof(byte))
            {
                writer.Write(parseRules.FixedUInt8.Id);
                writer.Write((byte)constant);
            }
            else if (constantType == typeof(short))
            {
                writer.Write(parseRules.FixedInt16.Id);
                writer.Write((short)constant);
            }
            else if (constantType == typeof(int))
            {
                writer.Write(parseRules.FixedInt32.Id);
                writer.Write((int)constant);
            }
            else if(constantType == typeof(long))
            {
                writer.Write(parseRules.FixedInt64.Id);
                writer.Write((long)constant);
            }
            else if(constantType == typeof(Guid))
            {
                writer.Write(parseRules.Guid.Id);
                writer.Write(((Guid)constant).ToByteArray());
            }
            else if(constantType == typeof(string))
            {
                writer.Write(parseRules.UTF8String.Id);
                writer.Write((string)constant);
            }
            else
            {
                throw new NotImplementedException("Writing constant of type " + constantType.ToString() + " NYI");
            }
        }

        public static void WriteParseRuleBinding(this BinaryWriter writer, ParseRule ruleBinding)
        {
            writer.Write(ruleBinding.Id);
            writer.WriteUTF8String(ruleBinding.Name);
        }

        public static void WriteParseRuleBindingBlock(this BinaryWriter writer, ParseRule[] rules)
        {
            writer.Write(rules.Length);
            for(int i = 0; i < rules.Length; i++)
            {
                writer.WriteParseRuleBinding(rules[i]);
            }
        }

        public static void WriteParseRuleBindingBlock(this BinaryWriter writer, RecordParserContext context)
        {
            List<ParseRule> bindings = new List<ParseRule>();
            foreach (ParseRule defaultRule in new RecordParserContext().ParseRules.Values)
            {
                ParseRule binding = context.ParseRules.Get(defaultRule.Name);
                if(binding.Id != 0)
                {
                    bindings.Add(binding);
                }
            }
            writer.WriteParseRuleBindingBlock(bindings.ToArray());
        }

        public static void WriteDynamicTypeBlock(this BinaryWriter writer, RecordType[] types, ParseRuleTable parseRules)
        {
            writer.Write(parseRules.TypeBlock.Id);
            writer.Write(types.Length);
            for(int i = 0; i < types.Length; i++)
            {
                writer.Write(types[i]);
            }
        }

        public static void WriteDynamicFieldBlock(this BinaryWriter writer, RecordField[] fields, ParseRuleTable parseRules)
        {
            writer.Write(parseRules.FieldBlock.Id);
            writer.Write(fields.Length);
            for (int i = 0; i < fields.Length; i++)
            {
                writer.Write(fields[i]);
            }
        }
    }
}
