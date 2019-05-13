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
        public static void Write(this BinaryWriter writer, RecordType recordType)
        {
            writer.Write((int)recordType.Id);
            WriteUTF8String(writer, recordType.Name);
        }

        public static void Write(this BinaryWriter writer, RecordField recordField)
        {
            writer.Write((int)recordField.Id);
            writer.Write((int)recordField.ContainingType.Id);
            writer.Write((int)recordField.FieldType.Id);
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
                    break;
                case ParseInstructionType.StoreRead:
                    if(instruction.ParseRule != null)
                    {
                        writer.Write(parseRules.ParseInstructionStoreRead.Id);
                        writer.Write(instruction.DestinationField.Id);
                        writer.Write(instruction.ParseRule.Id);
                    }
                    else
                    {
                        writer.Write(parseRules.ParseInstructionStoreReadDynamic.Id);
                        writer.Write(instruction.DestinationField.Id);
                        writer.Write(instruction.ParsedType.Id);
                        writer.Write(instruction.ParseRuleField.Id);
                    }
                    break;
                case ParseInstructionType.StoreReadLookup:
                    writer.Write(parseRules.ParseInstructionStoreReadLookup.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.ParseRule.Id);
                    writer.Write(instruction.LookupTable.Id);
                    break;
                case ParseInstructionType.StoreField:
                    writer.Write(parseRules.ParseInstructionStoreField.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.SourceField.Id);
                    break;
                case ParseInstructionType.StoreFieldLookup:
                    writer.Write(parseRules.ParseInstructionStoreFieldLookup.Id);
                    writer.Write(instruction.DestinationField.Id);
                    writer.Write(instruction.SourceField.Id);
                    writer.Write(instruction.LookupTable.Id);
                    break;
                case ParseInstructionType.Publish:
                    writer.Write(parseRules.ParseInstructionPublish.Id);
                    writer.Write(instruction.PublishStream.Id);
                    break;
                case ParseInstructionType.IterateRead:
                    writer.Write(parseRules.ParseInstructionIterateRead.Id);
                    writer.Write(instruction.CountField.Id);
                    writer.Write(instruction.ParseRule.Id);
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
    }
}
