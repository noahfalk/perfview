using FastSerialization;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.EventPipe;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using SerializationException = System.Runtime.Serialization.SerializationException;

namespace TraceEventTests
{
    // Maybe someday we'll want a converter in TraceEvent proper, but for now this is 
    // just a test asset to help compare the Netperf and Nettrace formats
    public class NetTraceStreamWriter
    {
        NetPerfReader _input;
        BinaryWriter _writer;
        RecordParserContext _context;
        RecordType _eventType;
        RecordType _eventBlockType;
        RecordType _eventMetadataType;
        RecordType _eventMetadataBlockType;
        RecordTable _eventTable;
        RecordTable _eventMetadataTable;
        ParseRule _eventRule;
        ParseRule _eventBlockRule;
        ParseRule _eventMetadataRule;
        ParseRule _eventMetadataBlockRule;
        List<EventRecord> _events;
        List<EventMetadataRecord> _eventMetadataRecords;

        internal NetTraceStreamWriter(NetPerfReader input, Stream output)
        {
            _input = input;
            _writer = new BinaryWriter(output);
            _context = new RecordParserContext();
            _eventType = _context.Types.GetOrCreate(typeof(EventRecord));
            _eventBlockType = _context.Types.GetOrCreate(typeof(EventBlock));
            _eventMetadataType = _context.Types.GetOrCreate(typeof(EventMetadataRecord));
            _eventMetadataBlockType = _context.Types.GetOrCreate(typeof(EventMetadataBlock));
            _eventTable = _context.Tables.Add(new RecordTable<EventRecord>("Event", _eventType));
            _eventMetadataTable = _context.Tables.Add(new RecordTable<EventMetadataBlock>("EventMetadata", _eventMetadataType));
            _eventRule = _context.ParseRules.Add(new ParseRule(0, "Event", _eventType,
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("EventMetadataId"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("ThreadId"), _context.ParseRules.FixedInt64),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("TimeStamp"), _context.ParseRules.FixedInt64),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("ActivityID"), _context.ParseRules.Guid),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("RelatedActivityID"), _context.ParseRules.Guid),
                new ParseInstructionStoreRead(_eventType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("Stack"), _context.ParseRules.FixedInt64, _eventType.GetField("Stack").FieldType, _context.Types.ParseRuleLocalVars.GetField("TempInt32")),
                new ParseInstructionStoreRead(_eventType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventType, _eventType.GetField("Payload"), _context.ParseRules.FixedUInt8, _eventType.GetField("Payload").FieldType, _context.Types.ParseRuleLocalVars.GetField("TempInt32")),
                new ParseInstructionPublish(_eventType, _eventTable)));
            _eventMetadataRule = _context.ParseRules.Add(new ParseRule(0, "EventMetadata", _eventMetadataType,
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("Id"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("EventId"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("Keywords"), _context.ParseRules.FixedInt64),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("Version"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("Level"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("ProviderName"), _context.ParseRules.UTF8String),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("EventName"), _context.ParseRules.UTF8String),
                new ParseInstructionStoreRead(_eventMetadataType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataType, _eventMetadataType.GetField("ParameterMetadata"), _context.ParseRules.FixedUInt8, _eventMetadataType.GetField("ParameterMetadata").FieldType, _context.Types.ParseRuleLocalVars.GetField("TempInt32")),
                new ParseInstructionPublish(_eventMetadataType, _eventMetadataTable)));
            _eventBlockRule = _context.ParseRules.Add(new ParseRule(0, "EventBlock", _eventBlockType,
                new ParseInstructionStoreRead(_eventBlockType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventBlockType, _eventBlockType.GetField("Events"), _eventRule, _eventBlockType.GetField("Events").FieldType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"))));
            _eventMetadataBlockRule = _context.ParseRules.Add(new ParseRule(0, "EventMetadataBlock", _eventMetadataBlockType,
                new ParseInstructionStoreRead(_eventMetadataBlockType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"), _context.ParseRules.FixedInt32),
                new ParseInstructionStoreRead(_eventMetadataBlockType, _eventMetadataBlockType.GetField("EventMetadataRecords"), _eventMetadataRule, _eventMetadataBlockType.GetField("EventMetadataRecords").FieldType, _context.Types.ParseRuleLocalVars.GetField("TempInt32"))));
            _context.BindAllTables();
            _events = new List<EventRecord>();
            _eventMetadataRecords = new List<EventMetadataRecord>();
        }

        unsafe public void Convert()
        {
            WriteHeader();
            _writer.WriteParseRuleBindingBlock(_context);
            _writer.WriteDynamicTypeBlock(_context.Types.Values.ToArray(), _context.ParseRules);
            _writer.WriteDynamicFieldBlock(_context.Fields.Values.ToArray(), _context.ParseRules);
            _writer.WriteDynamicTableBlock(_context.Tables.Values.ToArray(), _context.ParseRules);
            _writer.WriteDynamicParseRuleBlock(_context);

            _input.OnEventParsed = OnEventParsed;
            _input.OnMetadataParsed = OnMetadataParsed;
            _input.Process();
            WriteDynamicEventBlock();
            _writer.Write(_context.ParseRules.Null.Id);
        }

        unsafe void OnEventParsed(TraceEventNativeMethods.EVENT_RECORD* eventRecord)
        {
            EventRecord record = new EventRecord();
            record.EventMetadataId = eventRecord->EventHeader.Id;
            record.ThreadId = eventRecord->EventHeader.ThreadId;
            record.TimeStamp = eventRecord->EventHeader.TimeStamp;
            record.ActivityID = eventRecord->EventHeader.ActivityId;
            record.RelatedActivityID = _input.GetRelatedActivityID(eventRecord);
            for (int i = 0; i < eventRecord->ExtendedDataCount; i++)
            {
                if (eventRecord->ExtendedData[i].ExtType == TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE64)
                {
                    var stackRecord = (TraceEventNativeMethods.EVENT_EXTENDED_ITEM_STACK_TRACE64*)eventRecord->ExtendedData[i].DataPtr;
                    int addressesCount = (eventRecord->ExtendedData[i].DataSize - sizeof(ulong)) / 8;
                    record.Stack = new long[addressesCount];
                    for (int j = 0; j < addressesCount; j++)
                    {
                        record.Stack[j] = (long)stackRecord->Address[j];
                    }
                    break;
                }
                else if (eventRecord->ExtendedData[i].ExtType == TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE32)
                {
                    var stackRecord = (TraceEventNativeMethods.EVENT_EXTENDED_ITEM_STACK_TRACE32*)eventRecord->ExtendedData[i].DataPtr;
                    int addressesCount = (eventRecord->ExtendedData[i].DataSize - sizeof(ulong)) / 4;
                    record.Stack = new long[addressesCount];
                    for (int j = 0; j < addressesCount; j++)
                    {
                        record.Stack[j] = stackRecord->Address[j];
                    }
                    break;
                }
            }
            if(eventRecord->UserDataLength != 0)
            {
                record.Payload = new byte[eventRecord->UserDataLength];
                Marshal.Copy(eventRecord->UserData, record.Payload, 0, record.Payload.Length);
            }
            _events.Add(record);
            if(_events.Count == 1000)
            {
                WriteDynamicEventBlock();
            }
        }

        void OnMetadataParsed(EventPipeMetadataHeader header, PinnedStreamReader reader)
        {
            EventMetadataRecord metadata = new EventMetadataRecord();
            metadata.Id = header.MetaDataId;
            metadata.EventId = header.EventId;
            metadata.Keywords = (long)header.Keywords;
            metadata.Version = header.Version;
            metadata.Level = header.Level;
            metadata.ProviderName = header.ProviderName;
            metadata.EventName = header.EventName;
            if (header.ParameterMetadataLength != 0)
            {
                metadata.ParameterMetadata = new byte[header.ParameterMetadataLength];
                reader.Read(metadata.ParameterMetadata, 0, metadata.ParameterMetadata.Length);  
            }
            _eventMetadataRecords.Add(metadata);
            if (_eventMetadataRecords.Count == 1000)
            {
                WriteDynamicEventMetadataBlock();
            }
        }

        void WriteHeader()
        {
            byte[] magic = Encoding.UTF8.GetBytes("nettrace");
            _writer.Write(magic);
            _writer.Write(1); // minimum reader version                           // [0  - 4)
            _writer.Write(1); // writer version                                   // [4  - 8)
            _writer.Write(48);  // header size starting from the min read version // [8  - 12)
            _writer.Write(_input.PointerSize);                                    // [12 - 16)
            _writer.Write(_input.ProcessId);                                      // [16 - 20)
            _writer.Write(_input.NumberOfProcessors);                             // [20 - 24)
            _writer.Write(_input.SyncTimeUTC.ToFileTimeUtc());                    // [24 - 32)   
            _writer.Write(_input.SyncTimeQPC);                                    // [32 - 40)
            _writer.Write(_input.QPCFreq);                                        // [40 - 48)
        }

        void WriteDynamicEventMetadataBlock()
        {
            if (_eventMetadataRecords.Count == 0)
            {
                return;
            }
            _writer.Write(_eventMetadataBlockRule.Id);
            _writer.Write(_eventMetadataRecords.Count);
            for (int i = 0; i < _eventMetadataRecords.Count; i++)
            {
                _writer.Write(_eventMetadataRecords[i].Id);
                _writer.Write(_eventMetadataRecords[i].EventId);
                _writer.Write(_eventMetadataRecords[i].Keywords);
                _writer.Write(_eventMetadataRecords[i].Version);
                _writer.Write(_eventMetadataRecords[i].Level);
                _writer.WriteUTF8String(_eventMetadataRecords[i].ProviderName);
                _writer.WriteUTF8String(_eventMetadataRecords[i].EventName);
                if(_eventMetadataRecords[i].ParameterMetadata == null)
                {
                    _writer.Write((int)0);
                }
                else
                {
                    _writer.Write((int)_eventMetadataRecords[i].ParameterMetadata.Length);
                    _writer.Write(_eventMetadataRecords[i].ParameterMetadata);
                }
            }
            _eventMetadataRecords.Clear();
        }

        void WriteDynamicEventBlock()
        {
            if(_events.Count == 0)
            {
                return;
            }
            //ensure metadata block is written before events that might refer to it
            WriteDynamicEventMetadataBlock();
            _writer.Write(_eventBlockRule.Id);
            _writer.Write(_events.Count);
            for (int i = 0; i < _events.Count; i++)
            {
                _writer.Write(_events[i].EventMetadataId);
                _writer.Write(_events[i].ThreadId);
                _writer.Write(_events[i].TimeStamp);
                _writer.Write(_events[i].ActivityID.ToByteArray());
                _writer.Write(_events[i].RelatedActivityID.ToByteArray());
                if (_events[i].Stack == null)
                {
                    _writer.Write((int)0);
                }
                else
                {
                    _writer.Write((int)_events[i].Stack.Length);
                    foreach(long ip in _events[i].Stack)
                    {
                        _writer.Write(ip);
                    }
                }
                if (_events[i].Payload == null)
                {
                    _writer.Write((int)0);
                }
                else
                {
                    _writer.Write((int)_events[i].Payload.Length);
                    _writer.Write(_events[i].Payload);
                }
            }
            _events.Clear();
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

        public static void Write(this BinaryWriter writer, RecordTable recordTable)
        {
            Debug.WriteLine("Table: " + recordTable.ToString() + " Offset: " + writer.BaseStream.Position);
            writer.Write((int)recordTable.Id);
            writer.Write((int)(recordTable.ItemType != null ? recordTable.ItemType.Id : 0));
            writer.Write((int)(recordTable.PrimaryKeyField != null ? recordTable.PrimaryKeyField.Id : 0));
            WriteUTF8String(writer, recordTable.Name);
        }

        public static void Write(this BinaryWriter writer, ParseRule parseRule, ParseRuleTable parseRules)
        {
            Debug.WriteLine("ParseRule: " + parseRule.ToString() + " Offset: " + writer.BaseStream.Position);
            writer.Write((int)parseRule.Id);
            writer.Write((int)(parseRule.ParsedType != null ? parseRule.ParsedType.Id : 0));
            WriteUTF8String(writer, parseRule.Name);
            writer.Write((int)parseRule.Instructions.Length);
            foreach(ParseInstruction i in parseRule.Instructions)
            {
                writer.WriteDynamicInstruction(i, parseRules);
            }
        }

        public static void WriteUTF8String(this BinaryWriter writer, string val)
        {
            if(val == null)
            {
                WriteVarUInt(writer, 0);
            }
            else
            {
                byte[] bytes = Encoding.UTF8.GetBytes(val);
                if (bytes.Length > ushort.MaxValue)
                {
                    throw new SerializationException("string is too long for this encoding");
                }
                WriteVarUInt(writer, (ulong)bytes.Length);
                writer.Write(bytes);
            }
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

        public static void WriteDynamicInstruction(this BinaryWriter writer, ParseInstruction instruction, ParseRuleTable parseRules)
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
                        if (instruction.ParsedType != null)
                        {
                            writer.Write(instruction.ParsedType.Id);
                            writer.Write(instruction.CountField.Id);
                        }
                        else
                        {
                            writer.Write((int)0);
                            writer.Write((int)0);
                        }
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

        public static void WriteDynamicTableBlock(this BinaryWriter writer, RecordTable[] tables, ParseRuleTable parseRules)
        {
            writer.Write(parseRules.TableBlock.Id);
            writer.Write(tables.Length);
            for (int i = 0; i < tables.Length; i++)
            {
                writer.Write(tables[i]);
            }
        }

        public static void WriteDynamicParseRuleBlock(this BinaryWriter writer, RecordParserContext context)
        {
            List<ParseRule> parseRules = new List<ParseRule>(context.ParseRules.Values.Where(rule => rule.Id > 0));
            foreach (ParseRule defaultRule in new RecordParserContext().ParseRules.Values)
            {
                parseRules.RemoveAll(p => p.Name == defaultRule.Name);
            }
            writer.WriteDynamicParseRuleBlock(parseRules.ToArray(), context.ParseRules);
        }

        public static void WriteDynamicParseRuleBlock(this BinaryWriter writer, ParseRule[] parseRuleItems, ParseRuleTable parseRules)
        {
            writer.Write(parseRules.ParseRuleBlock.Id);
            writer.Write(parseRuleItems.Length);
            for (int i = 0; i < parseRuleItems.Length; i++)
            {
                writer.Write(parseRuleItems[i], parseRules);
            }
        }


    }
}
