using FastSerialization;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using Microsoft.Diagnostics.Tracing.Session;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    unsafe delegate void EventParsedFunction(TraceEventNativeMethods.EVENT_RECORD* eventRecord);

    internal class NetPerfReader : EventPipeSourceInfo, IFastSerializable, IFastSerializableVersion
    {
        public NetPerfReader(Deserializer deserializer)
        {
            _deserializer = deserializer;

#if SUPPORT_V1_V2
            // This is only here for V2 and V1.  V3+ should use the name EventTrace, it can be removed when we drop support.
            _deserializer.RegisterFactory("Microsoft.DotNet.Runtime.EventPipeFile", delegate { return this; });
#endif
            _deserializer.RegisterFactory("Trace", delegate { return this; });
            _deserializer.RegisterFactory("EventBlock", delegate { return new EventPipeEventBlock(this); });

            var entryObj = _deserializer.GetEntryObject(); // this call invokes FromStream and reads header data

            // Because we told the deserialize to use 'this' when creating a EventPipeFile, we 
            // expect the entry object to be 'this'.
            Debug.Assert(entryObj == this);
        }

        internal unsafe Guid GetRelatedActivityID(TraceEventNativeMethods.EVENT_RECORD* eventRecord)
        {
            // Recover the EventPipeEventHeader from the payload pointer and then fetch from the header.  
            EventPipeEventHeader* event_ = EventPipeEventHeader.HeaderFromPayloadPointer((byte*)eventRecord->UserData);
            return event_->RelatedActivityID;
        }

            public void Dispose()
        {
            _deserializer.Dispose();
        }

        public unsafe bool Process()
        {
            if (_fileFormatVersionNumber >= 3)
            {
                // loop through the stream until we hit a null object.  Deserialization of 
                // EventPipeEventBlocks will cause dispatch to happen.  
                // ReadObject uses registered factories and recognizes types by names, then derserializes them with FromStream
                while (_deserializer.ReadObject() != null)
                { }
            }
#if SUPPORT_V1_V2
            else
            {
                PinnedStreamReader deserializerReader = (PinnedStreamReader)_deserializer.Reader;

                while (deserializerReader.Current < _endOfEventStream)
                {
                    TraceEventNativeMethods.EVENT_RECORD* eventRecord = ReadEvent(deserializerReader);
                    if (eventRecord != null)
                    {
                        OnEventParsed?.Invoke(eventRecord);
                    }
                }
            }
#endif
            return true;
        }

        public EventParsedFunction OnEventParsed { get; set; }
        public Action<EventPipeMetadataHeader, PinnedStreamReader> OnMetadataParsed { get; set; }

        internal unsafe TraceEventNativeMethods.EVENT_RECORD* ReadEvent(PinnedStreamReader reader)
        {
            EventPipeEventHeader* eventData = (EventPipeEventHeader*)reader.GetPointer(EventPipeEventHeader.HeaderSize);
            eventData = (EventPipeEventHeader*)reader.GetPointer(eventData->TotalEventSize); // now we now the real size and get read entire event

            // Basic sanity checks.  Are the timestamps and sizes sane.  
            Debug.Assert(0 <= eventData->PayloadSize && eventData->PayloadSize <= eventData->TotalEventSize);
            Debug.Assert(0 < eventData->TotalEventSize && eventData->TotalEventSize < 0x20000);  // TODO really should be 64K but BulkSurvivingObjectRanges needs fixing.
            Debug.Assert(_fileFormatVersionNumber < 3 ||
                ((int)EventPipeEventHeader.PayloadBytes(eventData) % 4 == 0 && eventData->TotalEventSize % 4 == 0)); // ensure 4 byte alignment

            StreamLabel eventDataEnd = reader.Current.Add(eventData->TotalEventSize);

            Debug.Assert(0 <= EventPipeEventHeader.StackBytesSize(eventData) && EventPipeEventHeader.StackBytesSize(eventData) <= eventData->TotalEventSize);

            TraceEventNativeMethods.EVENT_RECORD* ret = null;
            if (eventData->IsMetadata())
            {
                int totalEventSize = eventData->TotalEventSize;
                int payloadSize = eventData->PayloadSize;

                // Note that this skip invalidates the eventData pointer, so it is important to pull any fields out we need first.  
                reader.Skip(EventPipeEventHeader.HeaderSize);

                StreamLabel metaDataEnd = reader.Current.Add(payloadSize);

                // Read in the header (The header does not include payload parameter information)
                var eventTemplate = DeserializeTemplate(reader, payloadSize, _fileFormatVersionNumber, PointerSize, ProcessId);
                _eventMetadataDictionary.Add(eventTemplate.MetaDataId, eventTemplate);

                // Record the metadata for this new event
                OnMetadataParsed(eventTemplate, reader);
                Debug.Assert(reader.Current == metaDataEnd);    // We should have read all the meta-data.  

                int stackBytes = reader.ReadInt32();
                Debug.Assert(stackBytes == 0, "Meta-data events should always have a empty stack");
            }
            else
            {
                if (_eventMetadataDictionary.TryGetValue(eventData->MetaDataId, out var metaData))
                {
                    ret = GetEventRecordForEventData(eventData, metaData);
                }
                else
                {
                    Debug.Assert(false, "Warning can't find metaData for ID " + eventData->MetaDataId.ToString("x"));
                }
            }

            reader.Goto(eventDataEnd);

            return ret;
        }

        /// <summary>
        /// Given a EventPipeEventHeader takes a EventPipeEventHeader that is specific to an event, copies it
        /// on top of the static information in its EVENT_RECORD which is specialized meta-data 
        /// and returns a pointer to it.  Thus this makes the EventPipe look like an ETW provider from
        /// the point of view of the upper level TraceEvent logic.  
        /// </summary>
        unsafe TraceEventNativeMethods.EVENT_RECORD* GetEventRecordForEventData(EventPipeEventHeader* eventData, EventPipeMetadataHeader header)
        {
            // We have already initialize all the fields of _eventRecord that do no vary from event to event. 
            // Now we only have to copy over the fields that are specific to particular event.  
            header.ThreadId = eventData->ThreadId;
            header.TimeStamp = eventData->TimeStamp;
            header.ActivityId = eventData->ActivityID;
            // EVENT_RECORD does not field for ReleatedActivityID (because it is rarely used).  See GetRelatedActivityID;
            header.SetPayload(eventData->Payload, eventData->PayloadSize);

            int stackBytesSize = EventPipeEventHeader.StackBytesSize(eventData);

            // TODO remove once .NET Core has been fixed to not emit stacks on CLR method events which are just for bookkeeping.  
            if (header.ProviderId == ClrRundownTraceEventParser.ProviderGuid ||
               (header.ProviderId == ClrTraceEventParser.ProviderGuid && (140 <= header.EventId && header.EventId <= 144 || header.EventId == 190)))     // These are various CLR method Events.  
            {
                stackBytesSize = 0;
            }
            header.SetStackBytes(EventPipeEventHeader.StackBytes(eventData), stackBytesSize);

            TraceEventNativeMethods.EVENT_RECORD* pRecord = header.GetRawRecord();

            // TODO the extra || operator is a hack because the runtime actually tries to emit events that
            // exceed this for the GC/BulkSurvivingObjectRanges (event id == 21).  We suppress that assert 
            // for now but this is a real bug in the runtime's event logging.  ETW can't handle payloads > 64K.  
            Debug.Assert(pRecord->UserDataLength == eventData->PayloadSize ||
                pRecord->EventHeader.ProviderId == ClrTraceEventParser.ProviderGuid && pRecord->EventHeader.Id == 21);

            return pRecord;
        }

        /// <summary>
        /// Creates a new MetaData instance from the serialized data at the current position of 'reader'
        /// of length 'length'.   This typically points at the PAYLOAD AREA of a meta-data events)
        /// 'fileFormatVersionNumber' is the version number of the file as a whole
        /// (since that affects the parsing of this data) and 'processID' is the process ID for the 
        /// whole stream (since it needs to be put into the EVENT_RECORD.
        /// 
        /// When this constructor returns the reader has read up to the serialized information about
        /// the parameters.  We do this because this code does not know the best representation for
        /// this parameter information and so it just lets other code handle it.  
        /// </summary>
        public EventPipeMetadataHeader DeserializeTemplate(PinnedStreamReader reader, int length, int fileFormatVersionNumber, int pointerSize, int processId)
        {
            EventPipeMetadataHeader template = new EventPipeMetadataHeader(pointerSize, processId);

            // Calculate the position of the end of the metadata blob.
            StreamLabel metadataEndLabel = reader.Current.Add(length);

            // Read the metaData
            if (3 <= fileFormatVersionNumber)
            {
                template.MetaDataId = reader.ReadInt32();
                template.ProviderName = reader.ReadNullTerminatedUnicodeString();
                template.ProviderId = GetProviderGuidFromProviderName(template.ProviderName);

                ReadEventMetaData(reader, template);
            }
#if SUPPORT_V1_V2
            else
            {
                ReadObsoleteEventMetaData(reader, fileFormatVersionNumber, template);
            }
#endif

            // Check for parameter metadata so that it can be consumed by the parser.
            template.ParameterMetadataLength = metadataEndLabel.Sub(reader.Current);
            return template;
        }

        /// <summary>
        /// Reads the meta data for information specific to one event.  
        /// </summary>
        private void ReadEventMetaData(PinnedStreamReader reader, EventPipeMetadataHeader metadataHeader)
        {
            int eventId = (ushort)reader.ReadInt32();
            metadataHeader.EventId = (ushort)eventId;
            Debug.Assert(metadataHeader.EventId == eventId);  // No truncation

            metadataHeader.EventName = reader.ReadNullTerminatedUnicodeString();

            // Deduce the opcode from the name.   
            if (metadataHeader.EventName.EndsWith("Start", StringComparison.OrdinalIgnoreCase))
            {
                metadataHeader.Opcode = (byte)TraceEventOpcode.Start;
            }
            else if (metadataHeader.EventName.EndsWith("Stop", StringComparison.OrdinalIgnoreCase))
            {
                metadataHeader.Opcode = (byte)TraceEventOpcode.Stop;
            }
            if (metadataHeader.EventName == "")
            {
                metadataHeader.EventName = null; //TraceEvent expects empty name to be canonicalized as null rather than ""
            }

            metadataHeader.Keywords = (ulong)reader.ReadInt64();

            int version = reader.ReadInt32();
            metadataHeader.Version = (byte)version;
            Debug.Assert(metadataHeader.Version == version);  // No truncation

            metadataHeader.Level = (byte)reader.ReadInt32();
            Debug.Assert(metadataHeader.Level <= 5);
        }

#if SUPPORT_V1_V2
        private void ReadObsoleteEventMetaData(PinnedStreamReader reader, int fileFormatVersionNumber, EventPipeMetadataHeader metadataHeader)
        {
            Debug.Assert(fileFormatVersionNumber < 3);

            // Old versions use the stream offset as the MetaData ID, but the reader has advanced to the payload so undo it.  
            metadataHeader.MetaDataId = ((int)reader.Current) - EventPipeEventHeader.HeaderSize;

            if (fileFormatVersionNumber == 1)
            {
                metadataHeader.ProviderId = reader.ReadGuid();
            }
            else
            {
                metadataHeader.ProviderName = reader.ReadNullTerminatedUnicodeString();
                metadataHeader.ProviderId = GetProviderGuidFromProviderName(metadataHeader.ProviderName);
            }

            var eventId = (ushort)reader.ReadInt32();
            metadataHeader.EventId = eventId;
            Debug.Assert(metadataHeader.EventId == eventId);  // No truncation

            var version = reader.ReadInt32();
            metadataHeader.Version = (byte)version;
            Debug.Assert(metadataHeader.Version == version);  // No truncation

            int metadataLength = reader.ReadInt32();
            if (0 < metadataLength)
            {
                ReadEventMetaData(reader, metadataHeader);
            }
        }
#endif

        public static Guid GetProviderGuidFromProviderName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return Guid.Empty;
            }

            // Legacy GUID lookups (events which existed before the current Guid generation conventions)
            if (name == TplEtwProviderTraceEventParser.ProviderName)
            {
                return TplEtwProviderTraceEventParser.ProviderGuid;
            }
            else if (name == ClrTraceEventParser.ProviderName)
            {
                return ClrTraceEventParser.ProviderGuid;
            }
            else if (name == ClrPrivateTraceEventParser.ProviderName)
            {
                return ClrPrivateTraceEventParser.ProviderGuid;
            }
            else if (name == ClrRundownTraceEventParser.ProviderName)
            {
                return ClrRundownTraceEventParser.ProviderGuid;
            }
            else if (name == ClrStressTraceEventParser.ProviderName)
            {
                return ClrStressTraceEventParser.ProviderGuid;
            }
            else if (name == FrameworkEventSourceTraceEventParser.ProviderName)
            {
                return FrameworkEventSourceTraceEventParser.ProviderGuid;
            }
#if SUPPORT_V1_V2
            else if (name == SampleProfilerTraceEventParser.ProviderName)
            {
                return SampleProfilerTraceEventParser.ProviderGuid;
            }
#endif
            // Hash the name according to current event source naming conventions
            else
            {
                return TraceEventProviders.GetEventSourceGuidFromName(name);
            }
        }

        /// <summary>
        /// This is the version number reader and writer (although we don't don't have a writer at the moment)
        /// It MUST be updated (as well as MinimumReaderVersion), if breaking changes have been made.
        /// If your changes are forward compatible (old readers can still read the new format) you 
        /// don't have to update the version number but it is useful to do so (while keeping MinimumReaderVersion unchanged)
        /// so that readers can quickly determine what new content is available.  
        /// </summary>
        public int Version => 3;

        /// <summary>
        /// This field is only used for writers, and this code does not have writers so it is not used.
        /// It should be set to Version unless changes since the last version are forward compatible
        /// (old readers can still read this format), in which case this shoudl be unchanged.  
        /// </summary>
        public int MinimumReaderVersion => Version;

        /// <summary>
        /// This is the smallest version that the deserializer here can read.   Currently 
        /// we are careful about backward compat so our deserializer can read anything that
        /// has ever been produced.   We may change this when we believe old writers basically
        /// no longer exist (and we can remove that support code). 
        /// </summary>
        public int MinimumVersionCanRead => 0;

        public void ToStream(Serializer serializer) => throw new InvalidOperationException("We dont ever serialize one of these in managed code so we don't need to implement ToSTream");

        public void FromStream(Deserializer deserializer)
        {
            OSVersion = new Version("0.0.0.0");
            CpuSpeedMHz = 10;
            _fileFormatVersionNumber = deserializer.VersionBeingRead;

#if SUPPORT_V1_V2
            if (deserializer.VersionBeingRead < 3)
            {
                ForwardReference reference = deserializer.ReadForwardReference();
                _endOfEventStream = deserializer.ResolveForwardReference(reference, preserveCurrent: true);
            }
#endif
            // The start time is stored as a SystemTime which is a bunch of shorts, convert to DateTime.  
            short year = deserializer.ReadInt16();
            short month = deserializer.ReadInt16();
            short dayOfWeek = deserializer.ReadInt16();
            short day = deserializer.ReadInt16();
            short hour = deserializer.ReadInt16();
            short minute = deserializer.ReadInt16();
            short second = deserializer.ReadInt16();
            short milliseconds = deserializer.ReadInt16();
            SyncTimeUTC = new DateTime(year, month, day, hour, minute, second, milliseconds, DateTimeKind.Utc);
            deserializer.Read(out SyncTimeQPC);
            deserializer.Read(out QPCFreq);

            SessionStartTimeQPC = SyncTimeQPC;

            if (3 <= deserializer.VersionBeingRead)
            {
                deserializer.Read(out PointerSize);
                deserializer.Read(out ProcessId);
                deserializer.Read(out NumberOfProcessors);
                deserializer.Read(out ExpectedCPUSamplingRate);
            }
#if SUPPORT_V1_V2
            else
            {
                ProcessId = 0; // V1 && V2 tests expect 0 for process Id
                PointerSize = 8; // V1 EventPipe only supports Linux which is x64 only.
                NumberOfProcessors = 1;
            }
#endif
        }

#if SUPPORT_V1_V2
        private StreamLabel _endOfEventStream;
#endif
        private int _fileFormatVersionNumber;
        private Dictionary<int, EventPipeMetadataHeader> _eventMetadataDictionary = new Dictionary<int, EventPipeMetadataHeader>();
        private Deserializer _deserializer;
    }

    /// <summary>
    /// An EVentPipeEventBlock represents a block of events.   It basicaly only has
    /// one field, which is the size in bytes of the block.  But when its FromStream
    /// is called, it will perform the callbacks for the events (thus deserializing
    /// it performs dispatch).  
    /// </summary>
    internal class EventPipeEventBlock : IFastSerializable
    {
        public EventPipeEventBlock(NetPerfReader reader)
        {
            _reader = reader;
        }

        public unsafe void FromStream(Deserializer deserializer)
        {
            // blockSizeInBytes INCLUDES any padding bytes to ensure alignment.  
            var blockSizeInBytes = deserializer.ReadInt();

            // after the block size comes eventual padding, we just need to skip it by jumping to the nearest aligned address
            if ((int)deserializer.Current % 4 != 0)
            {
                var nearestAlignedAddress = deserializer.Current.Add(4 - ((int)deserializer.Current % 4));
                deserializer.Goto(nearestAlignedAddress);
            }

            _startEventData = deserializer.Current;
            _endEventData = _startEventData.Add(blockSizeInBytes);
            Debug.Assert((int)_startEventData % 4 == 0 && (int)_endEventData % 4 == 0); // make sure that the data is aligned

            // Dispatch through all the events.  
            PinnedStreamReader deserializerReader = (PinnedStreamReader)deserializer.Reader;

            while (deserializerReader.Current < _endEventData)
            {
                TraceEventNativeMethods.EVENT_RECORD* eventRecord = _reader.ReadEvent(deserializerReader);
                if (eventRecord != null)
                {
                    _reader.OnEventParsed?.Invoke(eventRecord);
                }
            }

            deserializerReader.Goto(_endEventData); // go to the end of block, in case some padding was not skipped yet
        }

        public void ToStream(Serializer serializer) => throw new InvalidOperationException();

        private StreamLabel _startEventData;
        private StreamLabel _endEventData;
        private NetPerfReader _reader;
    }

    /// <summary>
    /// Private utility class.
    /// 
    /// At the start of every event from an EventPipe is a header that contains
    /// common fields like its size, threadID timestamp etc.  EventPipeEventHeader
    /// is the layout of this.  Events have two variable sized parts: the user
    /// defined fields, and the stack.   EventPipEventHeader knows how to 
    /// decode these pieces (but provides no semantics for it. 
    /// 
    /// It is not a public type, but used in low level parsing of EventPipeEventSource.  
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct EventPipeEventHeader
    {
        private int EventSize;          // Size bytes of this header and the payload and stacks if any.  does NOT encode the size of the EventSize field itself. 
        public int MetaDataId;          // a number identifying the description of this event.  
        public int ThreadId;
        public long TimeStamp;
        public Guid ActivityID;
        public Guid RelatedActivityID;
        public int PayloadSize;         // size in bytes of the user defined payload data. 
        public fixed byte Payload[4];   // Actually of variable size.  4 is used to avoid potential alignment issues.   This 4 also appears in HeaderSize below. 

        public int TotalEventSize => EventSize + sizeof(int);  // Includes the size of the EventSize field itself 

        public bool IsMetadata() => MetaDataId == 0; // 0 means that it's a metadata Id

        /// <summary>
        /// Header Size is defined to be the number of bytes before the Payload bytes.  
        /// </summary>
        public static int HeaderSize => sizeof(EventPipeEventHeader) - 4;

        public static EventPipeEventHeader* HeaderFromPayloadPointer(byte* payloadPtr)
            => (EventPipeEventHeader*)(payloadPtr - HeaderSize);

        public static int StackBytesSize(EventPipeEventHeader* header)
            => *((int*)(&header->Payload[header->PayloadSize]));

        public static byte* StackBytes(EventPipeEventHeader* header)
            => &header->Payload[header->PayloadSize + 4];

        public static byte* PayloadBytes(EventPipeEventHeader* header)
            => &header->Payload[0];
    }
}
