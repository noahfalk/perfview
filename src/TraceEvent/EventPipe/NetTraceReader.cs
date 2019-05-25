using FastSerialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    internal class NetTraceReader
    {
        static readonly byte[] s_requiredMagic = Encoding.UTF8.GetBytes("nettrace");
        static readonly int s_readerFormatVersion = 1;
        static readonly int s_formatV1HeaderSize = 48;

        IStreamReader _reader;
        RecordParserContext _context;
        
        public NetTraceReader(Stream inputStream)
        {
            _reader = new PinnedStreamReader(inputStream);
            _context = new RecordParserContext();
            RecordType eventType = _context.Types.GetOrCreate(typeof(EventRecord));
            _context.Types.GetOrCreate(typeof(EventBlock));
            _context.Tables.Add(new EventStream(this, eventType));
            RecordType metadataEventType = _context.Types.GetOrCreate(typeof(EventMetadataRecord));
            _context.Types.GetOrCreate(typeof(EventMetadataBlock));
            _context.Tables.Add(new EventMetadataStream(this, metadataEventType));
            RecordType stackType = _context.Types.GetOrCreate(typeof(StackRecord));

        }
        public NetTraceHeader Header { get; private set; }
        public Action<EventRecord> EventParsed;
        public Action<EventMetadataRecord> EventMetadataParsed;
        public Action<StackRecord> StackParsed;

        public void Process()
        {
            CheckMagic();
            Header = ReadHeader();
            object parsedObject = _context.Parse<object>(_reader, _context.ParseRules.ParseRuleBindingBlock);
            while(parsedObject != null)
            {
                parsedObject = _context.Parse<object>(_reader, _context.ParseRules.Object);
            }
        }

        public void CheckMagic()
        {
            byte[] magic = new byte[s_requiredMagic.Length];
            _reader.Read(magic, 0, magic.Length);
            if(!magic.SequenceEqual(s_requiredMagic))
            {
                ThrowReadException("Invalid stream format");
            }
        }

        public NetTraceHeader ReadHeader()
        {
            NetTraceHeader header = new NetTraceHeader();
            StreamLabel start = _reader.Current;
            header.MinCompatibleFormatVersion = _reader.ReadInt32();
            if(header.MinCompatibleFormatVersion > s_readerFormatVersion)
            {
                ThrowReadException("Nettrace version " + header.MinCompatibleFormatVersion + " not supported");
            }
            header.FormatVersion = _reader.ReadInt32();
            header.HeaderSize = _reader.ReadInt32();
            if(header.HeaderSize < s_formatV1HeaderSize)
            {
                ThrowReadException("Invalid Nettrace header size");
            }
            header.PointerSize = _reader.ReadInt32();
            header.ProcessId = _reader.ReadInt32();
            header.NumberOfProcessors = _reader.ReadInt32();
            header.SyncTimeUTC = _reader.ReadInt64();
            header.SyncTimeQPC = _reader.ReadInt64();
            header.QPCFrequency = _reader.ReadInt64();
            _reader.Skip(header.HeaderSize - _reader.Current.Sub(start));
            return header;
        }

        private void ThrowReadException(string message)
        {
            throw new SerializationException(message);
        }
    }

    internal class NetTraceHeader
    {
        public int MinCompatibleFormatVersion;
        public int FormatVersion;
        public int HeaderSize;
        public int PointerSize;
        public int ProcessId;
        public int NumberOfProcessors;
        public long SyncTimeUTC;
        public long SyncTimeQPC;
        public long QPCFrequency;
    }


    internal class EventBlock : Record
    {
        [RecordField] public EventRecord[] Events;
    }

    internal class EventRecord : Record
    {
        [RecordField] public int EventMetadataId;
        [RecordField] public long ThreadId;
        [RecordField] public long TimeStamp;
        [RecordField] public Guid ActivityID;
        [RecordField] public Guid RelatedActivityID;
        [RecordField] public byte[] Payload;
        [RecordField] public long[] Stack;
    }

    internal class EventStream : RecordTable<EventRecord>
    {
        NetTraceReader _netTraceReader;

        public EventStream(NetTraceReader reader, RecordType eventType) : base("Event", eventType)
        {
            _netTraceReader = reader;
        }

        protected override EventRecord OnPublish(EventRecord item)
        {
            _netTraceReader.EventParsed?.Invoke(item);
            return item;
        }
    }

    internal class EventMetadataBlock : Record
    {
        [RecordField] public EventMetadataRecord[] EventMetadataRecords;
    }

    internal class EventMetadataRecord : Record
    {
        [RecordField] public int Id;
        [RecordField] public int EventId;
        [RecordField] public long Keywords;
        [RecordField] public int Version;
        [RecordField] public int Level;
        [RecordField] public string ProviderName;
        [RecordField] public string EventName;
        [RecordField] public byte[] ParameterMetadata;
        [RecordField] public long[] Stack;
    }

    internal class EventMetadataStream : RecordTable<EventMetadataRecord>
    {
        NetTraceReader _netTraceReader;

        public EventMetadataStream(NetTraceReader reader, RecordType eventMetadataType) : base("EventMetadata", eventMetadataType)
        {
            _netTraceReader = reader;
        }

        protected override EventMetadataRecord OnPublish(EventMetadataRecord item)
        {
            _netTraceReader.EventMetadataParsed?.Invoke(item);
            return item;
        }
    }

    internal class StackBlock : Record
    {
        [RecordField] public StackRecord[] Stacks;
    }

    internal class StackRecord : Record
    {
        [RecordField] public int Id;
        [RecordField] public long[] StackIPs;
    }

    internal class StackStream : RecordTable<StackRecord>
    {
        NetTraceReader _netTraceReader;

        public StackStream(NetTraceReader reader, RecordType stackType) : base("Stack", stackType)
        {
            _netTraceReader = reader;
        }

        protected override StackRecord OnPublish(StackRecord item)
        {
            _netTraceReader.StackParsed?.Invoke(item);
            return item;
        }
    }
}
