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
        
        public NetTraceReader(Stream inputStream)
        {
            _reader = new PinnedStreamReader(inputStream);
        }
        public NetTraceHeader Header { get; private set; }

        public void Process()
        {
            CheckMagic();
            Header = ReadHeader();
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
}
