using FastSerialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tracing.EventPipe
{
    internal class NetPerfReader
    {
    }

    /// <summary>
    /// An EVentPipeEventBlock represents a block of events.   It basicaly only has
    /// one field, which is the size in bytes of the block.  But when its FromStream
    /// is called, it will perform the callbacks for the events (thus deserializing
    /// it performs dispatch).  
    /// </summary>
    internal class EventPipeEventBlock : IFastSerializable, IFastSerializableVersion
    {
        public EventPipeEventBlock(EventPipeEventSource source) => _source = source;

        public unsafe void FromStream(Deserializer deserializer)
        {
            // blockSizeInBytes INCLUDES any padding bytes to ensure alignment.  
            var blockSizeInBytes = deserializer.ReadInt();

            // after the block size comes eventual padding, we just need to skip it by jumping to the nearest aligned address
            int alignmentSize = _source._blockEntryAlignment;
            if ((int)deserializer.Current % alignmentSize != 0)
            {
                var nearestAlignedAddress = deserializer.Current.Add(alignmentSize - ((int)deserializer.Current % alignmentSize));
                deserializer.Goto(nearestAlignedAddress);
            }

            if (_source._eventBlockHeaderSize > 0)
            {
                byte[] header = new byte[_source._eventBlockHeaderSize];
                deserializer.Read(header, 0, header.Length);
                ParseHeader(header);
            }

            _startEventData = deserializer.Current;
            _endEventData = _startEventData.Add(blockSizeInBytes - _source._eventBlockHeaderSize);
            Debug.Assert((int)_startEventData % alignmentSize == 0 && (int)_endEventData % alignmentSize == 0); // make sure that the data is aligned

            // Dispatch through all the events.  
            PinnedStreamReader deserializerReader = (PinnedStreamReader)deserializer.Reader;

            while (deserializerReader.Current < _endEventData)
            {
                TraceEventNativeMethods.EVENT_RECORD* eventRecord = _source.ReadEvent(deserializerReader);
                if (eventRecord != null)
                {
                    // in the code below we set sessionEndTimeQPC to be the timestamp of the last event.  
                    // Thus the new timestamp should be later, and not more than 1 day later.  
                    Debug.Assert(_source.sessionEndTimeQPC <= eventRecord->EventHeader.TimeStamp);
                    Debug.Assert(_source.sessionEndTimeQPC == 0 || eventRecord->EventHeader.TimeStamp - _source.sessionEndTimeQPC < _source._QPCFreq * 24 * 3600);

                    var traceEvent = _source.Lookup(eventRecord);
                    _source.Dispatch(traceEvent);
                    _source.sessionEndTimeQPC = eventRecord->EventHeader.TimeStamp;
                }
            }

            deserializerReader.Goto(_endEventData); // go to the end of block, in case some padding was not skipped yet
        }

        void ParseHeader(byte[] header)
        {
            if (_source._eventBlockHeaderThreadIdOffset >= 0)
            {
                _threadId = BitConverter.ToInt64(header, _source._eventBlockHeaderThreadIdOffset);
            }
            if (_source._eventBlockHeaderFlagsOffset >= 0)
            {
                _flags = BitConverter.ToInt32(header, _source._eventBlockHeaderFlagsOffset);
            }
        }

        public void ToStream(Serializer serializer) => throw new InvalidOperationException();

        public int Version => 2;

        public int MinimumVersionCanRead => Version;

        public int MinimumReaderVersion => 0;

        private StreamLabel _startEventData;
        private StreamLabel _endEventData;
        private EventPipeEventSource _source;
        private long _threadId = -1;
        private int _flags = 0;

    }
}
