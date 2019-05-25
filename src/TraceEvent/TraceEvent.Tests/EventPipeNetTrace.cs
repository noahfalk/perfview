using FastSerialization;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.EventPipe;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace TraceEventTests
{
    public class EventPipeNetTrace : EventPipeTestBase
    {
        public EventPipeNetTrace(ITestOutputHelper output) : base(output)
        {
        }

        [Theory()]
        [MemberData(nameof(StreamableTestEventPipeFiles))]
        public void ConvertNetPerfToNetTrace(string eventPipeFileName)
        {
            // Initialize
            PrepareTestData();

            string eventPipeFilePath = Path.Combine(UnZippedDataDir, eventPipeFileName);

            List<EventRecord> events = new List<EventRecord>();
            List<EventMetadataRecord> eventMetadataRecords = new List<EventMetadataRecord>();
            var netPerfReader = new NetPerfReader(new Deserializer(new PinnedStreamReader(eventPipeFilePath, 0x20000), eventPipeFilePath));
            string nettraceFilePath = Path.Combine(OutputDir, Path.ChangeExtension(eventPipeFileName, ".nettrace"));
            using (FileStream netTraceStream = File.OpenWrite(nettraceFilePath))
            {
                NetTraceStreamWriter writer = new NetTraceStreamWriter(netPerfReader, netTraceStream);
                writer.Convert();
            }

            //verify header and extract events
            using (FileStream netTraceStream = File.OpenRead(nettraceFilePath))
            {
                NetTraceReader netTrace = new NetTraceReader(netTraceStream);
                netTrace.EventParsed = e => events.Add(e);
                netTrace.EventMetadataParsed = e => eventMetadataRecords.Add(e);
                netTrace.Process();

                NetTraceHeader header = netTrace.Header;
                Assert.Equal(1, header.MinCompatibleFormatVersion);
                Assert.Equal(1, header.FormatVersion);
                Assert.Equal(48, header.HeaderSize);
                Assert.Equal(netPerfReader.PointerSize, header.PointerSize);
                Assert.Equal(netPerfReader.ProcessId, header.ProcessId);
                Assert.Equal(netPerfReader.NumberOfProcessors, header.NumberOfProcessors);
                Assert.Equal(netPerfReader.SyncTimeUTC.ToFileTimeUtc(), header.SyncTimeUTC);
                Assert.Equal(netPerfReader.SyncTimeQPC, header.SyncTimeQPC);
                Assert.Equal(netPerfReader.QPCFreq, header.QPCFrequency);
            }

            // verify metadata
            var netPerfReader2 = new NetPerfReader(new Deserializer(new PinnedStreamReader(eventPipeFilePath, 0x20000), eventPipeFilePath));
            int i = 0;
            netPerfReader2.OnMetadataParsed = (e, r) =>
            {
                Assert.Equal(e.MetaDataId, eventMetadataRecords[i].Id);
                Assert.Equal(e.EventId, eventMetadataRecords[i].EventId);
                Assert.Equal((long)e.Keywords, eventMetadataRecords[i].Keywords);
                Assert.Equal(e.Version, eventMetadataRecords[i].Version);
                Assert.Equal(e.Level, eventMetadataRecords[i].Level);
                Assert.Equal(e.ProviderName, eventMetadataRecords[i].ProviderName);
                Assert.Equal(e.EventName, eventMetadataRecords[i].EventName);

                if (e.ParameterMetadataLength != 0)
                {
                    byte[] parameterMetadata = new byte[e.ParameterMetadataLength];
                    r.Read(parameterMetadata, 0, e.ParameterMetadataLength);
                    Assert.Equal(parameterMetadata.Length, eventMetadataRecords[i].ParameterMetadata.Length);
                    for (int j = 0; j < parameterMetadata.Length; j++)
                    {
                        Assert.Equal(parameterMetadata[j], eventMetadataRecords[i].ParameterMetadata[j]);
                    }
                }
                i++;
            };
            netPerfReader2.Process();
            Assert.Equal(i, eventMetadataRecords.Count);

            // verify events
            using (var traceSource = new EventPipeEventSource(eventPipeFilePath))
            {
                i = 0;
                traceSource.AllEvents += e =>
                {
                    Assert.Equal((int)e.eventID, events[i].EventMetadataId);
                    Assert.Equal(e.ThreadID, events[i].ThreadId);
                    Assert.Equal(e.TimeStampQPC, events[i].TimeStamp);
                    Assert.Equal(e.ActivityID, events[i].ActivityID);
                    Assert.Equal(e.RelatedActivityID, events[i].RelatedActivityID);
                    long[] stack = e.GetStack();
                    if (stack != null)
                    {
                        Assert.True(Enumerable.SequenceEqual(stack, events[i].Stack));
                    }
                    if (e.EventData() != null)
                    {
                        Assert.True(Enumerable.SequenceEqual(e.EventData(), events[i].Payload));
                    }
                    i++;
                };
                traceSource.Process();
                Assert.Equal(i, events.Count);
            }
        }


        class EventSizeInfo
        {
            public Tuple<int, Guid> Id;
            public string ProviderName;
            public string EventName;
            public int HeaderSize;
            public int PayloadSize;
            public int StackSize;
        }

        [Theory()]
        [MemberData(nameof(StreamableTestEventPipeFiles))]
        public void DisplayPerfStats(string eventPipeFileName)
        {
            // Initialize
            PrepareTestData();

            string eventPipeFilePath = Path.Combine(UnZippedDataDir, eventPipeFileName);

            List<EventSizeInfo> l = new List<EventSizeInfo>();

            // verify events
            using (var traceSource = new EventPipeEventSource(eventPipeFilePath))
            {
                Action<TraceEvent> handler = e =>
                {
                    EventSizeInfo esi = new EventSizeInfo();
                    esi.Id = Tuple.Create((int)e.eventID, e.ProviderGuid);
                    esi.ProviderName = e.ProviderName;
                    esi.EventName = e.EventName;
                    esi.HeaderSize = EventPipeEventHeader.HeaderSize;
                    esi.PayloadSize = e.EventData().Length + 4;
                    esi.StackSize = 4;
                    long[] stack = e.GetStack();
                    if (stack != null)
                    {
                        esi.StackSize += stack.Length * e.PointerSize;
                    }
                    l.Add(esi);
                };
                var rundown = new ClrRundownTraceEventParser(traceSource);
                rundown.All += handler;
                traceSource.Clr.All += handler;
                traceSource.Dynamic.All += handler;
                traceSource.Process();
            }

            foreach(var group in l.GroupBy(esi => esi.Id))
            {
                Output.WriteLine(string.Format("{0,30} {1,20} {2,5} {3,5} {4,5} {5,5}", group.First().ProviderName, group.First().EventName, group.Count(),
                    group.Sum(e => e.HeaderSize), group.Sum(e => e.StackSize), group.Sum(e => e.PayloadSize)));
            }
        }
    }

    public static class TraceEventExtensions
    {
        public unsafe static long[] GetStack(this TraceEvent e)
        {
            for (int i = 0; i < e.eventRecord->ExtendedDataCount; i++)
            {
                if (e.eventRecord->ExtendedData[i].ExtType == TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE64)
                {
                    var stackRecord = (TraceEventNativeMethods.EVENT_EXTENDED_ITEM_STACK_TRACE64*)e.eventRecord->ExtendedData[i].DataPtr;
                    int addressesCount = (e.eventRecord->ExtendedData[i].DataSize - sizeof(ulong)) / 8;
                    long[] ret = new long[addressesCount];
                    for (int j = 0; j < addressesCount; j++)
                    {
                        ret[j] = (long)stackRecord->Address[j];
                    }
                    return ret;
                }
                else if (e.eventRecord->ExtendedData[i].ExtType == TraceEventNativeMethods.EVENT_HEADER_EXT_TYPE_STACK_TRACE32)
                {
                    var stackRecord = (TraceEventNativeMethods.EVENT_EXTENDED_ITEM_STACK_TRACE32*)e.eventRecord->ExtendedData[i].DataPtr;
                    int addressesCount = (e.eventRecord->ExtendedData[i].DataSize - sizeof(ulong)) / 4;
                    long[] ret = new long[addressesCount];
                    for (int j = 0; j < addressesCount; j++)
                    {
                        ret[j] = stackRecord->Address[j];
                    }
                    return ret;
                }
            }
            return null;
        }
    }
}
