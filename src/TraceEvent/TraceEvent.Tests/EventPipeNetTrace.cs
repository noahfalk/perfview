using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.EventPipe;
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
        [MemberData(nameof(TestEventPipeFiles))]
        public void ConvertNetPerfToNetTrace(string eventPipeFileName)
        {
            // Initialize
            PrepareTestData();

            string eventPipeFilePath = Path.Combine(UnZippedDataDir, eventPipeFileName);

            using (var traceSource = new EventPipeEventSource(eventPipeFilePath))
            {
                string nettraceFilePath = Path.Combine(OutputDir, Path.ChangeExtension(eventPipeFileName, ".nettrace"));
                using (FileStream netTraceStream = File.OpenWrite(nettraceFilePath))
                {
                    NetTraceStreamWriter writer = new NetTraceStreamWriter(traceSource, netTraceStream);
                    writer.Convert();
                }

                using (FileStream netTraceStream = File.OpenRead(nettraceFilePath))
                {
                    NetTraceReader netTrace = new NetTraceReader(netTraceStream);
                    netTrace.CheckMagic();
                    NetTraceHeader header = netTrace.ReadHeader();

                    Assert.Equal(1, header.MinCompatibleFormatVersion);
                    Assert.Equal(1, header.FormatVersion);
                    Assert.Equal(48, header.HeaderSize);
                    Assert.Equal(traceSource.PointerSize, header.PointerSize);
                    Assert.Equal(traceSource._processId, header.ProcessId);
                    Assert.Equal(traceSource.NumberOfProcessors, header.NumberOfProcessors);
                    Assert.Equal(traceSource._syncTimeUTC.ToFileTimeUtc(), header.SyncTimeUTC);
                    Assert.Equal(traceSource._syncTimeQPC, header.SyncTimeQPC);
                    Assert.Equal(traceSource._QPCFreq, header.QPCFrequency);
                }
            }
        }
    }
}
