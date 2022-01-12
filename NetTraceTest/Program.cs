using Microsoft.Diagnostics.Symbols;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NetTraceTest
{
    class Program
    {
        static int totalEvents = 0;

        static void Main(string[] args)
        {
            string traceFile = @"C:\users\noahfalk\Downloads\trace-serp-2021-03-31.nettrace";
            //EventPipeEventSource source = new EventPipeEventSource(traceFile);
            //source.OnEventsDropped += Source_OnEventsDropped;
            //source.AllEvents += Source_AllEvents;
            //int dropped = source.EventsLost;

            TraceLog tl = TraceLog.OpenOrConvert(traceFile);
            TraceLogEventSource tles = tl.Events.GetSource();
            using var symbolReader = new SymbolReader(TextWriter.Null) { SymbolPath = SymbolPath.MicrosoftSymbolServerPath };
            ActivityComputer activityComputer = new ActivityComputer(tles, symbolReader);
            StartStopActivityComputer ssComputer = new StartStopActivityComputer(tles, activityComputer);
            tles.Process();

            ActivityIDInfo[] observeFirst = ssComputer.m_activityIDInfo.Values.Where(a => a.ObservedBeforeStartEvent).ToArray();
            ActivityIDInfo[] observeFirstNoSS = ssComputer.m_activityIDInfo.Values.Where(a => a.ObservedBeforeStartEvent && a.SSActivity == null).ToArray();
            ActivityIDInfo[] differentThread = ssComputer.m_activityIDInfo.Values.Where(a => a.ObservedOnDifferentThread).ToArray();
            ActivityIDInfo[] total = ssComputer.m_activityIDInfo.Values.ToArray();

            int eventCount = total.Sum(ai => ai.Count);

            List<TraceEvent> events = tl.Events.Select(e => e.Clone()).ToList();

            var eventsByActivityId = events.GroupBy(e => e.ActivityID).ToArray();
            foreach(var g in eventsByActivityId)
            {
                TraceEvent[] ministamp = g.Where(e => e.ProviderName == "MiniStampEventSource").ToArray(); 
                Console.WriteLine(g.Key + " " + ministamp.Length + " " + g.Count());
            }

            foreach (var g in eventsByActivityId.Take(15))
            {
                TraceEvent[] ministamp = g.Where(e => e.ProviderName == "MiniStampEventSource").ToArray();
                if(ministamp.Length == 0)
                {
                    continue;
                }
                Console.WriteLine(g.Key + " " + ministamp.Length + " " + g.Count());
                foreach(var e in g.OrderBy(e => e.TimeStampRelativeMSec))
                {
                    Console.WriteLine("{0:n3} {1}/{2} {3}", e.TimeStampRelativeMSec, e.ProviderName, e.EventName, e.OpcodeName);
                }
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();
            }

        }

        private static void Tles_AllEvents(TraceEvent obj)
        {
            throw new NotImplementedException();
        }

        private static void Source_AllEvents(TraceEvent obj)
        {
            totalEvents++;
        }

        private static void Source_OnEventsDropped(object sender, EventPipeDroppedEventInfo e)
        {
            Console.WriteLine("Thread {0} dropped {1} events between {2:n3}-{3:n3}", 
                e.CaptureThreadId, e.CountEventsDropped, e.LastTimestampRelativeMSec, e.CurrentTimestampRelativeMSec);
        }
    }
}
