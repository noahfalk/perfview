using Microsoft.Diagnostics.Tracing.Etlx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Microsoft.Diagnostics.Tracing.Parsers.DynamicTraceEventData;

namespace Microsoft.Diagnostics.Tracing
{
    public class DistributedTraceIdComputer
    {
        TraceLogEventSource _source;
        StartStopActivityComputer _startStopActivityComputer;
        GrowableArray<string> _traceIds;
        GrowableArray<string> _spanIds;
        GrowableArray<string> _parentSpanIds;

        public DistributedTraceIdComputer(TraceLogEventSource source, StartStopActivityComputer startStopActivityComputer)
        {
            _source = source;
            _startStopActivityComputer = startStopActivityComputer;
            _traceIds = new GrowableArray<string>();
            _spanIds = new GrowableArray<string>();
            _parentSpanIds = new GrowableArray<string>();
            _startStopActivityComputer.Stop += ClearActivityIDs;

            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity/Start", OnDiagSourceActivityStart);
            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity/Stop", OnDiagSourceActivityStop);
            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity1/Start", OnDiagSourceActivityStart);
            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity1/Stop", OnDiagSourceActivityStop);
            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity2/Start", OnDiagSourceActivityStart);
            _source.Dynamic.AddCallbackForProviderEvent("Microsoft-Diagnostics-DiagnosticSource", "Activity2/Stop", OnDiagSourceActivityStop);
        }

        private void ClearActivityIDs(StartStopActivity activity, TraceEvent traceEvent)
        {
            _traceIds.Set((int)activity.Index, null);
            _spanIds.Set((int)activity.Index, null);
            _parentSpanIds.Set((int)activity.Index, null);
        }

        private void SetActivityIDs(StartStopActivity activity, string systemDiagnosticsActivityId)
        {
            if(TryParseW3CId(systemDiagnosticsActivityId, out string traceId, out string spanId))
            {
                _traceIds.Set((int)activity.Index, traceId);
                _spanIds.Set((int)activity.Index, spanId);
            }
        }

        private void SetParentSpanID(StartStopActivity activity, string parentSpanId)
        {
            _parentSpanIds.Set((int)activity.Index, parentSpanId);
        }

        private void OnDiagSourceActivityStart(TraceEvent traceEvent)
        {
            StartStopActivity cur = _startStopActivityComputer.GetCurrentStartStopActivity(traceEvent.Thread(), traceEvent);
            if(cur != null)
            {
                object arguments = traceEvent.PayloadByName("Arguments");
                if(arguments is StructValue[] svArguments)
                {
                    foreach(StructValue sv in svArguments)
                    {
                        if((sv["Key"] as string) == "ActivityId")
                        {
                            if(sv["Value"] is string activityId)
                            {
                                SetActivityIDs(cur, activityId);
                            }
                        }
                        else if ((sv["Key"] as string) == "Id")
                        {
                            if (sv["Value"] is string activityId)
                            {
                                SetActivityIDs(cur, activityId);
                            }
                        }
                        else if ((sv["Key"] as string) == "ParentSpanId")
                        {
                            if (sv["Value"] is string parentSpanId)
                            {
                                SetParentSpanID(cur, parentSpanId);
                            }
                        }
                    }
                }
            }
        }

        private void OnDiagSourceActivityStop(TraceEvent traceEvent)
        {
            StartStopActivity cur = _startStopActivityComputer.GetCurrentStartStopActivity(traceEvent.Thread(), traceEvent);
            if (cur != null)
            {
                ClearActivityIDs(cur, traceEvent);
            }
        }

        public string GetCurrentTraceId(StartStopActivity startStopActivity)
        {
            StartStopActivity cur = startStopActivity;
            while(cur != null)
            {
                string traceId = _traceIds.Get((int)cur.Index);
                if(traceId != null)
                {
                    return traceId;
                }
                cur = cur.Creator;
            }
            return null;
        }

        public string GetCurrentSpanId(StartStopActivity startStopActivity)
        {
            StartStopActivity cur = startStopActivity;
            while (cur != null)
            {
                string spanId = _spanIds.Get((int)cur.Index);
                if (spanId != null)
                {
                    return spanId;
                }
                cur = cur.Creator;
            }
            return null;
        }
        public string GetCurrentParentSpanId(StartStopActivity startStopActivity)
        {
            StartStopActivity cur = startStopActivity;
            while (cur != null)
            {
                string spanId = _parentSpanIds.Get((int)cur.Index);
                if (spanId != null)
                {
                    return spanId;
                }
                cur = cur.Creator;
            }
            return null;
        }

        bool TryParseW3CId(string id, out string traceId, out string spanId)
        {
            traceId = null;
            spanId = null;
            // W3C should look like:
            // 00-hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh-hhhhhhhhhhhhhhhh-hh
            if(id.Length != 55 || 
               id[0] != '0' ||
               id[1] != '0' ||
               id[2] != '-' ||
               id[35] != '-' ||
               id[52] != '-' ||
               !IsLowerHexChar(id[53]) ||
               !IsLowerHexChar(id[54]))
            {
                return false;
            }
            for(int i = 3; i < 35; i++)
            {
                if(!IsLowerHexChar(id[i]))
                {
                    return false;
                }
            }
            for (int i = 36; i < 52; i++)
            {
                if (!IsLowerHexChar(id[i]))
                {
                    return false;
                }
            }
            traceId = id.Substring(3, 32);
            spanId = id.Substring(36, 16);
            return true;
        }

        bool IsLowerHexChar(char c) =>
            ('0' <= c && c <= '9') ||
            ('a' <= c && c <= 'f');
    }
}
