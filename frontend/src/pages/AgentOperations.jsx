import React, { useEffect, useState } from 'react';
import { Activity, Bot, Clock3, ShieldAlert, Zap } from 'lucide-react';
import Sidebar from '../components/Sidebar';
import { useLiveRefresh } from '../lib/useLiveRefresh';
import { fetchAgentOperationsMetrics, formatNumber } from '../services/api';
import { getPageCache, setPageCache } from '../lib/pageCache';

const WINDOW_OPTIONS = [7, 30, 90];

const formatDuration = (seconds) => {
  const totalSeconds = Number(seconds || 0);
  if (totalSeconds <= 0) return '0m';
  if (totalSeconds < 60) return `${Math.round(totalSeconds)}s`;
  if (totalSeconds < 3600) return `${Math.round(totalSeconds / 60)}m`;
  return `${(totalSeconds / 3600).toFixed(1)}h`;
};

const percentWidth = (value, total) => {
  if (!total) return 0;
  return Math.max(0, Math.min(100, (value / total) * 100));
};

const AgentOperations = () => {
  const cache = getPageCache('agent-operations');
  const [activeNav] = useState('agent-operations');
  const [days, setDays] = useState(cache?.days || 30);
  const [loading, setLoading] = useState(!cache);
  const [error, setError] = useState(cache?.error || '');
  const [metrics, setMetrics] = useState(cache?.metrics || {
    totals: {
      total_invoices_processed: 0,
      fully_automated: 0,
      fast_approval: 0,
      deeper_human_involvement: 0,
    },
    automation_rate_percent: 0,
    average_processing_time_seconds: 0,
    agent_activity: [],
    sla_health: {
      ok: 0,
      warning: 0,
      breaching: 0,
      breached: 0,
    },
    exceptions: {
      auto_resolved: 0,
      escalated: 0,
    },
  });

  useEffect(() => {
    setPageCache('agent-operations', {
      days,
      error,
      metrics,
    });
  }, [days, error, metrics]);

  const load = async () => {
    try {
      setLoading((current) => current && !metrics.agent_activity.length);
      const payload = await fetchAgentOperationsMetrics({ days });
      setMetrics(payload);
      setError('');
    } catch (err) {
      setError(err.message || 'Could not load agent operations metrics.');
    } finally {
      setLoading(false);
    }
  };

  useLiveRefresh(load, [days]);

  const totals = metrics.totals || {};
  const totalProcessed = totals.total_invoices_processed || 0;
  const outcomeSegments = [
    {
      label: 'Fully automated',
      value: totals.fully_automated || 0,
      tone: 'bg-green-500',
      note: 'Zero human touchpoints',
    },
    {
      label: 'Fast approval',
      value: totals.fast_approval || 0,
      tone: 'bg-amber-400',
      note: 'Exactly one approval',
    },
    {
      label: 'Full review',
      value: totals.deeper_human_involvement || 0,
      tone: 'bg-red-500',
      note: 'Multiple or deeper human interventions',
    },
  ];

  const slaCards = [
    {
      label: 'OK',
      value: metrics.sla_health?.ok || 0,
      className: 'bg-green-50 border-green-200 text-green-700',
    },
    {
      label: 'Warning',
      value: metrics.sla_health?.warning || 0,
      className: 'bg-amber-50 border-amber-200 text-amber-700',
    },
    {
      label: 'Breaching',
      value: metrics.sla_health?.breaching || 0,
      className: 'bg-orange-50 border-orange-200 text-orange-700',
    },
    {
      label: 'Breached',
      value: metrics.sla_health?.breached || 0,
      className: 'bg-red-50 border-red-200 text-red-700',
    },
  ];

  return (
    <div className="min-h-screen bg-white">
      <Sidebar activeItem={activeNav} />

      <div className="lg:ml-[220px] p-4 sm:p-6 lg:p-8 transition-all">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between mb-6">
          <div className="flex items-center gap-3">
            <h1 className="text-2xl sm:text-3xl font-semibold text-black">Agent Operations</h1>
          </div>
          <div className="flex items-center gap-2">
            {WINDOW_OPTIONS.map((option) => (
              <button
                key={option}
                type="button"
                onClick={() => setDays(option)}
                className={`px-4 py-2 rounded-full border text-sm font-medium ${
                  days === option
                    ? 'border-black bg-black text-white'
                    : 'border-gray-200 bg-white text-black hover:bg-gray-50'
                }`}
              >
                {option} days
              </button>
            ))}
          </div>
        </div>

        {loading && (
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-gray-600">Loading operations metrics...</p>
          </div>
        )}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-red-600">{error}</p>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 mb-6">
          <div className="lg:col-span-2 bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-xs uppercase tracking-[0.18em] text-gray-500 mb-2">Automation Rate</p>
                <p className="text-4xl sm:text-5xl font-semibold text-black">
                  {(metrics.automation_rate_percent || 0).toFixed(1)}%
                </p>
                <p className="text-sm text-gray-500 mt-3">
                  {formatNumber(totals.fully_automated || 0)} of {formatNumber(totalProcessed)} invoices completed with zero human touchpoints.
                </p>
              </div>
              <div className="w-14 h-14 rounded-2xl border border-gray-200 flex items-center justify-center">
                <Zap className="w-6 h-6 text-black" />
              </div>
            </div>
          </div>

          <div className="bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Avg Processing Time</p>
              <Clock3 className="w-4 h-4 text-gray-500" />
            </div>
            <p className="text-3xl font-semibold text-black">{formatDuration(metrics.average_processing_time_seconds)}</p>
            <p className="text-sm text-gray-500 mt-2">From first recorded invoice event to the latest one.</p>
          </div>

          <div className="bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
            <div className="flex items-center justify-between mb-3">
              <p className="text-xs uppercase tracking-[0.18em] text-gray-500">Exception Recovery</p>
              <ShieldAlert className="w-4 h-4 text-gray-500" />
            </div>
            <p className="text-3xl font-semibold text-black">{formatNumber(metrics.exceptions?.auto_resolved || 0)}</p>
            <p className="text-sm text-gray-500 mt-2">
              Auto-resolved exceptions versus {formatNumber(metrics.exceptions?.escalated || 0)} escalations in the selected window.
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 mb-6">
          <div className="xl:col-span-2 bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
            <div className="flex items-center justify-between mb-5">
              <div>
                <h2 className="text-lg font-semibold text-black">Invoice Outcomes</h2>
                <p className="text-sm text-gray-500 mt-1">Outcome mix for the selected time window.</p>
              </div>
              <span className="text-sm text-gray-500">{formatNumber(totalProcessed)} invoices</span>
            </div>

            <div className="w-full h-5 rounded-full overflow-hidden bg-gray-100 flex mb-5">
              {outcomeSegments.map((segment) => (
                <div
                  key={segment.label}
                  className={segment.tone}
                  style={{ width: `${percentWidth(segment.value, totalProcessed)}%` }}
                  title={`${segment.label}: ${segment.value}`}
                />
              ))}
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {outcomeSegments.map((segment) => (
                <div key={segment.label} className="rounded-xl border border-gray-200 p-4">
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`w-3 h-3 rounded-full ${segment.tone}`} />
                    <p className="text-sm font-medium text-black">{segment.label}</p>
                  </div>
                  <p className="text-2xl font-semibold text-black">{formatNumber(segment.value)}</p>
                  <p className="text-sm text-gray-500 mt-1">{segment.note}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
            <div className="flex items-center justify-between mb-5">
              <div>
                <h2 className="text-lg font-semibold text-black">SLA Health</h2>
                <p className="text-sm text-gray-500 mt-1">Active invoice SLA distribution.</p>
              </div>
              <Activity className="w-5 h-5 text-gray-500" />
            </div>

            <div className="space-y-3">
              {slaCards.map((card) => (
                <div key={card.label} className={`rounded-xl border px-4 py-3 ${card.className}`}>
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">{card.label}</span>
                    <span className="text-2xl font-semibold">{formatNumber(card.value)}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="bg-white border border-gray-200 rounded-2xl p-5 sm:p-6">
          <div className="flex items-center justify-between mb-5">
            <div>
              <h2 className="text-lg font-semibold text-black">Per-Agent Activity</h2>
              <p className="text-sm text-gray-500 mt-1">Decision counts and average confidence by agent.</p>
            </div>
            <Bot className="w-5 h-5 text-gray-500" />
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left text-gray-500">
                  <th className="py-3 pr-4 font-medium">Agent</th>
                  <th className="py-3 pr-4 font-medium">Decisions</th>
                  <th className="py-3 pr-4 font-medium">Average Confidence</th>
                </tr>
              </thead>
              <tbody>
                {(metrics.agent_activity || []).map((row) => (
                  <tr key={row.agent_name} className="border-b border-gray-100 last:border-b-0">
                    <td className="py-3 pr-4 text-black font-medium">{row.agent_name}</td>
                    <td className="py-3 pr-4 text-gray-700">{formatNumber(row.decision_count || 0)}</td>
                    <td className="py-3 pr-4 text-gray-700">
                      {row.average_confidence == null ? 'n/a' : `${(row.average_confidence * 100).toFixed(1)}%`}
                    </td>
                  </tr>
                ))}
                {!metrics.agent_activity?.length && (
                  <tr>
                    <td colSpan={3} className="py-8 text-center text-gray-500">
                      No agent decisions were recorded in this window.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AgentOperations;
