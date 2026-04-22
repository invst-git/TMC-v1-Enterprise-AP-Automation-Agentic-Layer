import React, { useEffect, useMemo, useState } from 'react';
import {
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Clock3,
  FileSearch,
  MessageSquareText,
  ShieldAlert,
  XCircle,
} from 'lucide-react';
import Sidebar from '../components/Sidebar';
import {
  assignAgentReviewItem,
  fetchAgentReviewQueue,
  formatCurrency,
  rejectAgentReviewItem,
  resolveAgentReviewItem,
} from '../services/api';
import { useLiveRefresh } from '../lib/useLiveRefresh';
import { getPageCache, setPageCache } from '../lib/pageCache';

const STATUS_FILTERS = [
  { value: '', label: 'All' },
  { value: 'pending', label: 'Pending' },
  { value: 'in_review', label: 'In Review' },
];

const PRIORITY_FILTERS = [
  { value: 'all', label: 'All Priorities' },
  { value: 'urgent', label: 'Urgent' },
  { value: 'high', label: 'High' },
  { value: 'normal', label: 'Normal' },
];

const priorityMeta = (priority) => {
  const value = Number(priority || 100);
  if (value <= 80) {
    return {
      label: 'Urgent',
      className: 'bg-red-50 text-red-700 border-red-200',
    };
  }
  if (value <= 100) {
    return {
      label: 'High',
      className: 'bg-amber-50 text-amber-700 border-amber-200',
    };
  }
  return {
    label: 'Normal',
    className: 'bg-gray-50 text-gray-700 border-gray-200',
  };
};

const matchesPriorityFilter = (item, priorityFilter) => {
  const priority = Number(item.priority || 100);
  if (priorityFilter === 'urgent') return priority <= 80;
  if (priorityFilter === 'high') return priority > 80 && priority <= 100;
  if (priorityFilter === 'normal') return priority > 100;
  return true;
};

const formatRelativeTime = (value) => {
  if (!value) return 'Unknown';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return 'Unknown';
  const diffSeconds = Math.max(0, Math.floor((Date.now() - parsed.getTime()) / 1000));
  if (diffSeconds < 60) return `${diffSeconds}s ago`;
  if (diffSeconds < 3600) return `${Math.floor(diffSeconds / 60)}m ago`;
  if (diffSeconds < 86400) return `${Math.floor(diffSeconds / 3600)}h ago`;
  return `${Math.floor(diffSeconds / 86400)}d ago`;
};

const formatTimestamp = (value) => {
  if (!value) return 'Unknown time';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return 'Unknown time';
  return parsed.toLocaleString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
};

const formatConfidence = (value) => {
  const numericValue = Number(value || 0);
  return `${Math.round(Math.max(0, Math.min(1, numericValue)) * 100)}%`;
};

const formatAttemptPath = (pathName) => {
  const key = String(pathName || '').trim();
  const labels = {
    vendor_fuzzy_match: 'Vendor Fuzzy Match',
    po_fuzzy_match: 'PO Fuzzy Match',
    amount_tolerance_small: 'Amount Tolerance Check',
    amount_tolerance_moderate: 'Amount Tolerance with Precedent',
    ocr_targeted_retry: 'Targeted OCR Retry',
  };
  if (labels[key]) return labels[key];
  if (!key) return 'Recovery Path';
  return key
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
};

const formatAttemptOutcome = (outcome) => {
  const key = String(outcome || '').trim();
  if (!key) return 'No Result';
  return key
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
};

const attemptTone = (outcome) => {
  const lowered = String(outcome || '').toLowerCase();
  if (['resolved', 'matched', 'success', 'improved', 'recovered'].some((token) => lowered.includes(token))) {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  }
  if (['retry', 'partial', 'warning', 'candidate', 'considered'].some((token) => lowered.includes(token))) {
    return 'bg-amber-50 text-amber-700 border-amber-200';
  }
  return 'bg-red-50 text-red-700 border-red-200';
};

const formatFindingValue = (value) => {
  if (value === null || value === undefined || value === '') return 'None';
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(2);
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  if (Array.isArray(value)) {
    if (!value.length) return 'None';
    return value.map((entry) => (typeof entry === 'object' ? JSON.stringify(entry) : String(entry))).join(', ');
  }
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
};

const analysisLabel = (analysis) => {
  if (!analysis) return 'Unavailable';
  const decision = String(analysis.decision || 'needs_review')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
  const reason = String(analysis.reason || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
  if (!reason) return decision;
  return `${decision} - ${reason}`;
};

const ReviewQueue = () => {
  const cache = getPageCache('review-queue');
  const [activeNav] = useState('review-queue');
  const [statusFilter, setStatusFilter] = useState(cache?.statusFilter || '');
  const [priorityFilter, setPriorityFilter] = useState(cache?.priorityFilter || 'all');
  const [items, setItems] = useState(cache?.items || []);
  const [loading, setLoading] = useState(!cache);
  const [error, setError] = useState(cache?.error || '');
  const [expandedId, setExpandedId] = useState(cache?.expandedId || null);
  const [reviewerById, setReviewerById] = useState({});
  const [notesById, setNotesById] = useState({});
  const [selectedPoById, setSelectedPoById] = useState({});
  const [actionState, setActionState] = useState({ itemId: null, action: '' });
  const [actionErrorById, setActionErrorById] = useState({});

  useEffect(() => {
    setPageCache('review-queue', {
      statusFilter,
      priorityFilter,
      items,
      error,
      expandedId,
    });
  }, [statusFilter, priorityFilter, items, error, expandedId]);

  const load = async () => {
    try {
      setLoading((current) => current && !items.length);
      const payload = await fetchAgentReviewQueue({
        activeOnly: true,
        status: statusFilter || undefined,
        limit: 200,
      });
      setItems(payload || []);
      setExpandedId((current) => ((payload || []).some((item) => item.id === current) ? current : null));
      setError('');
    } catch (err) {
      setError(err.message || 'Could not load the review queue.');
    } finally {
      setLoading(false);
    }
  };

  const liveState = useLiveRefresh(load, [statusFilter]);

  const filteredItems = useMemo(
    () => items.filter((item) => matchesPriorityFilter(item, priorityFilter)),
    [items, priorityFilter],
  );

  const runItemAction = async (item, action, handler) => {
    const reviewer = String(reviewerById[item.id] ?? item.assigned_to ?? '').trim();
    const notes = String(notesById[item.id] || '').trim();

    if (action !== 'assign' && !notes) {
      setActionErrorById((current) => ({
        ...current,
        [item.id]: 'Enter resolution notes before taking action.',
      }));
      return;
    }
    if (!reviewer) {
      setActionErrorById((current) => ({
        ...current,
        [item.id]: 'Enter the reviewer name before taking action.',
      }));
      return;
    }

    try {
      setActionState({ itemId: item.id, action });
      setActionErrorById((current) => ({ ...current, [item.id]: '' }));
      await handler({ reviewer, notes });
      if (action !== 'assign') {
        setItems((current) => current.filter((currentItem) => currentItem.id !== item.id));
        setExpandedId((current) => (current === item.id ? null : current));
        setNotesById((current) => ({ ...current, [item.id]: '' }));
      } else {
        setItems((current) =>
          current.map((currentItem) =>
            currentItem.id === item.id
              ? {
                  ...currentItem,
                  assigned_to: reviewer,
                  status: 'assigned',
                  display_status: 'in_review',
                }
              : currentItem,
          ),
        );
      }
      await load();
    } catch (err) {
      setActionErrorById((current) => ({
        ...current,
        [item.id]: err.message || 'The action could not be completed.',
      }));
    } finally {
      setActionState({ itemId: null, action: '' });
    }
  };

  return (
    <div className="min-h-screen bg-white">
      <Sidebar activeItem={activeNav} />

      <div className="lg:ml-[220px] p-4 sm:p-6 lg:p-8 transition-all">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between mb-6">
          <div className="flex items-center gap-3">
            <h1 className="text-2xl sm:text-3xl font-semibold text-black">Review Queue</h1>
            <span className={`rounded-full px-2 py-1 text-[11px] font-medium ${liveState.connected ? 'bg-green-50 text-green-700 border border-green-200' : 'bg-amber-50 text-amber-700 border border-amber-200'}`}>
              {liveState.connected ? 'Live' : 'Reconnecting'}
            </span>
          </div>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
            <div className="flex items-center gap-1 bg-gray-100 rounded-full p-1 border border-gray-200">
              {STATUS_FILTERS.map((filter) => (
                <button
                  key={filter.label}
                  type="button"
                  onClick={() => setStatusFilter(filter.value)}
                  className={`px-3 py-1.5 rounded-full text-xs font-medium transition-all ${statusFilter === filter.value ? 'bg-black text-white' : 'bg-transparent text-gray-600 hover:text-black'}`}
                >
                  {filter.label}
                </button>
              ))}
            </div>
            <select
              className="border border-gray-200 rounded-lg text-sm px-3 py-2"
              value={priorityFilter}
              onChange={(event) => setPriorityFilter(event.target.value)}
            >
              {PRIORITY_FILTERS.map((filter) => (
                <option key={filter.value} value={filter.value}>
                  {filter.label}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="px-4 py-2 border border-black rounded-full text-sm font-medium text-black hover:bg-gray-50"
              onClick={load}
            >
              Refresh
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <p className="text-xs uppercase tracking-[0.18em] text-gray-500 mb-2">Active Items</p>
            <p className="text-3xl font-semibold text-black">{filteredItems.length}</p>
            <p className="text-sm text-gray-500 mt-2">Pending and in-review items currently visible under the active filters.</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <p className="text-xs uppercase tracking-[0.18em] text-gray-500 mb-2">Urgent Items</p>
            <p className="text-3xl font-semibold text-black">{filteredItems.filter((item) => Number(item.priority || 100) <= 80).length}</p>
            <p className="text-sm text-gray-500 mt-2">These items crossed the urgent priority threshold and should be reviewed first.</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <p className="text-xs uppercase tracking-[0.18em] text-gray-500 mb-2">Automated Attempts</p>
            <p className="text-3xl font-semibold text-black">
              {filteredItems.reduce((total, item) => total + Number(item.automated_attempt_count || 0), 0)}
            </p>
            <p className="text-sm text-gray-500 mt-2">Total automated recovery attempts captured across the visible queue.</p>
          </div>
        </div>

        {loading && (
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-gray-600">Loading review items...</p>
          </div>
        )}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-red-600">{error}</p>
          </div>
        )}

        {!loading && filteredItems.length === 0 ? (
          <div className="bg-white border border-gray-200 rounded-2xl p-6">
            <p className="text-sm text-gray-600">No review items match the current filters.</p>
          </div>
        ) : (
          <div className="space-y-4">
            {filteredItems.map((item) => {
              const isExpanded = expandedId === item.id;
              const priority = priorityMeta(item.priority);
              const resolutionPacket = item.resolution_packet || {};
              const attempts = resolutionPacket.attempts || [];
              const candidatePos = item.candidate_pos || [];
              const reviewerValue = reviewerById[item.id] ?? item.assigned_to ?? '';
              const notesValue = notesById[item.id] || '';
              const canSubmit = Boolean(String(reviewerValue).trim() && String(notesValue).trim());
              const selectedPoId = selectedPoById[item.id] || candidatePos[0]?.po_id || '';
              const invoiceSummary = item.invoice_summary || {};
              const actionError = actionErrorById[item.id];
              const isBusy = actionState.itemId === item.id;

              return (
                <div key={item.id} className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
                  <button
                    type="button"
                    onClick={() => setExpandedId((current) => (current === item.id ? null : item.id))}
                    className="w-full p-4 sm:p-5 text-left hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2 mb-3">
                          <span className={`px-2.5 py-1 rounded-full border text-[11px] font-semibold ${priority.className}`}>
                            {priority.label}
                          </span>
                          <span className="px-2.5 py-1 rounded-full border border-gray-200 text-[11px] font-semibold text-gray-700 bg-gray-50">
                            {item.display_status === 'in_review' ? 'In Review' : 'Pending'}
                          </span>
                          <span className="text-xs text-gray-500">
                            {item.assigned_to ? `Assigned to ${item.assigned_to}` : 'Unassigned'}
                          </span>
                        </div>
                        <h2 className="text-lg font-semibold text-black">
                          {invoiceSummary.invoice_number || item.invoice_id || item.id}
                        </h2>
                        <p className="text-sm text-gray-600 mt-1">
                          {item.review_reason_label} for {invoiceSummary.vendor_name || invoiceSummary.supplier_name || 'Unknown vendor'}
                        </p>
                      </div>

                      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 lg:min-w-[520px]">
                        <div>
                          <p className="text-[11px] uppercase tracking-[0.16em] text-gray-500 mb-1">Created</p>
                          <p className="text-sm font-medium text-black flex items-center gap-1">
                            <Clock3 className="w-4 h-4 text-gray-500" />
                            {formatRelativeTime(item.created_at)}
                          </p>
                        </div>
                        <div>
                          <p className="text-[11px] uppercase tracking-[0.16em] text-gray-500 mb-1">Attempts</p>
                          <p className="text-sm font-medium text-black">{item.automated_attempt_count || 0}</p>
                        </div>
                        <div className="sm:col-span-2">
                          <p className="text-[11px] uppercase tracking-[0.16em] text-gray-500 mb-1">Recommended Action</p>
                          <p className="text-sm text-black line-clamp-2">{item.recommended_action}</p>
                        </div>
                      </div>

                      <div className="flex items-center justify-end">
                        {isExpanded ? (
                          <ChevronUp className="w-5 h-5 text-gray-500" />
                        ) : (
                          <ChevronDown className="w-5 h-5 text-gray-500" />
                        )}
                      </div>
                    </div>
                  </button>
                  {isExpanded && (
                    <div className="border-t border-gray-200 px-4 sm:px-5 py-5 bg-gray-50">
                      <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
                        <div className="xl:col-span-2 space-y-5">
                          <div className="bg-white border border-gray-200 rounded-2xl p-4">
                            <div className="flex items-center gap-2 mb-4">
                              <FileSearch className="w-4 h-4 text-gray-500" />
                              <h3 className="text-sm font-semibold text-black">Invoice Summary</h3>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Invoice</p>
                                <p className="text-black font-medium">{invoiceSummary.invoice_number || item.invoice_id || 'Unknown'}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Vendor</p>
                                <p className="text-black font-medium">{invoiceSummary.vendor_name || invoiceSummary.supplier_name || 'Unknown'}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Invoice Date</p>
                                <p className="text-black">{invoiceSummary.invoice_date ? formatTimestamp(invoiceSummary.invoice_date) : 'Unknown'}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Total</p>
                                <p className="text-black">{formatCurrency(invoiceSummary.total_amount)}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">PO Number</p>
                                <p className="text-black">{invoiceSummary.po_number || 'Not provided'}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Current Status</p>
                                <p className="text-black">{invoiceSummary.status || 'Unknown'}</p>
                              </div>
                            </div>
                          </div>

                          <div className="bg-amber-50 border border-amber-200 rounded-2xl p-4">
                            <div className="flex items-center gap-2 mb-2">
                              <ShieldAlert className="w-4 h-4 text-amber-700" />
                              <h3 className="text-sm font-semibold text-amber-900">Recommended Human Action</h3>
                            </div>
                            <p className="text-sm text-amber-900">{item.recommended_action}</p>
                          </div>

                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="bg-white border border-gray-200 rounded-2xl p-4">
                              <p className="text-xs uppercase tracking-[0.16em] text-gray-500 mb-2">Initial Analysis</p>
                              <p className="text-sm font-medium text-black">{analysisLabel(resolutionPacket.initial_analysis)}</p>
                              <p className="text-sm text-gray-600 mt-2">
                                Confidence {formatConfidence(resolutionPacket.initial_analysis?.confidence)}
                              </p>
                            </div>
                            <div className="bg-white border border-gray-200 rounded-2xl p-4">
                              <p className="text-xs uppercase tracking-[0.16em] text-gray-500 mb-2">Final Analysis</p>
                              <p className="text-sm font-medium text-black">{analysisLabel(resolutionPacket.final_analysis)}</p>
                              <p className="text-sm text-gray-600 mt-2">
                                Confidence {formatConfidence(resolutionPacket.final_analysis?.confidence)}
                              </p>
                            </div>
                          </div>

                          <div className="bg-white border border-gray-200 rounded-2xl p-4">
                            <h3 className="text-sm font-semibold text-black mb-4">Automated Recovery Paths</h3>
                            {attempts.length === 0 ? (
                              <p className="text-sm text-gray-600">No automated recovery attempts were recorded for this item.</p>
                            ) : (
                              <div className="space-y-3">
                                {attempts.map((attempt, index) => (
                                  <div key={`${item.id}-attempt-${index}`} className="border border-gray-200 rounded-xl p-4">
                                    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between mb-3">
                                      <div>
                                        <p className="text-sm font-semibold text-black">{formatAttemptPath(attempt.path_name)}</p>
                                        <p className="text-sm text-gray-600 mt-1">{attempt.summary || 'No summary available.'}</p>
                                      </div>
                                      <div className="flex flex-wrap items-center gap-2">
                                        <span className={`px-2.5 py-1 rounded-full border text-[11px] font-semibold ${attemptTone(attempt.outcome)}`}>
                                          {formatAttemptOutcome(attempt.outcome)}
                                        </span>
                                        <span className="px-2.5 py-1 rounded-full border border-gray-200 text-[11px] font-semibold text-gray-700 bg-gray-50">
                                          Confidence {formatConfidence(attempt.confidence)}
                                        </span>
                                      </div>
                                    </div>
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                      {Object.entries(attempt.findings || {}).map(([key, value]) => (
                                        <div key={`${item.id}-${index}-${key}`} className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
                                          <p className="text-[11px] uppercase tracking-[0.14em] text-gray-500 mb-1">
                                            {key.replace(/[_-]+/g, ' ')}
                                          </p>
                                          <p className="text-sm text-black break-words">{formatFindingValue(value)}</p>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>

                          <div className="bg-white border border-gray-200 rounded-2xl p-4">
                            <h3 className="text-sm font-semibold text-black mb-4">Candidate Purchase Orders</h3>
                            {candidatePos.length === 0 ? (
                              <p className="text-sm text-gray-600">No candidate purchase orders were captured for this review item.</p>
                            ) : (
                              <div className="space-y-3">
                                {candidatePos.map((candidate) => {
                                  const checked = String(selectedPoId) === String(candidate.po_id);
                                  return (
                                    <label
                                      key={candidate.po_id}
                                      className={`block rounded-xl border p-4 cursor-pointer transition-colors ${checked ? 'border-black bg-gray-50' : 'border-gray-200 bg-white hover:bg-gray-50'}`}
                                    >
                                      <div className="flex items-start gap-3">
                                        <input
                                          type="radio"
                                          name={`candidate-po-${item.id}`}
                                          className="mt-1"
                                          checked={checked}
                                          onChange={() => setSelectedPoById((current) => ({ ...current, [item.id]: candidate.po_id }))}
                                        />
                                        <div className="flex-1 grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
                                          <div>
                                            <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">PO Number</p>
                                            <p className="font-medium text-black">{candidate.po_number || candidate.po_id}</p>
                                          </div>
                                          <div>
                                            <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Similarity</p>
                                            <p className="text-black">{formatConfidence(candidate.similarity_score)}</p>
                                          </div>
                                          <div>
                                            <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Amount Delta</p>
                                            <p className="text-black">{formatCurrency(candidate.amount_diff)}</p>
                                          </div>
                                          <div>
                                            <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Tolerance</p>
                                            <p className="text-black">{candidate.within_tolerance ? 'Within tolerance' : 'Outside tolerance'}</p>
                                          </div>
                                          <div className="md:col-span-2">
                                            <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Eligibility Notes</p>
                                            <p className="text-black">{candidate.eligibility_reason || 'No additional notes.'}</p>
                                          </div>
                                        </div>
                                      </div>
                                    </label>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        </div>

                        <div className="space-y-5">
                          <div className="bg-white border border-gray-200 rounded-2xl p-4">
                            <h3 className="text-sm font-semibold text-black mb-4">Operator Action</h3>
                            <label className="block mb-4">
                              <span className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-2 block">Reviewer</span>
                              <input
                                type="text"
                                className="w-full border border-gray-200 rounded-xl px-3 py-2 text-sm text-black"
                                placeholder="Enter reviewer name or email"
                                value={reviewerValue}
                                onChange={(event) => setReviewerById((current) => ({ ...current, [item.id]: event.target.value }))}
                              />
                            </label>
                            <label className="block mb-4">
                              <span className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-2 block">Resolution Notes</span>
                              <textarea
                                className="w-full min-h-[132px] border border-gray-200 rounded-xl px-3 py-2 text-sm text-black resize-y"
                                placeholder="Explain why you are approving the match, requesting clarification, or rejecting the item."
                                value={notesValue}
                                onChange={(event) => setNotesById((current) => ({ ...current, [item.id]: event.target.value }))}
                              />
                            </label>
                            {actionError && (
                              <div className="mb-4 rounded-xl border border-red-200 bg-red-50 px-3 py-2">
                                <p className="text-sm text-red-700">{actionError}</p>
                              </div>
                            )}
                            <div className="space-y-3">
                              <button
                                type="button"
                                className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl border border-gray-200 text-sm font-medium text-black hover:bg-gray-50 disabled:opacity-60 disabled:cursor-not-allowed"
                                disabled={isBusy || !String(reviewerValue).trim()}
                                onClick={() => runItemAction(item, 'assign', ({ reviewer }) => assignAgentReviewItem({ reviewItemId: item.id, reviewer }))}
                              >
                                <FileSearch className="w-4 h-4" />
                                {item.assigned_to ? 'Update Assignment' : 'Assign to Reviewer'}
                              </button>
                              <button
                                type="button"
                                className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl bg-black text-white text-sm font-medium hover:bg-gray-900 disabled:opacity-60 disabled:cursor-not-allowed"
                                disabled={isBusy || !canSubmit || candidatePos.length === 0}
                                onClick={() => runItemAction(item, 'approve_match', ({ reviewer, notes }) => resolveAgentReviewItem({
                                  reviewItemId: item.id,
                                  reviewer,
                                  action: 'approve_match',
                                  resolutionNotes: notes,
                                  selectedPoId: selectedPoId || undefined,
                                }))}
                              >
                                <CheckCircle2 className="w-4 h-4" />
                                {isBusy && actionState.action === 'approve_match' ? 'Approving...' : 'Approve Match'}
                              </button>
                              <button
                                type="button"
                                className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl border border-amber-300 bg-amber-50 text-amber-900 text-sm font-medium hover:bg-amber-100 disabled:opacity-60 disabled:cursor-not-allowed"
                                disabled={isBusy || !canSubmit}
                                onClick={() => runItemAction(item, 'request_vendor_clarification', ({ reviewer, notes }) => resolveAgentReviewItem({
                                  reviewItemId: item.id,
                                  reviewer,
                                  action: 'request_vendor_clarification',
                                  resolutionNotes: notes,
                                }))}
                              >
                                <MessageSquareText className="w-4 h-4" />
                                {isBusy && actionState.action === 'request_vendor_clarification' ? 'Requesting...' : 'Request Vendor Clarification'}
                              </button>
                              <button
                                type="button"
                                className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-xl border border-red-300 bg-red-50 text-red-700 text-sm font-medium hover:bg-red-100 disabled:opacity-60 disabled:cursor-not-allowed"
                                disabled={isBusy || !canSubmit}
                                onClick={() => runItemAction(item, 'reject', ({ reviewer, notes }) => rejectAgentReviewItem({
                                  reviewItemId: item.id,
                                  reviewer,
                                  resolutionNotes: notes,
                                }))}
                              >
                                <XCircle className="w-4 h-4" />
                                {isBusy && actionState.action === 'reject' ? 'Rejecting...' : 'Reject'}
                              </button>
                            </div>
                          </div>

                          <div className="bg-white border border-gray-200 rounded-2xl p-4">
                            <h3 className="text-sm font-semibold text-black mb-3">Queue Context</h3>
                            <div className="space-y-3 text-sm">
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Queue Name</p>
                                <p className="text-black">{item.queue_name || 'exceptions'}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Created At</p>
                                <p className="text-black">{formatTimestamp(item.created_at)}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Updated At</p>
                                <p className="text-black">{formatTimestamp(item.updated_at)}</p>
                              </div>
                              <div>
                                <p className="text-xs uppercase tracking-[0.14em] text-gray-500 mb-1">Review Item ID</p>
                                <p className="text-black break-all">{item.id}</p>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReviewQueue;
