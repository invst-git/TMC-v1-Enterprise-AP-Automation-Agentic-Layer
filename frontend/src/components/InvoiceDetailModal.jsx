import React, { useEffect, useState } from 'react';
import { Calendar, DollarSign, FileText, Package } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from './ui/dialog';
import {
  fetchInvoiceAuditTrail,
  fetchInvoiceById,
  formatCurrency,
  getStatusLabel,
} from '../services/api';
import { useLiveRefresh } from '../lib/useLiveRefresh';

const TONE_STYLES = {
  success: 'border-emerald-200 bg-emerald-50 text-emerald-900',
  warning: 'border-amber-200 bg-amber-50 text-amber-900',
  failure: 'border-red-200 bg-red-50 text-red-900',
  state_transition: 'border-sky-200 bg-sky-50 text-sky-900',
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

const formatDuration = (seconds) => {
  const value = Number(seconds || 0);
  if (!Number.isFinite(value) || value <= 0) return '0m';
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  if (hours <= 0) return `${minutes}m`;
  if (minutes <= 0) return `${hours}h`;
  return `${hours}h ${minutes}m`;
};

const InvoiceDetailModal = ({ invoice, open, onClose }) => {
  const [details, setDetails] = useState(invoice || null);
  const [auditTrail, setAuditTrail] = useState(null);
  const [loading, setLoading] = useState(false);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditError, setAuditError] = useState(null);
  const [activeView, setActiveView] = useState('overview');

  const load = async () => {
    if (!open || !invoice || !invoice.id) {
      setDetails(invoice || null);
      setAuditTrail(null);
      setAuditError(null);
      return;
    }

    const showInvoiceLoader = !details || details.id !== invoice.id;
    const showAuditLoader = !auditTrail || auditTrail.invoice_id !== invoice.id;

    try {
      if (showInvoiceLoader) setLoading(true);
      if (showAuditLoader) setAuditLoading(true);
      const [full, audit] = await Promise.all([
        fetchInvoiceById(invoice.id),
        fetchInvoiceAuditTrail(invoice.id, { limit: 500 }),
      ]);
      setDetails({ ...invoice, ...full });
      setAuditTrail(audit);
      setAuditError(null);
    } catch (error) {
      if (showInvoiceLoader) setDetails(invoice);
      setAuditError(error.message);
    } finally {
      setLoading(false);
      setAuditLoading(false);
    }
  };

  useEffect(() => {
    if (!open) return;
    setActiveView('overview');
  }, [invoice?.id, open]);

  useLiveRefresh(load, [invoice?.id || '', open ? 'open' : 'closed']);

  if (!invoice) return null;

  const currentDetails = details || invoice;
  const currentStatus = currentDetails?.status || invoice.status;
  const auditSummary = auditTrail?.summary || {};
  const auditEvents = Array.isArray(auditTrail?.events) ? auditTrail.events : [];

  const getStatusStyle = (status) => {
    switch (status) {
      case 'matched_auto':
        return 'bg-gray-800 text-white border-gray-800';
      case 'ready_for_payment':
        return 'bg-slate-900 text-white border-slate-900';
      case 'paid':
        return 'bg-emerald-600 text-white border-emerald-600';
      case 'payment_pending':
        return 'bg-amber-50 text-amber-900 border-amber-300';
      case 'needs_review':
        return 'bg-red-50 text-red-900 border-red-300';
      case 'vendor_mismatch':
      case 'unmatched':
        return 'bg-white text-black border-gray-300';
      default:
        return 'bg-white text-black border-gray-300';
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl bg-white border border-gray-200">
        <DialogHeader>
          <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <DialogTitle className="text-xl sm:text-2xl font-semibold text-black">
                Invoice Details
              </DialogTitle>
              <p className="text-sm text-gray-500 mt-1">
                {currentDetails?.invoiceNumber || invoice.invoiceNumber}
              </p>
            </div>
            <div className="flex flex-col items-start gap-3 sm:items-end">
              <span className={`px-3 py-1 rounded-full text-xs font-medium border ${getStatusStyle(currentStatus)}`}>
                {getStatusLabel(currentStatus)}
              </span>
              <div className="inline-flex rounded-lg border border-gray-200 p-1">
                <button
                  type="button"
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${activeView === 'overview' ? 'bg-black text-white' : 'text-gray-600 hover:bg-gray-50'}`}
                  onClick={() => setActiveView('overview')}
                >
                  Overview
                </button>
                <button
                  type="button"
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${activeView === 'audit' ? 'bg-black text-white' : 'text-gray-600 hover:bg-gray-50'}`}
                  onClick={() => setActiveView('audit')}
                >
                  Audit Trail
                </button>
              </div>
            </div>
          </div>
        </DialogHeader>

        <div className="mt-6 space-y-6">
          {loading && (
            <div className="p-2 bg-gray-50 border border-gray-200 rounded">
              <p className="text-xs text-gray-600">Loading invoice details…</p>
            </div>
          )}

          {activeView === 'overview' && (
            <>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div className="p-4 bg-gray-50 rounded-lg border border-gray-200">
                  <div className="flex items-center gap-2 mb-2">
                    <Package className="w-4 h-4 text-gray-600" />
                    <span className="text-xs text-gray-500 uppercase tracking-wide">
                      Vendor
                    </span>
                  </div>
                  <p className="text-base sm:text-lg font-semibold text-black">
                    {currentDetails?.vendorName || invoice.vendorName}
                  </p>
                </div>

                <div className="p-4 bg-gray-50 rounded-lg border border-gray-200">
                  <div className="flex items-center gap-2 mb-2">
                    <DollarSign className="w-4 h-4 text-gray-600" />
                    <span className="text-xs text-gray-500 uppercase tracking-wide">
                      Amount
                    </span>
                  </div>
                  <p className="text-base sm:text-lg font-semibold text-black">
                    {formatCurrency(currentDetails?.amount ?? invoice.amount)}
                  </p>
                </div>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <Calendar className="w-4 h-4 text-gray-600" />
                    <span className="text-xs text-gray-500 uppercase tracking-wide">
                      Invoice Date
                    </span>
                  </div>
                  <p className="text-sm text-black">
                    {new Date(currentDetails?.date || invoice.date).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'long',
                      day: 'numeric',
                    })}
                  </p>
                </div>

                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <Calendar className="w-4 h-4 text-gray-600" />
                    <span className="text-xs text-gray-500 uppercase tracking-wide">
                      Due Date
                    </span>
                  </div>
                  <p className="text-sm text-black">
                    {new Date(currentDetails?.dueDate || invoice.dueDate).toLocaleDateString('en-US', {
                      year: 'numeric',
                      month: 'long',
                      day: 'numeric',
                    })}
                  </p>
                </div>
              </div>

              <div>
                <div className="flex items-center gap-2 mb-2">
                  <FileText className="w-4 h-4 text-gray-600" />
                  <span className="text-xs text-gray-500 uppercase tracking-wide">
                    Description
                  </span>
                </div>
                <p className="text-sm text-black">
                  {currentDetails?.description || invoice.description || ''}
                </p>
              </div>

              <div>
                <span className="text-xs text-gray-500 uppercase tracking-wide">
                  Purchase Order
                </span>
                <p className="text-sm text-black mt-1">
                  {currentDetails?.poNumber || invoice.poNumber || 'No PO Number'}
                </p>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div className="p-3 bg-gray-50 rounded border border-gray-200">
                  <p className="text-xs text-gray-500">Currency</p>
                  <p className="text-sm text-black mt-1">{currentDetails?.currency || 'USD'}</p>
                </div>
                <div className="p-3 bg-gray-50 rounded border border-gray-200">
                  <p className="text-xs text-gray-500">Subtotal</p>
                  <p className="text-sm text-black mt-1">{formatCurrency(currentDetails?.subtotal ?? 0)}</p>
                </div>
                <div className="p-3 bg-gray-50 rounded border border-gray-200">
                  <p className="text-xs text-gray-500">Tax</p>
                  <p className="text-sm text-black mt-1">{formatCurrency(currentDetails?.tax ?? 0)}</p>
                </div>
              </div>

              {Array.isArray(currentDetails?.lines) && currentDetails.lines.length > 0 && (
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <FileText className="w-4 h-4 text-gray-600" />
                    <span className="text-xs text-gray-500 uppercase tracking-wide">
                      Line Items
                    </span>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm border border-gray-200 rounded">
                      <thead>
                        <tr className="bg-gray-50 text-xs text-gray-600">
                          <th className="text-left p-2">#</th>
                          <th className="text-left p-2">Description</th>
                          <th className="text-left p-2">Qty</th>
                          <th className="text-left p-2">Unit</th>
                          <th className="text-left p-2">Unit Price</th>
                          <th className="text-left p-2">Line Total</th>
                        </tr>
                      </thead>
                      <tbody>
                        {currentDetails.lines.map((line, index) => (
                          <tr key={index} className="border-t border-gray-100">
                            <td className="p-2 text-gray-700">{line.line_number ?? ''}</td>
                            <td className="p-2 text-gray-900">{line.description || ''}</td>
                            <td className="p-2 text-gray-700">{line.quantity ?? ''}</td>
                            <td className="p-2 text-gray-700">{line.unit_of_measure || ''}</td>
                            <td className="p-2 text-gray-700">{formatCurrency(line.unit_price ?? 0)}</td>
                            <td className="p-2 text-gray-900 font-medium">{formatCurrency(line.line_total ?? 0)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}

          {activeView === 'audit' && (
            <div className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                  <p className="text-xs uppercase tracking-wide text-gray-500">Agents Involved</p>
                  <p className="mt-1 text-lg font-semibold text-black">{auditSummary.agent_count ?? 0}</p>
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                  <p className="text-xs uppercase tracking-wide text-gray-500">Human Touchpoints</p>
                  <p className="mt-1 text-lg font-semibold text-black">{auditSummary.human_touchpoint_count ?? 0}</p>
                </div>
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
                  <p className="text-xs uppercase tracking-wide text-gray-500">Processing Time</p>
                  <p className="mt-1 text-lg font-semibold text-black">{formatDuration(auditSummary.total_processing_time_seconds)}</p>
                </div>
              </div>

              {auditLoading && (
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-600">
                  Loading audit trail…
                </div>
              )}

              {auditError && !auditLoading && (
                <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-800">
                  {auditError}
                </div>
              )}

              {!auditLoading && !auditError && auditEvents.length === 0 && (
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-4 text-sm text-gray-600">
                  No audit events are available for this invoice yet.
                </div>
              )}

              {!auditLoading && !auditError && auditEvents.length > 0 && (
                <div className="space-y-3 max-h-[28rem] overflow-y-auto pr-1">
                  {auditEvents.map((event) => {
                    const toneClass = TONE_STYLES[event.display_tone] || TONE_STYLES.state_transition;
                    return (
                      <div key={`${event.type}-${event.id}`} className={`rounded-lg border p-4 ${toneClass}`}>
                        <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                          <div>
                            {event.type === 'decision' ? (
                              <>
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="text-xs font-semibold uppercase tracking-wide">Decision</span>
                                  <span className="text-sm font-semibold">
                                    {event.agent_name || 'Unknown agent'}: {event.action || event.decision_type}
                                  </span>
                                  {typeof event.confidence === 'number' && (
                                    <span className="rounded-full border border-current/20 px-2 py-0.5 text-[11px] font-medium">
                                      {Math.round(event.confidence * 100)}% confidence
                                    </span>
                                  )}
                                </div>
                                <p className="mt-2 text-sm text-current/80">
                                  {event.reasoning || 'No reasoning text recorded.'}
                                </p>
                                {event.overridden && (
                                  <div className="mt-3 rounded-md border border-red-300 bg-white/70 p-2 text-xs text-red-900">
                                    Human override: {event.override_reason || 'Resolved during review.'}
                                  </div>
                                )}
                              </>
                            ) : (
                              <>
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="text-xs font-semibold uppercase tracking-wide">State Change</span>
                                  <span className="text-sm font-semibold">
                                    {(event.before_state || 'Start')} to {event.after_state || 'Unknown'}
                                  </span>
                                  <span className="rounded-full border border-current/20 px-2 py-0.5 text-[11px] font-medium">
                                    {event.event_type || 'transition'}
                                  </span>
                                </div>
                                <p className="mt-2 text-sm text-current/80">
                                  {event.reason || 'No transition note recorded.'}
                                </p>
                                {(event.actor_type || event.actor_id) && (
                                  <p className="mt-2 text-xs text-current/70">
                                    Actor: {event.actor_type || 'system'}{event.actor_id ? ` (${event.actor_id})` : ''}
                                  </p>
                                )}
                              </>
                            )}
                          </div>
                          <p className="text-xs text-current/70 whitespace-nowrap">
                            {formatTimestamp(event.timestamp)}
                          </p>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          <div className="flex gap-3 pt-4 border-t border-gray-200">
            <button
              className="flex-1 px-4 py-2.5 bg-black text-white rounded-lg font-medium hover:bg-gray-800 transition-colors"
              onClick={onClose}
            >
              Close
            </button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default InvoiceDetailModal;
