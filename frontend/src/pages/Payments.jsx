import React, { useEffect, useMemo, useState } from 'react';
import Sidebar from '../components/Sidebar';
import {
  fetchPayableInvoices,
  fetchPendingPaymentConfirmations,
  fetchPaymentHistory,
  fetchVendors,
  formatCurrency,
  getStatusLabel,
  routePaymentBatch,
  confirmPayment,
  cancelPayment,
} from '../services/api';
import { loadStripe } from '@stripe/stripe-js';
import { Elements, CardElement, useElements, useStripe } from '@stripe/react-stripe-js';
import { useLiveRefresh } from '../lib/useLiveRefresh';
import { getPageCache, setPageCache } from '../lib/pageCache';

const stripePromise = loadStripe(process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY || '');

const formatPaymentDateTime = (value) => {
  if (!value) return 'Unknown';
  try {
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    }).format(new Date(value));
  } catch (_) {
    return String(value);
  }
};

const getPaymentStatusMeta = (status) => {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'succeeded') {
    return { label: 'Succeeded', className: 'bg-green-50 text-green-700 border-green-200' };
  }
  if (normalized === 'requires_confirmation') {
    return { label: 'Awaiting Card Confirmation', className: 'bg-amber-50 text-amber-700 border-amber-200' };
  }
  if (normalized === 'failed') {
    return { label: 'Failed', className: 'bg-red-50 text-red-700 border-red-200' };
  }
  if (normalized === 'canceled') {
    return { label: 'Canceled', className: 'bg-gray-100 text-gray-700 border-gray-200' };
  }
  return { label: status || 'Unknown', className: 'bg-gray-100 text-gray-700 border-gray-200' };
};

const PayerForm = ({ onSubmit, disabled }) => {
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [line1, setLine1] = useState('');
  const [city, setCity] = useState('');
  const [state, setState] = useState('');
  const [postal_code, setPostalCode] = useState('');
  const [country, setCountry] = useState('US');

  const handle = (e) => {
    e.preventDefault();
    onSubmit({ email, name, address: { line1, city, state, postal_code, country } });
  };

  return (
    <form onSubmit={handle} className="space-y-3">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="Email" type="email" value={email} onChange={e => setEmail(e.target.value)} required />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="Name" value={name} onChange={e => setName(e.target.value)} required />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm sm:col-span-2" placeholder="Address line 1" value={line1} onChange={e => setLine1(e.target.value)} />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="City" value={city} onChange={e => setCity(e.target.value)} />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="State" value={state} onChange={e => setState(e.target.value)} />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="Postal Code" value={postal_code} onChange={e => setPostalCode(e.target.value)} />
        <input className="border border-gray-300 rounded px-3 py-2 text-sm" placeholder="Country (US)" value={country} onChange={e => setCountry(e.target.value)} />
      </div>
      <button type="submit" disabled={disabled} className="px-4 py-2 border border-black rounded-lg text-sm font-medium text-black hover:bg-gray-50 disabled:opacity-60">Save payer</button>
    </form>
  );
};

const PayBox = ({ selection, currency, pendingConfirmations = [], onPaid, onError, onApprovalPending, customer, payerReady }) => {
  const stripe = useStripe();
  const elements = useElements();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const total = selection.reduce((acc, s) => acc + (s.amount || 0), 0);

  const confirmExistingIntent = async ({ clientSecret, paymentIntentId, billingCustomer }) => {
    if (!clientSecret) throw new Error('No client secret');
    if (!stripe || !elements) throw new Error('Payment card details are not ready yet. Please try again.');
    const result = await stripe.confirmCardPayment(clientSecret, {
      payment_method: {
        card: elements.getElement(CardElement),
        billing_details: billingCustomer,
      }
    });
    if (result.error) {
      setError(result.error.message || 'Payment failed');
      if (paymentIntentId) {
        try { await cancelPayment({ paymentIntentId }); } catch (_) {}
      }
      if (onError) onError(result.error.message || 'Payment failed');
      return;
    }
    if (result.paymentIntent && result.paymentIntent.status === 'succeeded') {
      try {
        await confirmPayment({ paymentIntentId: result.paymentIntent.id });
      } catch (_) {}
      onPaid(result.paymentIntent.id);
      return;
    }
    setError('Payment not completed.');
    if (paymentIntentId) {
      try { await cancelPayment({ paymentIntentId }); } catch (_) {}
    }
    if (onError) onError('Payment not completed.');
  };

  const handlePay = async () => {
    setSubmitting(true);
    setError('');
    try {
      const routeResult = await routePaymentBatch({
        invoiceIds: selection.map(s => s.id),
        currency,
        customer,
        saveMethod: false
      });
      if ((routeResult?.status || '').toLowerCase() === 'pending_approval') {
        const riskLevel = routeResult?.analysis?.risk_level || 'medium';
        const requestId =
          routeResult?.authorization_request_id ||
          routeResult?.authorizationRequestId ||
          routeResult?.authorization_request?.id;
        const message = requestId
          ? `This ${riskLevel} risk batch is waiting for approval before payment can continue. Authorization request ${requestId} is now in the authorization queue. No charge has been attempted yet.`
          : `This ${riskLevel} risk batch is waiting for approval before payment can continue.`;
        setError('');
        if (onApprovalPending) onApprovalPending(message, routeResult);
        return;
      }
      const paymentResult = routeResult?.payment_result || routeResult?.paymentResult || routeResult;
      const { clientSecret, paymentIntentId, error: serverError } = paymentResult || {};
      if (!clientSecret) throw new Error(serverError || 'No client secret');
      await confirmExistingIntent({
        clientSecret,
        paymentIntentId,
        billingCustomer: customer,
      });
    } catch (e) {
      const message = e?.message || 'We could not complete that request. Please try again.';
      const lowered = String(message).toLowerCase();
      if (lowered.includes('still being evaluated') || lowered.includes('still in progress')) {
        setError('');
        if (onApprovalPending) onApprovalPending(`${message} No charge has been attempted yet.`);
      } else {
        setError(message);
        if (onError) onError(message);
      }
    } finally {
      setSubmitting(false);
    }
  };

  const handleResumePending = async (confirmation) => {
    setSubmitting(true);
    setError('');
    try {
      await confirmExistingIntent({
        clientSecret: confirmation.clientSecret || confirmation?.paymentResult?.clientSecret,
        paymentIntentId: confirmation.paymentIntentId || confirmation?.paymentResult?.paymentIntentId,
        billingCustomer: (payerReady ? customer : null) || confirmation.customer || {},
      });
    } catch (e) {
      const message = e?.message || 'We could not resume that payment. Please try again.';
      setError(message);
      if (onError) onError(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="space-y-3">
      <div className="p-3 border border-gray-200 rounded">
        <CardElement options={{ hidePostalCode: true }} />
      </div>
      {pendingConfirmations.length > 0 && (
        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-[0.14em] text-gray-500">Pending Payment Confirmations</div>
          {pendingConfirmations.map((confirmation) => (
            <div key={confirmation.authorizationRequestId} className="border border-amber-200 bg-amber-50 rounded-lg p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-medium text-black">{confirmation.primaryVendorName || 'Unknown Vendor'}</div>
                  <div className="text-xs text-gray-600 mt-1">
                    {formatCurrency(confirmation.totalAmount || 0)} ({confirmation.currency || 'USD'}) · {confirmation.invoiceCount || 0} invoice{Number(confirmation.invoiceCount || 0) === 1 ? '' : 's'}
                  </div>
                  <div className="text-xs text-gray-600 mt-1 break-all">
                    Authorization {confirmation.authorizationRequestId}
                  </div>
                </div>
                <button
                  type="button"
                  disabled={submitting || !confirmation.clientSecret}
                  className="px-3 py-2 bg-black text-white rounded-lg text-xs font-medium hover:bg-gray-800 disabled:opacity-60"
                  onClick={() => handleResumePending(confirmation)}
                >
                  Resume payment
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
      {!payerReady && (
        <div className="text-xs text-amber-700">
          Save the payer details before starting payment or approval routing. Pending confirmations can also use the stored payer details from the authorization request.
        </div>
      )}
      {error && <div className="text-sm text-red-600">{error}</div>}
      <button onClick={handlePay} disabled={submitting || total <= 0 || !payerReady}
        className="w-full px-4 py-2 bg-black text-white rounded-lg text-sm font-medium hover:bg-gray-800 disabled:opacity-60">
        {submitting ? 'Processing...' : `Pay ${formatCurrency(total)} (${currency || 'USD'})`}
      </button>
    </div>
  );
};

const Payments = () => {
  const cache = getPageCache('payments');
  const [activeNav] = useState('payments');
  const [vendors, setVendors] = useState(cache?.vendors || []);
  const [selectedVendorId, setSelectedVendorId] = useState(cache?.selectedVendorId || '');
  const [currency, setCurrency] = useState(cache?.currency || '');
  const [items, setItems] = useState(cache?.items || []);
  const [pendingConfirmations, setPendingConfirmations] = useState(cache?.pendingConfirmations || []);
  const [paymentHistory, setPaymentHistory] = useState(cache?.paymentHistory || []);
  const [loading, setLoading] = useState(!cache);
  const [error, setError] = useState(cache?.error || null);
  const [selected, setSelected] = useState({});
  const [customer, setCustomer] = useState(null);
  const [paid, setPaid] = useState(false);
  const [payError, setPayError] = useState('');
  const [paymentNotice, setPaymentNotice] = useState('');

  useEffect(() => {
    setPageCache('payments', {
      vendors,
      selectedVendorId,
      currency,
      items,
      pendingConfirmations,
      paymentHistory,
      error,
    });
  }, [vendors, selectedVendorId, currency, items, pendingConfirmations, paymentHistory, error]);

  const grouped = useMemo(() => {
    const map = new Map();
    for (const it of items) {
      const key = it.vendorId || 'unknown';
      const name = it.vendorName || 'Unknown Vendor';
      if (!map.has(key)) map.set(key, { vendorId: key, vendorName: name, list: [] });
      map.get(key).list.push(it);
    }
    return Array.from(map.values()).sort((a, b) => a.vendorName.localeCompare(b.vendorName));
  }, [items]);

  const selectionArray = useMemo(() => items.filter(i => selected[i.id]), [items, selected]);
  const selectionCurrency = useMemo(() => {
    const set = new Set(selectionArray.map(i => i.currency).filter(Boolean));
    return set.size === 1 ? selectionArray[0]?.currency : '';
  }, [selectionArray]);
  const pendingVendorSummary = useMemo(() => {
    const names = Array.from(new Set(
      pendingConfirmations
        .map((item) => item.primaryVendorName)
        .filter(Boolean)
    ));
    return names.join(', ');
  }, [pendingConfirmations]);
  const payerReady = Boolean(customer?.email && customer?.name);

  const load = async () => {
    try {
      if (!items.length && !vendors.length) {
        setLoading(true);
      }
      const [vData, invData, pendingData, historyData] = await Promise.all([
        fetchVendors(),
        fetchPayableInvoices({ vendorId: selectedVendorId || undefined, currency: currency || undefined, limit: 200 }),
        fetchPendingPaymentConfirmations({ limit: 20 }),
        fetchPaymentHistory({ vendorId: selectedVendorId || undefined, currency: currency || undefined, limit: 20 }),
      ]);
      setVendors(vData);
      setItems(invData);
      setPendingConfirmations(pendingData);
      setPaymentHistory(historyData);
      setSelected((current) => {
        const next = { ...current };
        const allowed = new Set(invData.map((item) => item.id));
        Object.keys(next).forEach((id) => {
          if (!allowed.has(id)) delete next[id];
        });
        return next;
      });
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useLiveRefresh(load, [selectedVendorId, currency]);

  const toggle = (id) => setSelected(s => ({ ...s, [id]: !s[id] }));
  const selectAllForVendor = (vendorId, check) => {
    const updates = {};
    for (const i of items) if ((i.vendorId || 'unknown') === vendorId) updates[i.id] = check;
    setSelected(s => ({ ...s, ...updates }));
  };

  const onSubmitPayer = (c) => setCustomer(c);

  return (
    <div className="min-h-screen bg-white">
      {/* BrandBar removed */}
      <Sidebar activeItem={activeNav} />

      <div className="lg:ml-[220px] p-4 sm:p-6 lg:p-8 transition-all">
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
          <div className="flex items-center gap-3">
            <h1 className="text-2xl sm:text-3xl font-semibold text-black">Payments</h1>
          </div>
          <div className="flex items-center gap-3 w-full sm:w-auto">
            <select className="border border-gray-200 rounded-lg text-sm px-3 py-2" value={selectedVendorId} onChange={e => setSelectedVendorId(e.target.value)}>
              <option value="">All vendors</option>
              {vendors.map(v => (
                <option key={v.id || v[0]} value={(v.id || v[0])}>{v.name || v[1]}</option>
              ))}
            </select>
            <input className="border border-gray-200 rounded-lg text-sm px-3 py-2 w-36" placeholder="Currency (e.g., USD)" value={currency} onChange={e => setCurrency(e.target.value)} />
            <button className="px-4 py-2 border border-black rounded-full text-sm font-medium text-black hover:bg-gray-50" onClick={load}>Refresh</button>
          </div>
        </div>

        {loading && (
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-gray-600">Loading payable invoices...</p>
          </div>
        )}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-6">
            <p className="text-sm text-red-600">{error}</p>
          </div>
        )}
        {pendingConfirmations.length > 0 && (
          <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 mb-6">
            <p className="text-sm font-medium text-amber-900">
              {pendingConfirmations.length} payment{pendingConfirmations.length === 1 ? '' : 's'} {pendingConfirmations.length === 1 ? 'is' : 'are'} waiting for card confirmation.
            </p>
            <p className="text-sm text-amber-800 mt-1">
              These invoices are no longer in the payable list because they are already in `payment_pending`. Resume them from the Payment Method panel on the right.
              {pendingVendorSummary ? ` Vendors: ${pendingVendorSummary}.` : ''}
            </p>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Invoices list (2/3) */}
          <div className="lg:col-span-2 space-y-4">
            {!loading && grouped.length === 0 && (
              <div className="bg-gray-50 border border-gray-200 rounded-xl p-4">
                <p className="text-sm text-gray-700">No invoices are currently eligible for a new payment.</p>
                {pendingConfirmations.length > 0 && (
                  <p className="text-sm text-gray-600 mt-1">
                    There {pendingConfirmations.length === 1 ? 'is' : 'are'} {pendingConfirmations.length} payment{pendingConfirmations.length === 1 ? '' : 's'} awaiting confirmation in the Payment Method panel.
                  </p>
                )}
              </div>
            )}
            {grouped.map(group => (
              <div key={group.vendorId} className="bg-white border border-gray-200 rounded-xl overflow-hidden">
                <div className="p-3 sm:p-4 flex items-center justify-between border-b border-gray-200">
                  <div className="font-medium text-black text-sm">{group.vendorName}</div>
                  <div className="flex items-center gap-2 text-xs">
                    <label className="flex items-center gap-1">
                      <input type="checkbox" onChange={e => selectAllForVendor(group.vendorId, e.target.checked)} />
                      <span>Select all</span>
                    </label>
                  </div>
                </div>
                <div className="divide-y divide-gray-100">
                  {group.list.map(inv => (
                    <div key={inv.id} className="p-3 sm:p-4 flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <input type="checkbox" checked={!!selected[inv.id]} onChange={() => toggle(inv.id)} />
                        <div className="w-16 text-xs text-gray-500">{inv.displayDate}</div>
                        <div className="text-sm text-black font-medium">{formatCurrency(inv.amount)}</div>
                        <div className="text-xs text-gray-600">{inv.invoiceNumber}</div>
                        <div className="text-xs text-gray-600">{getStatusLabel(inv.status)}</div>
                      </div>
                      <div className="text-xs text-gray-500">{inv.currency}</div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Summary + Payment (1/3) */}
          <div className="bg-white border border-gray-200 rounded-xl p-4 sm:p-6 space-y-4">
            <div>
              <h2 className="text-base font-medium text-black mb-2">Summary</h2>
              <div className="text-sm text-gray-600">Selected invoices: {selectionArray.length}</div>
              <div className="text-sm text-gray-600">Currency: {selectionCurrency || '-'}</div>
              <div className="text-sm font-semibold text-black mt-1">
                Total: {formatCurrency(selectionArray.reduce((acc, s) => acc + (s.amount || 0), 0))}
              </div>
              {selectionArray.length > 0 && selectionArray.some(s => s.currency !== selectionCurrency) && (
                <div className="text-xs text-red-600 mt-2">Mixed currency selection is not allowed.</div>
              )}
            </div>

            <div>
              <h2 className="text-base font-medium text-black mb-2">Payer</h2>
              <PayerForm onSubmit={onSubmitPayer} />
              {payerReady && customer && (
                <div className="mt-2 text-xs text-green-700">
                  Saved payer: {customer.name} ({customer.email})
                </div>
              )}
            </div>

            <div>
              <h2 className="text-base font-medium text-black mb-2">Payment Method</h2>
              {!process.env.REACT_APP_STRIPE_PUBLISHABLE_KEY && (
                <div className="text-xs text-red-600 mb-2">Missing REACT_APP_STRIPE_PUBLISHABLE_KEY in environment.</div>
              )}
              <Elements stripe={stripePromise}>
                <PayBox
                  selection={selectionArray}
                  currency={selectionCurrency || currency || 'USD'}
                  pendingConfirmations={pendingConfirmations}
                  onPaid={() => {
                    setPaid(true);
                    setPayError('');
                    setPaymentNotice('');
                    setSelected({});
                    load();
                  }}
                  onError={(message) => {
                    setPaid(false);
                    setPaymentNotice('');
                    setPayError(message);
                  }}
                  onApprovalPending={(message) => {
                    setPaid(false);
                    setPayError('');
                    setPaymentNotice(message);
                    setSelected({});
                    load();
                  }}
                  customer={customer || {}}
                  payerReady={payerReady}
                />
              </Elements>
            </div>

            {paid && (
              <div className="p-3 bg-green-50 border border-green-200 rounded text-sm text-green-700">Payment succeeded. Invoices have been marked as paid.</div>
            )}
            {paymentNotice && (
              <div className="p-3 bg-amber-50 border border-amber-200 rounded text-sm text-amber-700">{paymentNotice}</div>
            )}
            {payError && (
              <div className="p-3 bg-red-50 border border-red-200 rounded text-sm text-red-700">{payError}</div>
            )}
          </div>
        </div>

        <div className="bg-white border border-gray-200 rounded-xl p-4 sm:p-6 mt-4">
          <div className="flex items-center justify-between gap-3 mb-4">
            <div>
              <h2 className="text-base font-medium text-black">Payment History</h2>
              <p className="text-sm text-gray-600 mt-1">Recent payment attempts and completed payments for the current filter.</p>
            </div>
          </div>
          {paymentHistory.length === 0 ? (
            <p className="text-sm text-gray-600">No payment history found for the current filters.</p>
          ) : (
            <div className="space-y-3">
              {paymentHistory.map((payment) => {
                const statusMeta = getPaymentStatusMeta(payment.status);
                return (
                  <div key={payment.paymentId} className="border border-gray-200 rounded-xl p-4">
                    <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-3">
                      <div className="space-y-2">
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-sm font-medium text-black">{payment.primaryVendorName || 'Unknown Vendor'}</div>
                          <span className={`inline-flex items-center rounded-full border px-2 py-1 text-xs font-medium ${statusMeta.className}`}>
                            {statusMeta.label}
                          </span>
                        </div>
                        <div className="text-xs text-gray-600">
                          {payment.invoiceCount || 0} invoice{Number(payment.invoiceCount || 0) === 1 ? '' : 's'}
                          {payment.invoiceNumbers?.length ? ` / ${payment.invoiceNumbers.join(', ')}` : ''}
                        </div>
                        <div className="text-xs text-gray-600">
                          Payer: {payment.customerEmail || 'Unknown'} / Created: {formatPaymentDateTime(payment.createdAt)}
                        </div>
                        {payment.paymentIntentId && (
                          <div className="text-xs text-gray-500 break-all">
                            Payment Intent: {payment.paymentIntentId}
                          </div>
                        )}
                      </div>
                      <div className="text-left lg:text-right">
                        <div className="text-base font-semibold text-black">{formatCurrency(payment.amount || 0)}</div>
                        <div className="text-xs text-gray-600 mt-1">{String(payment.currency || 'USD').toUpperCase()}</div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Payments;
