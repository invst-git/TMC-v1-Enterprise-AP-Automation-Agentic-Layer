// API Service for The Matching Company Dashboard
// Connects to Flask backend at http://localhost:5000 (via CRA proxy) or custom base via REACT_APP_API_URL

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api';

const mapApiErrorMessage = (message) => {
  const lowered = String(message || '').toLowerCase();

  if (!lowered) return 'We could not complete that request. Please try again.';
  if (lowered.includes('failed to fetch') || lowered.includes('networkerror')) {
    return 'The app cannot reach the server right now. Please check the connection and try again.';
  }
  if (lowered.includes('duplicate invoice')) {
    return 'This invoice was already received earlier, so it was blocked to prevent duplicate payment.';
  }
  if (lowered.includes('duplicate payment')) {
    return 'A duplicate invoice was detected in this payment request, so the payment was blocked for safety.';
  }
  if (lowered.includes('still being evaluated') || lowered.includes('still in progress')) {
    return 'This payment batch is still being evaluated. Please wait a moment and try again.';
  }
  if (lowered.includes('already paid')) {
    return 'This invoice has already been paid.';
  }
  if (lowered.includes('already pending')) {
    return 'This invoice is already in an active payment flow.';
  }
  if (lowered.includes('mixed currency')) {
    return 'Invoices with different currencies cannot be paid together.';
  }
  if (lowered.includes('not ready to be paid') || lowered.includes('not eligible for payment')) {
    return 'This invoice is not ready to be paid yet.';
  }
  if (lowered.includes('not found')) {
    return 'The requested record could not be found.';
  }
  return String(message || 'We could not complete that request. Please try again.');
};

const nativeFetch = (...args) => window.fetch(...args);
const fetch = async (...args) => {
  try {
    return await nativeFetch(...args);
  } catch (error) {
    throw new Error(mapApiErrorMessage(error?.message || error));
  }
};

// Helper function to handle API responses
const handleResponse = async (response) => {
  if (!response.ok) {
    // try to parse JSON error; fallback to text
    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      const error = await response.json().catch(() => ({ message: 'An error occurred' }));
      throw new Error(mapApiErrorMessage(error.error || error.message || `HTTP error! status: ${response.status}`));
    }
    const text = await response.text().catch(() => 'An error occurred');
    throw new Error(mapApiErrorMessage(text || `HTTP error! status: ${response.status}`));
  }
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) return response.json();
  return response.text();
};

const getInFlight = new Map();

const getJson = async (url) => {
  if (getInFlight.has(url)) {
    return getInFlight.get(url);
  }

  const request = fetch(url)
    .then(handleResponse)
    .finally(() => {
      getInFlight.delete(url);
    });

  getInFlight.set(url, request);
  return request;
};

export const subscribeToLiveUpdates = ({
  onOpen,
  onChange,
  onHeartbeat,
  onError,
} = {}) => {
  if (typeof window === 'undefined' || typeof window.EventSource === 'undefined') {
    const intervalId = window.setInterval(() => {
      if (typeof onHeartbeat === 'function') {
        onHeartbeat({ eventType: 'heartbeat', serverTime: new Date().toISOString() });
      }
    }, 1000);
    return {
      close() {
        window.clearInterval(intervalId);
      },
    };
  }

  const source = new window.EventSource(`${API_BASE_URL}/live/stream`);
  const parsePayload = (event) => {
    try {
      return JSON.parse(event.data);
    } catch (_) {
      return { eventType: 'unknown', serverTime: new Date().toISOString() };
    }
  };

  source.addEventListener('ready', (event) => {
    if (typeof onOpen === 'function') onOpen(parsePayload(event));
  });
  source.addEventListener('change', (event) => {
    if (typeof onChange === 'function') onChange(parsePayload(event));
  });
  source.addEventListener('heartbeat', (event) => {
    if (typeof onHeartbeat === 'function') onHeartbeat(parsePayload(event));
  });
  source.onerror = (event) => {
    if (typeof onError === 'function') onError(event);
  };

  return {
    close() {
      source.close();
    },
  };
};

// Extract <pre>...</pre> content from HTML returned by Flask templates
const extractLogsFromHtml = (html) => {
  if (!html) return '';
  const match = html.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  if (!match) return '';
  // Basic HTML entity decoding for common cases
  let text = match[1]
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
  return text.trim();
};

// Fetch dashboard statistics
export const fetchDashboardStats = async () => {
  return getJson(`${API_BASE_URL}/dashboard/stats`);
};

// Fetch graph data for last 30 days
export const fetchGraphData = async () => {
  return getJson(`${API_BASE_URL}/dashboard/graph-data`);
};

// Fetch recent invoices
export const fetchRecentInvoices = async (limit = 10) => {
  return getJson(`${API_BASE_URL}/invoices/recent?limit=${limit}`);
};

// Fetch single invoice by ID (includes extended fields)
export const fetchInvoiceById = async (invoiceId) => {
  return getJson(`${API_BASE_URL}/invoices/${invoiceId}`);
};

export const fetchInvoiceAuditTrail = async (invoiceId, { limit = 500 } = {}) => {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/invoices/${invoiceId}/audit-trail?${params.toString()}`);
};

// Fetch all vendors
export const fetchVendors = async () => {
  return getJson(`${API_BASE_URL}/vendors`);
};

// Fetch lightweight vendor options for filters/dropdowns
export const fetchVendorOptions = async () => {
  return getJson(`${API_BASE_URL}/vendors/options`);
};

// Fetch vendor statistics
export const fetchVendorStats = async () => {
  return getJson(`${API_BASE_URL}/vendors/stats`);
};

// Fetch single vendor by ID (now includes purchase orders and invoice list)
export const fetchVendorById = async (vendorId) => {
  return getJson(`${API_BASE_URL}/vendors/${vendorId}`);
};

// Create vendor
export const createVendor = async ({ name, taxId, contact, address }) => {
  const response = await fetch(`${API_BASE_URL}/vendors`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, taxId, contact, address })
  });
  return await handleResponse(response);
};

// Delete vendor and related records
export const deleteVendor = async (vendorId) => {
  const response = await fetch(`${API_BASE_URL}/vendors/${vendorId}`, { method: 'DELETE' });
  return await handleResponse(response);
};

// Mentions typeahead for @
export const fetchVendorMentions = async ({ vendorId, kind = 'invoices', q = '', limit = 10 }) => {
  const params = new URLSearchParams();
  params.set('kind', kind);
  if (q) params.set('q', q);
  params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/vendors/${vendorId}/mentions?${params.toString()}`);
};

// Chat lifecycle
export const startVendorChat = async ({ vendorId, title, reuseLatest = true }) => {
  const response = await fetch(`${API_BASE_URL}/vendors/${vendorId}/chat/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title: title || undefined, reuseLatest })
  });
  return await handleResponse(response);
};

export const getVendorChatMessages = async ({ vendorId, chatId, limit = 50, before }) => {
  const params = new URLSearchParams();
  params.set('limit', String(limit));
  if (before) params.set('before', before);
  return getJson(`${API_BASE_URL}/vendors/${vendorId}/chat/${chatId}/messages?${params.toString()}`);
};

export const sendVendorChatMessage = async ({ vendorId, chatId, prompt, tags }) => {
  const response = await fetch(`${API_BASE_URL}/vendors/${vendorId}/chat/${chatId}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt, tags })
  });
  return await handleResponse(response);
};

export const listVendorChats = async ({ vendorId, limit = 20 }) => {
  const params = new URLSearchParams();
  params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/vendors/${vendorId}/chat?${params.toString()}`);
};

// Fetch single purchase order by ID
export const fetchPurchaseOrderById = async (poId) => {
  return getJson(`${API_BASE_URL}/purchase-orders/${poId}`);
};

// Fetch exception invoices with optional filters
export const fetchExceptionInvoices = async ({ vendorId, status, limit = 100 } = {}) => {
  const params = new URLSearchParams();
  if (vendorId) params.set('vendor_id', vendorId);
  if (status) params.set('status', status);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/invoices/exceptions?${params.toString()}`);
};

// Fetch payable invoices (eligible for payment)
export const fetchPayableInvoices = async ({ vendorId, currency, limit = 200 } = {}) => {
  const params = new URLSearchParams();
  if (vendorId) params.set('vendor_id', vendorId);
  if (currency) params.set('currency', currency);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/invoices/payable?${params.toString()}`);
};

export const fetchPendingPaymentConfirmations = async ({ limit = 25 } = {}) => {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/payments/pending-confirmations?${params.toString()}`);
};

export const fetchPaymentHistory = async ({ vendorId, currency, limit = 25 } = {}) => {
  const params = new URLSearchParams();
  if (vendorId) params.set('vendor_id', vendorId);
  if (currency) params.set('currency', currency);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/payments/history?${params.toString()}`);
};

export const fetchAgentOverview = async () => {
  return getJson(`${API_BASE_URL}/agent/overview`);
};

export const fetchAgentOperationsMetrics = async ({ days = 30 } = {}) => {
  const params = new URLSearchParams();
  if (days) params.set('days', String(days));
  return getJson(`${API_BASE_URL}/agent/operations/metrics?${params.toString()}`);
};

export const fetchAgentSourceDocuments = async ({
  ingestionStatus,
  segmentationStatus,
  extractionStatus,
  vendorId,
  sourceType,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (ingestionStatus) params.set('ingestion_status', ingestionStatus);
  if (segmentationStatus) params.set('segmentation_status', segmentationStatus);
  if (extractionStatus) params.set('extraction_status', extractionStatus);
  if (vendorId) params.set('vendor_id', vendorId);
  if (sourceType) params.set('source_type', sourceType);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/source-documents?${params.toString()}`);
};

export const fetchAgentSourceDocumentDetail = async (sourceDocumentId) => {
  return getJson(`${API_BASE_URL}/agent/source-documents/${sourceDocumentId}`);
};

export const fetchAgentWorkflowStates = async ({
  entityType,
  currentState,
  currentStage,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (entityType) params.set('entity_type', entityType);
  if (currentState) params.set('current_state', currentState);
  if (currentStage) params.set('current_stage', currentStage);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/workflow-states?${params.toString()}`);
};

export const fetchAgentWorkflowHistory = async ({ entityType, entityId, limit = 100 }) => {
  const params = new URLSearchParams();
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/workflow-history/${entityType}/${entityId}?${params.toString()}`);
};

export const fetchAgentTasks = async ({
  status,
  taskType,
  entityType,
  entityId,
  sourceDocumentId,
  retriesOnly = false,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (status) params.set('status', status);
  if (taskType) params.set('task_type', taskType);
  if (entityType) params.set('entity_type', entityType);
  if (entityId) params.set('entity_id', entityId);
  if (sourceDocumentId) params.set('source_document_id', sourceDocumentId);
  if (retriesOnly) params.set('retries_only', 'true');
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/tasks?${params.toString()}`);
};

export const fetchAgentDecisions = async ({
  entityType,
  entityId,
  taskId,
  sourceDocumentId,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (entityType) params.set('entity_type', entityType);
  if (entityId) params.set('entity_id', entityId);
  if (taskId) params.set('task_id', taskId);
  if (sourceDocumentId) params.set('source_document_id', sourceDocumentId);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/decisions?${params.toString()}`);
};

export const fetchAgentReviewQueue = async ({
  status,
  activeOnly = false,
  queueName,
  entityType,
  entityId,
  sourceDocumentId,
  invoiceId,
  assignedTo,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (status) params.set('status', status);
  if (activeOnly) params.set('active_only', 'true');
  if (queueName) params.set('queue_name', queueName);
  if (entityType) params.set('entity_type', entityType);
  if (entityId) params.set('entity_id', entityId);
  if (sourceDocumentId) params.set('source_document_id', sourceDocumentId);
  if (invoiceId) params.set('invoice_id', invoiceId);
  if (assignedTo) params.set('assigned_to', assignedTo);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/review-queue?${params.toString()}`);
};

export const fetchAgentReviewQueueCounts = async () => {
  return getJson(`${API_BASE_URL}/agent/review-queue/counts`);
};

export const assignAgentReviewItem = async ({ reviewItemId, reviewer }) => {
  const response = await fetch(`${API_BASE_URL}/agent/review-queue/${reviewItemId}/assign`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reviewer }),
  });
  return await handleResponse(response);
};

export const resolveAgentReviewItem = async ({
  reviewItemId,
  reviewer,
  action,
  resolutionNotes,
  selectedPoId,
}) => {
  const response = await fetch(`${API_BASE_URL}/agent/review-queue/${reviewItemId}/resolve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reviewer, action, resolutionNotes, selectedPoId }),
  });
  return await handleResponse(response);
};

export const rejectAgentReviewItem = async ({
  reviewItemId,
  reviewer,
  resolutionNotes,
}) => {
  const response = await fetch(`${API_BASE_URL}/agent/review-queue/${reviewItemId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ reviewer, resolutionNotes }),
  });
  return await handleResponse(response);
};

export const fetchAgentSlaConfigs = async ({ entityType, activeOnly = false } = {}) => {
  const params = new URLSearchParams();
  if (entityType) params.set('entity_type', entityType);
  if (activeOnly) params.set('active_only', 'true');
  return getJson(`${API_BASE_URL}/agent/sla-configs?${params.toString()}`);
};

export const fetchAgentSlaBreaches = async ({
  entityType,
  currentState,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (entityType) params.set('entity_type', entityType);
  if (currentState) params.set('current_state', currentState);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/sla-breaches?${params.toString()}`);
};

export const fetchVendorCommunications = async ({
  status,
  direction,
  vendorId,
  invoiceId,
  sourceDocumentId,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (status) params.set('status', status);
  if (direction) params.set('direction', direction);
  if (vendorId) params.set('vendor_id', vendorId);
  if (invoiceId) params.set('invoice_id', invoiceId);
  if (sourceDocumentId) params.set('source_document_id', sourceDocumentId);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/vendor-communications?${params.toString()}`);
};

export const fetchVendorCommunicationById = async (communicationId) => {
  return getJson(`${API_BASE_URL}/agent/vendor-communications/${communicationId}`);
};

export const createVendorCommunicationDraft = async ({
  invoiceId,
  reviewReason,
  sourceDocumentId,
}) => {
  const response = await fetch(`${API_BASE_URL}/agent/vendor-communications/draft`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ invoiceId, reviewReason, sourceDocumentId }),
  });
  return await handleResponse(response);
};

export const approveVendorCommunication = async ({ communicationId, approvedBy }) => {
  const response = await fetch(`${API_BASE_URL}/agent/vendor-communications/${communicationId}/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ approvedBy }),
  });
  return await handleResponse(response);
};

export const rejectVendorCommunication = async ({ communicationId, rejectedBy }) => {
  const response = await fetch(`${API_BASE_URL}/agent/vendor-communications/${communicationId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rejectedBy }),
  });
  return await handleResponse(response);
};

export const markVendorCommunicationSent = async ({ communicationId, sentBy }) => {
  const response = await fetch(`${API_BASE_URL}/agent/vendor-communications/${communicationId}/mark-sent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sentBy }),
  });
  return await handleResponse(response);
};

export const fetchPaymentAuthorizations = async ({
  approvalStatus,
  riskLevel,
  limit = 100,
} = {}) => {
  const params = new URLSearchParams();
  if (approvalStatus) params.set('approval_status', approvalStatus);
  if (riskLevel) params.set('risk_level', riskLevel);
  if (limit) params.set('limit', String(limit));
  return getJson(`${API_BASE_URL}/agent/payments/authorizations?${params.toString()}`);
};

export const fetchPaymentAuthorizationById = async (requestId) => {
  return getJson(`${API_BASE_URL}/agent/payments/authorizations/${requestId}`);
};

export const requestPaymentAuthorization = async ({
  invoiceIds,
  customer,
  currency,
  saveMethod,
  requestedBy,
}) => {
  const response = await fetch(`${API_BASE_URL}/agent/payments/authorize`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ invoiceIds, customer, currency, saveMethod, requestedBy }),
  });
  return await handleResponse(response);
};

export const routePaymentBatch = async ({
  invoiceIds,
  customer,
  currency,
  saveMethod,
  requestedBy,
}) => {
  const response = await fetch(`${API_BASE_URL}/agent/payments/route`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ invoiceIds, customer, currency, saveMethod, requestedBy }),
  });
  return await handleResponse(response);
};

export const approvePaymentAuthorization = async ({ requestId, approvedBy }) => {
  const response = await fetch(`${API_BASE_URL}/agent/payments/authorizations/${requestId}/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ approvedBy }),
  });
  return await handleResponse(response);
};

export const rejectPaymentAuthorization = async ({ requestId, rejectedBy }) => {
  const response = await fetch(`${API_BASE_URL}/agent/payments/authorizations/${requestId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rejectedBy }),
  });
  return await handleResponse(response);
};

export const executePaymentAuthorization = async ({ requestId }) => {
  const response = await fetch(`${API_BASE_URL}/agent/payments/authorizations/${requestId}/execute`, {
    method: 'POST',
  });
  return await handleResponse(response);
};

// Create a Stripe PaymentIntent for selected invoices
export const createPaymentIntent = async ({ invoiceIds, currency, customer, saveMethod }) => {
  const response = await fetch(`/api/payments/create-intent`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ invoiceIds, currency, customer, saveMethod })
  });
  return await handleResponse(response);
};

export const confirmPayment = async ({ paymentIntentId }) => {
  const response = await fetch(`/api/payments/confirm`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paymentIntentId })
  });
  return await handleResponse(response);
};

export const cancelPayment = async ({ paymentIntentId }) => {
  const response = await fetch(`/api/payments/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paymentIntentId })
  });
  return await handleResponse(response);
};
export const getRunStatus = async () => {
  const response = await fetch("/api/run/status");
  return await handleResponse(response);
};


export const runNow = async (options = {}) => {
  const {
    wait = false,
    onUpdate,
    intervalMs = 1000,
    timeoutMs = 120000,
  } = options;

  let pre = null;
  try {
    pre = await getRunStatus();
  } catch (_) {}

  if (!pre || !pre.isRunning) {
    await fetch('/run-now', { method: 'POST' });
  }

  if (!wait) {
    const html = await (await fetch('/')).text();
    return extractLogsFromHtml(html);
  }

  const start = Date.now();
  let lastLogs = '';

  while (Date.now() - start < timeoutMs) {
    try {
      const status = await getRunStatus();

      if (typeof onUpdate === 'function') {
        onUpdate(status.lastRunResult || '');
      }

      if (!status.isRunning) {
        return status.lastRunResult || '';
      }

      lastLogs = status.lastRunResult || lastLogs;
    } catch (_) {
    }

    await new Promise(r => setTimeout(r, intervalMs));
  }

  return lastLogs;
};


// Upload invoice via /upload HTML endpoint; returns logs extracted from HTML
export const uploadInvoice = async (vendorId, file) => {
  const form = new FormData();
  form.append('vendor_id', vendorId);
  form.append('file', file);
  const uploadRes = await fetch('/upload', { method: 'POST', body: form });
  const html = await uploadRes.text();
  return extractLogsFromHtml(html);
};

// Utility functions (matching mock.js format)
export const getStatusLabel = (status) => {
  const labels = {
    'matched_auto': 'Matched',
    'unmatched': 'Unmatched',
    'vendor_mismatch': 'Mismatch',
    'needs_review': 'Needs Review',
    'ready_for_payment': 'Ready for Payment',
    'payment_pending': 'Payment Pending',
    'paid': 'Paid',
    'duplicate_blocked': 'Duplicate Blocked'
  };
  return labels[status] || status;
};

export const formatCurrency = (amount) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2
  }).format(amount || 0);
};

export const formatNumber = (num) => {
  return new Intl.NumberFormat('en-US').format(num || 0);
};
