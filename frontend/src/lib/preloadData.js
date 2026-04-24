import {
  fetchAgentOperationsMetrics,
  fetchAgentReviewQueue,
  fetchDashboardStats,
  fetchExceptionInvoices,
  fetchGraphData,
  fetchPayableInvoices,
  fetchPaymentHistory,
  fetchPendingPaymentConfirmations,
  fetchRecentInvoices,
  fetchVendorOptions,
  fetchVendorStats,
  fetchVendors,
} from '../services/api';
import { getPageCache, setPageCache } from './pageCache';

let initialWarmupScheduled = false;
let warmupTimer = null;
let warmupInFlight = false;
let warmupQueued = false;

const mergePageCache = (key, value) => {
  setPageCache(key, {
    ...(getPageCache(key) || {}),
    ...value,
    error: null,
  });
};

const runWithConcurrency = async (tasks, concurrency = 2) => {
  let index = 0;
  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, async () => {
    while (index < tasks.length) {
      const task = tasks[index];
      index += 1;
      try {
        await task();
      } catch (_) {
        // Warmup should never block normal navigation.
      }
    }
  });
  await Promise.all(workers);
};

const warmupTasks = ({ onlyExisting = false } = {}) => {
  const include = (key) => !onlyExisting || Boolean(getPageCache(key));

  return [
    include('dashboard') &&
      (async () => {
        const [stats, graphData, recentInvoices, vendors] = await Promise.all([
          fetchDashboardStats(),
          fetchGraphData(),
          fetchRecentInvoices(8),
          fetchVendorOptions(),
        ]);
        mergePageCache('dashboard', { stats, graphData, recentInvoices, vendors });
      }),

    include('vendors') &&
      (async () => {
        const [vendors, stats] = await Promise.all([
          fetchVendors(),
          fetchVendorStats(),
        ]);
        mergePageCache('vendors', { vendors, stats });
      }),

    include('exceptions') &&
      (async () => {
        const cache = getPageCache('exceptions') || {};
        const [vendors, items] = await Promise.all([
          fetchVendorOptions(),
          fetchExceptionInvoices({
            vendorId: cache.selectedVendorId || undefined,
            status: cache.statusFilter || undefined,
            limit: 200,
          }),
        ]);
        mergePageCache('exceptions', { vendors, items });
      }),

    include('payments') &&
      (async () => {
        const cache = getPageCache('payments') || {};
        const [vendors, items, pendingConfirmations, paymentHistory] = await Promise.all([
          fetchVendorOptions(),
          fetchPayableInvoices({
            vendorId: cache.selectedVendorId || undefined,
            currency: cache.currency || undefined,
            limit: 200,
          }),
          fetchPendingPaymentConfirmations({ limit: 20 }),
          fetchPaymentHistory({
            vendorId: cache.selectedVendorId || undefined,
            currency: cache.currency || undefined,
            limit: 20,
          }),
        ]);
        mergePageCache('payments', {
          vendors,
          items,
          pendingConfirmations,
          paymentHistory,
        });
      }),

    include('review-queue') &&
      (async () => {
        const cache = getPageCache('review-queue') || {};
        const items = await fetchAgentReviewQueue({
          activeOnly: true,
          status: cache.statusFilter || undefined,
          limit: 200,
        });
        mergePageCache('review-queue', {
          items: items || [],
        });
      }),

    include('agent-operations') &&
      (async () => {
        const cache = getPageCache('agent-operations') || {};
        const metrics = await fetchAgentOperationsMetrics({ days: cache.days || 30 });
        mergePageCache('agent-operations', { metrics });
      }),
  ].filter(Boolean);
};

export const warmPageData = async ({ onlyExisting = false } = {}) => {
  if (warmupInFlight) {
    warmupQueued = true;
    return;
  }

  warmupInFlight = true;
  try {
    await runWithConcurrency(warmupTasks({ onlyExisting }), 2);
  } finally {
    warmupInFlight = false;
    if (warmupQueued) {
      warmupQueued = false;
      void warmPageData({ onlyExisting: true });
    }
  }
};

export const scheduleWarmPageData = ({ delayMs = 800, onlyExisting = false } = {}) => {
  if (warmupTimer) {
    window.clearTimeout(warmupTimer);
  }
  warmupTimer = window.setTimeout(() => {
    warmupTimer = null;
    void warmPageData({ onlyExisting });
  }, delayMs);
};

export const scheduleInitialDataWarmup = () => {
  if (initialWarmupScheduled || typeof window === 'undefined') return;
  initialWarmupScheduled = true;

  const schedule = () => scheduleWarmPageData({ delayMs: 0, onlyExisting: false });
  if (typeof window.requestIdleCallback === 'function') {
    window.requestIdleCallback(schedule, { timeout: 1500 });
    return;
  }
  window.setTimeout(schedule, 500);
};
