import { useEffect, useRef, useState } from 'react';
import { subscribeToLiveUpdates } from '../services/api';

const STALE_AFTER_MS = 4000;

const toTimestampMs = (value) => {
  if (!value) return 0;
  const parsed = new Date(value);
  const timestamp = parsed.getTime();
  return Number.isNaN(timestamp) ? 0 : timestamp;
};

const isFresh = (value) => {
  const timestamp = toTimestampMs(value);
  if (!timestamp) return false;
  return Date.now() - timestamp <= STALE_AFTER_MS;
};

export const useLiveRefresh = (load, deps = []) => {
  const loadRef = useRef(load);
  const lastEventAtRef = useRef(null);
  const lastSyncAtRef = useRef(null);
  const [liveState, setLiveState] = useState({
    connected: false,
    lastEventAt: null,
    lastSyncAt: null,
    revision: 0,
  });

  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    let closed = false;
    let inFlight = false;
    let refreshQueued = false;

    const refresh = async () => {
      if (closed || document.visibilityState === 'hidden') return;
      if (inFlight) {
        refreshQueued = true;
        return;
      }
      inFlight = true;
      try {
        await loadRef.current();
        const syncedAt = new Date().toISOString();
        lastSyncAtRef.current = syncedAt;
        setLiveState((current) => ({
          ...current,
          connected: true,
          lastSyncAt: syncedAt,
        }));
      } finally {
        inFlight = false;
        if (refreshQueued) {
          refreshQueued = false;
          void refresh();
        }
      }
    };

    const subscription = subscribeToLiveUpdates({
      onOpen: (payload) => {
        if (closed) return;
        const eventAt = payload.serverTime || payload.occurredAt || new Date().toISOString();
        lastEventAtRef.current = eventAt;
        setLiveState((current) => ({
          connected: true,
          lastEventAt: eventAt,
          lastSyncAt: current.lastSyncAt,
          revision: payload.revision || current.revision || 0,
        }));
        void refresh();
      },
      onChange: (payload) => {
        if (closed) return;
        const eventAt = payload.serverTime || payload.occurredAt || new Date().toISOString();
        lastEventAtRef.current = eventAt;
        setLiveState((current) => ({
          connected: true,
          lastEventAt: eventAt,
          lastSyncAt: current.lastSyncAt,
          revision: payload.revision || current.revision || 0,
        }));
        void refresh();
      },
      onHeartbeat: (payload) => {
        if (closed) return;
        const eventAt = payload.serverTime || payload.occurredAt || new Date().toISOString();
        lastEventAtRef.current = eventAt;
        setLiveState((current) => ({
          connected: true,
          lastEventAt: eventAt,
          lastSyncAt: current.lastSyncAt,
          revision: payload.revision || current.revision,
        }));
        void refresh();
      },
      onError: () => {
        if (closed) return;
        setLiveState((current) => ({
          ...current,
          connected:
            isFresh(lastEventAtRef.current) || isFresh(lastSyncAtRef.current)
              ? current.connected
              : false,
        }));
      },
    });

    const stalenessInterval = window.setInterval(() => {
      if (closed) return;
      const shouldFallbackRefresh =
        document.visibilityState !== 'hidden' &&
        !isFresh(lastEventAtRef.current) &&
        !isFresh(lastSyncAtRef.current) &&
        !inFlight;
      if (shouldFallbackRefresh) {
        void refresh();
      }
      setLiveState((current) => {
        const healthy = isFresh(lastEventAtRef.current) || isFresh(lastSyncAtRef.current);
        if (current.connected === healthy) return current;
        return {
          ...current,
          connected: healthy,
        };
      });
    }, 1000);

    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        void refresh();
      }
    };

    document.addEventListener('visibilitychange', handleVisibility);
    void refresh();

    return () => {
      closed = true;
      subscription.close();
      window.clearInterval(stalenessInterval);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return liveState;
};
