import React, { createContext, useContext, useEffect, useRef, useState } from 'react';
import { subscribeToLiveUpdates } from '../services/api';
import { scheduleWarmPageData } from './preloadData';

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

const LiveRefreshContext = createContext({
  connected: false,
  lastEventAt: null,
  revision: 0,
});

export const LiveRefreshProvider = ({ children }) => {
  const lastEventAtRef = useRef(null);
  const revisionRef = useRef(0);
  const [state, setState] = useState({
    connected: false,
    lastEventAt: null,
    revision: 0,
  });

  useEffect(() => {
    let closed = false;

    const markConnected = (payload = {}, { bumpRevision = false } = {}) => {
      if (closed) return;
      const eventAt = payload.serverTime || payload.occurredAt || new Date().toISOString();
      lastEventAtRef.current = eventAt;
      setState((current) => {
        const nextRevision = bumpRevision
          ? Number(payload.revision ?? revisionRef.current + 1)
          : current.revision;
        revisionRef.current = nextRevision;
        return {
          connected: true,
          lastEventAt: eventAt,
          revision: nextRevision,
        };
      });
    };

    const subscription = subscribeToLiveUpdates({
      onOpen: (payload) => markConnected(payload, { bumpRevision: true }),
      onChange: (payload) => {
        markConnected(payload, { bumpRevision: true });
        scheduleWarmPageData({ delayMs: 1200, onlyExisting: true });
      },
      onHeartbeat: (payload) => markConnected(payload, { bumpRevision: false }),
      onError: () => {
        if (closed) return;
        if (!isFresh(lastEventAtRef.current)) {
          setState((current) => ({
            ...current,
            connected: false,
          }));
        }
      },
    });

    const stalenessInterval = window.setInterval(() => {
      if (closed) return;
      const healthy = isFresh(lastEventAtRef.current);
      setState((current) => {
        if (current.connected === healthy) return current;
        return {
          ...current,
          connected: healthy,
        };
      });
    }, 1000);

    return () => {
      closed = true;
      subscription.close();
      window.clearInterval(stalenessInterval);
    };
  }, []);

  return (
    <LiveRefreshContext.Provider value={state}>
      {children}
    </LiveRefreshContext.Provider>
  );
};

export const useLiveConnection = () => useContext(LiveRefreshContext);
