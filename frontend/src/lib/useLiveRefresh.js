import { useCallback, useEffect, useRef, useState } from 'react';
import { useLiveConnection } from './liveRefreshContext';

export const useLiveRefresh = (load, deps = []) => {
  const loadRef = useRef(load);
  const inFlightRef = useRef(false);
  const refreshQueuedRef = useRef(false);
  const lastSeenRevisionRef = useRef(null);
  const connection = useLiveConnection();
  const [liveState, setLiveState] = useState({
    connected: connection.connected,
    lastEventAt: connection.lastEventAt,
    lastSyncAt: null,
    revision: connection.revision,
  });

  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  const refresh = useCallback(async () => {
    if (document.visibilityState === 'hidden') return;
    if (inFlightRef.current) {
      refreshQueuedRef.current = true;
      return;
    }
    inFlightRef.current = true;
    try {
      await loadRef.current();
      const syncedAt = new Date().toISOString();
      setLiveState((current) => ({
        ...current,
        connected: connection.connected,
        lastEventAt: connection.lastEventAt,
        lastSyncAt: syncedAt,
        revision: connection.revision,
      }));
    } finally {
      inFlightRef.current = false;
      if (refreshQueuedRef.current) {
        refreshQueuedRef.current = false;
        void refresh();
      }
    }
  }, [connection.connected, connection.lastEventAt, connection.revision]);

  useEffect(() => {
    setLiveState((current) => ({
      ...current,
      connected: connection.connected,
      lastEventAt: connection.lastEventAt,
      revision: connection.revision,
    }));
  }, [connection.connected, connection.lastEventAt, connection.revision]);

  useEffect(() => {
    lastSeenRevisionRef.current = connection.revision;
    void refresh();

    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        void refresh();
      }
    };

    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    if (lastSeenRevisionRef.current === null) {
      lastSeenRevisionRef.current = connection.revision;
      return;
    }
    if (connection.revision !== lastSeenRevisionRef.current) {
      lastSeenRevisionRef.current = connection.revision;
      void refresh();
    }
  }, [connection.revision, refresh]);

  return liveState;
};
