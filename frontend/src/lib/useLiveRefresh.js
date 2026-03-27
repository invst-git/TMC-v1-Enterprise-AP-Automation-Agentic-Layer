import { useEffect, useRef, useState } from 'react';
import { subscribeToLiveUpdates } from '../services/api';

export const useLiveRefresh = (load, deps = []) => {
  const loadRef = useRef(load);
  const [liveState, setLiveState] = useState({
    connected: false,
    lastEventAt: null,
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
        setLiveState({
          connected: true,
          lastEventAt: payload.serverTime || payload.occurredAt || new Date().toISOString(),
          revision: payload.revision || 0,
        });
        void refresh();
      },
      onChange: (payload) => {
        if (closed) return;
        setLiveState({
          connected: true,
          lastEventAt: payload.serverTime || payload.occurredAt || new Date().toISOString(),
          revision: payload.revision || 0,
        });
        void refresh();
      },
      onHeartbeat: (payload) => {
        if (closed) return;
        setLiveState((current) => ({
          connected: true,
          lastEventAt: payload.serverTime || payload.occurredAt || new Date().toISOString(),
          revision: payload.revision || current.revision,
        }));
        void refresh();
      },
      onError: () => {
        if (closed) return;
        setLiveState((current) => ({
          ...current,
          connected: false,
        }));
      },
    });

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
      document.removeEventListener('visibilitychange', handleVisibility);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return liveState;
};
