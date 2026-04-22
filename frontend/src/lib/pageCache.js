const pageCache = new Map();

export const getPageCache = (key, fallback = null) => {
  if (!pageCache.has(key)) return fallback;
  return pageCache.get(key);
};

export const setPageCache = (key, value) => {
  pageCache.set(key, value);
};
