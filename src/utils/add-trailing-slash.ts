export function addTrailingSlash(path?: string): string {
  return path != null ? (path.endsWith('/') ? path : `${path}/`) : '';
};
