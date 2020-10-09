export function setConfigValue (configValue: any): string {
  const envValue = process.env[configValue];
  return envValue || configValue;
};
