export function nowRfc3339(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}
