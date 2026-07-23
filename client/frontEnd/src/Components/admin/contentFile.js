export const parseContentFile = async (file) => {
  if (!file) throw new Error("Choose a JSON content feed");
  if (file.size > 1_000_000) throw new Error("Content feed must be 1 MB or smaller");
  if (!file.name.toLowerCase().endsWith(".json")) throw new Error("Content feed must be a JSON file");
  let payload;
  try {
    payload = JSON.parse(await file.text());
  } catch {
    throw new Error("Content feed is not valid JSON");
  }
  if (payload?.schema_version !== "1.0" || payload?.source !== "open_icecat" || !Array.isArray(payload?.observations)) {
    throw new Error("Content feed does not match the ForgeSavant Open Icecat contract");
  }
  return payload;
};
