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
  const supportedSource = ["forgesavant_product_content", "open_icecat"].includes(payload?.source);
  if (payload?.schema_version !== "1.0" || !supportedSource || !Array.isArray(payload?.observations)) {
    throw new Error("Content feed does not match the ForgeSavant product-content contract");
  }
  return payload;
};
