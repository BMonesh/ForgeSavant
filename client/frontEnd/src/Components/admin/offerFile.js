const normalizeHeader = (value) => value.trim().toLowerCase().replace(/[\s-]+/g, "_");

export const parseCsv = (text) => {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];
    if (char === '"' && quoted && next === '"') {
      field += '"';
      index += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      row.push(field);
      field = "";
    } else if ((char === "\n" || char === "\r") && !quoted) {
      if (char === "\r" && next === "\n") index += 1;
      row.push(field);
      if (row.some((value) => value.trim())) rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }
  row.push(field);
  if (row.some((value) => value.trim())) rows.push(row);
  if (quoted) throw new Error("CSV contains an unclosed quoted field");
  if (rows.length < 2) throw new Error("CSV must include a header and at least one offer");

  const headers = rows[0].map(normalizeHeader);
  if (new Set(headers).size !== headers.length) throw new Error("CSV contains duplicate column names");
  return rows.slice(1).map((values, rowIndex) => {
    if (values.length !== headers.length) {
      throw new Error(`CSV row ${rowIndex + 2} has ${values.length} fields; expected ${headers.length}`);
    }
    return Object.fromEntries(headers.map((header, index) => [header, values[index]?.trim() || ""]));
  });
};

export const parseOfferText = (text, filename = "") => {
  const trimmed = text.replace(/^\uFEFF/, "").trim();
  if (!trimmed) throw new Error("The selected file is empty");
  if (filename.toLowerCase().endsWith(".json") || trimmed.startsWith("[") || trimmed.startsWith("{")) {
    const parsed = JSON.parse(trimmed);
    const offers = Array.isArray(parsed) ? parsed : parsed.offers;
    if (!Array.isArray(offers)) throw new Error("JSON must be an array or an object containing an offers array");
    return offers;
  }
  return parseCsv(trimmed);
};

export const parseOfferBundleText = (text, filename = "") => {
  const offers = parseOfferText(text, filename);
  const trimmed = text.replace(/^\uFEFF/, "").trim();
  if (!(filename.toLowerCase().endsWith(".json") || trimmed.startsWith("{"))) {
    return { offers, source: "" };
  }
  const parsed = JSON.parse(trimmed);
  const source = Array.isArray(parsed) ? "" : String(parsed.source || "").trim().toLowerCase();
  return { offers, source };
};

export const parseOfferFile = async (file) => parseOfferBundleText(await file.text(), file.name);
