export const parseAffiliateLinkFile = (text) => {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error("The Amazon link file must be valid JSON");
  }
  const links = Array.isArray(parsed) ? parsed : parsed?.links;
  if (!Array.isArray(links) || !links.length) {
    throw new Error("JSON must contain a non-empty links array");
  }
  return links;
};
