const BUILD_DRAFT_KEY = "forgesavant:build-draft";

export const loadBuildDraft = () => {
  const stored = localStorage.getItem(BUILD_DRAFT_KEY);
  if (!stored) return null;

  try {
    return JSON.parse(stored);
  } catch {
    localStorage.removeItem(BUILD_DRAFT_KEY);
    return null;
  }
};

export const saveBuildDraft = (draft) => {
  localStorage.setItem(BUILD_DRAFT_KEY, JSON.stringify(draft));
};

export const clearBuildDraft = () => {
  localStorage.removeItem(BUILD_DRAFT_KEY);
};
