export const getAuthError = (error, fallback) => {
  const response = error.response?.data;

  if (response?.error) return response.error;
  if (response?.message) return response.message;
  if (response?.errors?.length) return response.errors[0].msg;
  if (!error.response) return "The service is unavailable. Check the backend and try again.";
  return fallback;
};
