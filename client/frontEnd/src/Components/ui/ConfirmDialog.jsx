/* eslint-disable react/prop-types */
import { useEffect, useRef } from "react";
import { FiAlertTriangle, FiX } from "react-icons/fi";

const ConfirmDialog = ({ open, title, description, busy, onCancel, onConfirm }) => {
  const cancelRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;
    cancelRef.current?.focus();

    const handleKeyDown = (event) => {
      if (event.key === "Escape" && !busy) onCancel();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [busy, onCancel, open]);

  if (!open) return null;

  return (
    <div className="dialog-backdrop" role="presentation" onMouseDown={busy ? undefined : onCancel}>
      <section className="confirm-dialog" role="alertdialog" aria-modal="true" aria-labelledby="confirm-title" aria-describedby="confirm-description" onMouseDown={(event) => event.stopPropagation()}>
        <div className="dialog-icon"><FiAlertTriangle aria-hidden="true" /></div>
        <button type="button" className="dialog-close" onClick={onCancel} disabled={busy} aria-label="Close confirmation"><FiX aria-hidden="true" /></button>
        <div>
          <p className="ui-kicker">Destructive action</p>
          <h2 id="confirm-title">{title}</h2>
          <p id="confirm-description">{description}</p>
        </div>
        <div className="dialog-actions">
          <button ref={cancelRef} type="button" className="builder-secondary" onClick={onCancel} disabled={busy}>Cancel</button>
          <button type="button" className="danger-button" onClick={onConfirm} disabled={busy}>{busy ? "Removing..." : "Remove build"}</button>
        </div>
      </section>
    </div>
  );
};

export default ConfirmDialog;
