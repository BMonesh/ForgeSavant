/* eslint-disable react/prop-types */
import "./BrandLogo.css";

const BrandLogo = ({ className = "", title = "ForgeSavant", decorative = false }) => (
  <svg
    className={`brand-logo ${className}`.trim()}
    viewBox="0 0 64 64"
    role={decorative ? undefined : "img"}
    aria-label={decorative ? undefined : title}
    aria-hidden={decorative || undefined}
    focusable="false"
  >
    {decorative ? null : <title>{title}</title>}
    <path
      d="M7 6h50v23H47V16H22l-5 5v10h20v10H17v17H7V6Z"
      fill="currentColor"
    />
    <path
      d="M27 22h25v10H39l13 13v13H25V48h17l-8-8h-7V22Zm10 10 5 5v-5h-5Z"
      fill="currentColor"
    />
    <circle cx="12" cy="11" r="2" fill="currentColor" />
    <circle cx="52" cy="11" r="2" fill="currentColor" />
    <circle cx="12" cy="53" r="2" fill="currentColor" />
    <circle cx="52" cy="53" r="2" fill="currentColor" />
  </svg>
);

export default BrandLogo;
