/* eslint-disable react/prop-types */
import "./BrandLogo.css";
import brandMark from "../../assets/forgesavant-mark.webp";

const BrandLogo = ({ className = "" }) => (
  <img
    className={`brand-logo ${className}`.trim()}
    src={brandMark}
    alt=""
    aria-hidden="true"
  />
);

export default BrandLogo;
