/* eslint-disable react/prop-types */
import { useEffect, useMemo, useState } from "react";
import { FiCheck, FiSearch } from "react-icons/fi";
import { formatPrice, getItemUtilityFacts } from "./buildUtils";

const getSearchText = (item) =>
  [
    item.name,
    item.manufacturer,
    item.type,
    ...Object.values(item.specifications || {}),
  ]
    .join(" ")
    .toLowerCase();

const getFilterOptions = (items, stepId) => {
  const manufacturers = [
    ...new Set(items.map((item) => item.manufacturer).filter(Boolean)),
  ].sort();

  const specs = items
    .map((item) => {
      const itemSpecs = item.specifications || {};
      if (stepId === "processor") return itemSpecs.socket;
      if (stepId === "motherboard") return itemSpecs.form_factor;
      if (stepId === "gpu") return itemSpecs.memory;
      if (stepId === "primaryStorage" || stepId === "secondaryStorage") return itemSpecs.capacity;
      if (stepId === "ram") return itemSpecs.type;
      if (stepId === "smps") return itemSpecs.wattage;
      if (stepId === "cabinet") return itemSpecs.form_factor;
      return item.type;
    })
    .filter(Boolean);

  return {
    manufacturer: manufacturers,
    spec: [...new Set(specs)].sort(),
  };
};

const ComponentPicker = ({
  stepId,
  title,
  description,
  items,
  selectedId,
  loading,
  error,
  onRetry,
  onSelect,
  onSkip,
  canContinue,
  rowImage,
}) => {
  const [search, setSearch] = useState("");
  const [manufacturer, setManufacturer] = useState("");
  const [spec, setSpec] = useState("");
  const [sortBy, setSortBy] = useState("name");

  useEffect(() => {
    setSearch("");
    setManufacturer("");
    setSpec("");
    setSortBy("name");
  }, [stepId]);

  const filters = useMemo(() => getFilterOptions(items, stepId), [items, stepId]);

  const filteredItems = useMemo(() => {
    const query = search.trim().toLowerCase();
    const nextItems = items.filter((item) => {
      const facts = getItemUtilityFacts(item, stepId);
      const matchesSearch = query ? getSearchText(item).includes(query) : true;
      const matchesManufacturer = manufacturer
        ? item.manufacturer === manufacturer
        : true;
      const matchesSpec = spec ? facts.includes(spec) : true;
      return matchesSearch && matchesManufacturer && matchesSpec;
    });

    return [...nextItems].sort((a, b) => {
      if (sortBy === "priceAsc") return Number(a.price || 0) - Number(b.price || 0);
      if (sortBy === "priceDesc") return Number(b.price || 0) - Number(a.price || 0);
      if (sortBy === "wattage") {
        const aWatts = Number(String(a.specifications?.wattage || a.specifications?.tdp || "").match(/\d+/)?.[0] || 0);
        const bWatts = Number(String(b.specifications?.wattage || b.specifications?.tdp || "").match(/\d+/)?.[0] || 0);
        return aWatts - bWatts;
      }
      return a.name.localeCompare(b.name);
    });
  }, [items, manufacturer, search, sortBy, spec, stepId]);

  return (
    <section className="component-picker" aria-labelledby="component-picker-title">
      <div className="picker-header">
        <div>
          <p className="ui-kicker">Current step</p>
          <h2 id="component-picker-title">Choose {title.toLowerCase()}</h2>
        </div>
        <p>{description}</p>
      </div>

      {error ? (
        <div className="picker-state picker-error" role="alert">
          <strong>Catalog unavailable</strong>
          <span>{error}</span>
          <button type="button" onClick={onRetry}>Retry</button>
        </div>
      ) : null}

      <div className="picker-toolbar">
        <label className="picker-search">
          <span>Search</span>
          <span className="search-control">
            <FiSearch aria-hidden="true" />
            <input
              type="search"
              placeholder="Search components"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              disabled={loading || Boolean(error)}
            />
          </span>
        </label>
        <label>
          <span>Maker</span>
          <select
            value={manufacturer}
            onChange={(event) => setManufacturer(event.target.value)}
            disabled={loading || Boolean(error) || filters.manufacturer.length === 0}
          >
            <option value="">All</option>
            {filters.manufacturer.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Spec</span>
          <select
            value={spec}
            onChange={(event) => setSpec(event.target.value)}
            disabled={loading || Boolean(error) || filters.spec.length === 0}
          >
            <option value="">All</option>
            {filters.spec.map((option) => (
              <option key={option} value={option}>{option}</option>
            ))}
          </select>
        </label>
        <label>
          <span>Sort</span>
          <select
            value={sortBy}
            onChange={(event) => setSortBy(event.target.value)}
            disabled={loading || Boolean(error)}
          >
            <option value="name">Name</option>
            <option value="priceAsc">Price low</option>
            <option value="priceDesc">Price high</option>
            <option value="wattage">Wattage/TDP</option>
          </select>
        </label>
      </div>

      <div className="picker-list" role="listbox" aria-label={title} aria-busy={loading}>
        {loading ? (
          <>
            <span className="sr-only" role="status">Loading compatible {title.toLowerCase()} options</span>
            {Array.from({ length: 5 }).map((_, index) => (
              <div key={index} className="picker-skeleton" aria-hidden="true" />
            ))}
          </>
        ) : filteredItems.length > 0 ? (
          filteredItems.map((item) => {
            const facts = getItemUtilityFacts(item, stepId).slice(0, 4);
            const isSelected = selectedId === item._id;
            const pricingStatus = item.pricing?.status || "sample";
            const pricingLabel = pricingStatus === "live" ? "Live price" : pricingStatus === "stale" ? "Stale price" : "Planning price";
            return (
              <button
                key={item._id}
                type="button"
                className={`component-row ${isSelected ? "selected" : ""}`}
                onClick={() => onSelect(item)}
                role="option"
                aria-selected={isSelected}
              >
                <span className="row-select-indicator" aria-hidden="true">
                  {isSelected ? <FiCheck /> : null}
                </span>
                <span className="component-thumbnail">
                  <img
                    src={item.image_url || rowImage}
                    alt=""
                    onError={(event) => {
                      event.currentTarget.onerror = null;
                      event.currentTarget.src = rowImage;
                    }}
                  />
                </span>
                <span>
                  <strong>{item.name}</strong>
                  <small>{item.manufacturer || item.type || "Component"}</small>
                  <small className={`price-provenance ${pricingStatus}`} title={item.pricing?.observedAt ? `Observed ${new Date(item.pricing.observedAt).toLocaleString()}` : "Development catalog value"}>
                    {pricingLabel}{item.pricing?.source && pricingStatus !== "sample" ? ` · ${item.pricing.source.replaceAll("_", " ")}` : ""}
                  </small>
                </span>
                <span className="component-facts">
                  {facts.map((fact) => (
                    <em key={fact}>{fact}</em>
                  ))}
                </span>
                <span className="component-price">{formatPrice(item.price)}</span>
              </button>
            );
          })
        ) : (
          <div className="picker-state">
            <strong>No matching parts</strong>
            <span>Change the search or revisit an upstream compatibility choice.</span>
          </div>
        )}
      </div>

      <div className="picker-actions" aria-live="polite">
        <span>{filteredItems.length} option{filteredItems.length === 1 ? "" : "s"}</span>
        <div className="picker-action-group">
          {stepId === "secondaryStorage" ? <button className="picker-skip" type="button" onClick={onSkip}>Continue without a secondary drive</button> : null}
          <span className="picker-action-hint">
            {canContinue ? "Selection ready" : stepId === "secondaryStorage" ? "Choose a drive or skip this optional step" : "Choose an option to continue"}
          </span>
        </div>
      </div>
    </section>
  );
};

export default ComponentPicker;
