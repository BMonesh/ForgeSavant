# ForgeSavant — Stitch AI UI Design Prompt

Copy everything below into Stitch AI and attach the two original black-and-yellow ForgeSavant reference screens.

---

Design a complete, distinctive, production-ready responsive web application UI for **ForgeSavant**, an intelligent custom-PC planning and component decision platform for the Indian market.

## What the application does

ForgeSavant helps beginners, gamers, creators, professionals, and PC enthusiasts plan a compatible custom computer without needing deep hardware knowledge. A user can describe a workload and budget, receive explainable build recommendations, browse a structured component catalog, compare parts, assemble a PC step by step, verify compatibility and estimated power requirements, save multiple builds, and inspect the evidence behind recommendations.

This is not a conventional electronics store. It is a trustworthy planning, compatibility, comparison, and data-intelligence product. Prices may come from authorized retailer observations, while external purchase destinations can include clearly disclosed affiliate links. Product specifications come from normalized, traceable sources. The interface must distinguish verified facts, estimates, sample data, stale data, and unavailable data instead of pretending all information is equally reliable.

The product also has a data-science layer. It measures catalog coverage, product-data quality, benchmark evidence, price history, recommendation readiness, and anonymized product usage. These analytics should feel integrated into the product rather than added as a generic dashboard.

## Primary experience goals

1. Make building a PC feel understandable, guided, and exciting.
2. Show *why* each recommendation is suitable.
3. Make compatibility constraints visible before the user makes a mistake.
4. Present price and data-quality limitations honestly.
5. Maintain one recognizable visual language across every public, authenticated, builder, detail, and admin page.
6. Work beautifully on desktop, tablet, and mobile.
7. Meet WCAG AA accessibility: sufficient contrast, keyboard navigation, visible focus states, semantic controls, and reduced-motion support.

## Creative direction

Use the attached initial design only as conceptual inspiration. Preserve its memorable industrial “computer forge/workshop” energy, black surfaces, warm safety yellow, mechanical PC imagery, construction metaphors, and bold confidence. Redesign it into a sophisticated modern product rather than copying the composition.

The desired feeling is:

- precision engineering workshop
- editorial technology publication
- premium professional tool
- transparent data laboratory
- energetic but not childish

Avoid:

- a generic SaaS dashboard
- excessive rounded cards
- neon cyberpunk clichés
- gaming RGB everywhere
- glassmorphism
- cramped ecommerce grids
- fake charts or fake retailer prices
- hazard tape used as constant decoration
- different design systems on different pages

Use a mostly neutral palette:

- near-black / graphite for major surfaces
- warm off-white for reading surfaces
- safety yellow as a controlled action and status accent
- muted steel gray for secondary information
- green only for confirmed compatibility or healthy data
- amber for warnings or partial evidence
- red only for actual conflicts or errors

Combine a bold, tightly spaced grotesk display face with a highly legible UI sans-serif and a restrained monospace face for part numbers, evidence, metrics, and technical labels. Use strong grid alignment, deliberate asymmetry, thin technical rules, large editorial headings, dense but readable data panels, and subtle blueprint/grid motifs.

Motion should be purposeful: component selection, compatibility recalculation, data refresh, builder progression, and section transitions. Do not add decorative motion that delays tasks.

## Global application shell

Create one consistent responsive header and navigation system used throughout the application.

Primary navigation:

- Recommended
- Components
- How it works
- Builder
- My builds

Account states:

- signed out: Sign in and Create account
- signed in: profile menu, My builds, Sign out
- administrator: access to Data quality, Content, Offers, Affiliate links, and Analytics

The active location must always be obvious. Builder mode may use a focused workspace header, but it must still visibly belong to the same ForgeSavant product through the same logo, type, color, spacing, and controls.

Create responsive desktop, tablet, and mobile navigation. On mobile, retain quick access to Builder and My builds.

## Required pages and states

### 1. Landing / home page

Create an original hero that clearly communicates: “Build the right PC. Know why it works.”

Include:

- primary CTA: Start a build
- secondary CTA: See how it works
- a striking PC/component composition integrated with the layout
- visible proof points such as catalog size, compatibility rules, or benchmark evidence
- a short explanation of guided building
- featured/reference build
- component-category explorer
- “how decisions are made” section
- trust/data provenance section
- FAQ
- affiliate disclosure link in the footer

Do not make the hero image look like a white rectangular image pasted onto the page. Images must have transparent backgrounds, matching surface colors, masks, gradients, or intentional framed treatment.

### 2. Recommended builds

Show a curated set of explainable reference configurations for use cases such as:

- value 1080p gaming
- balanced 1440p gaming
- high-refresh competitive gaming
- creator / video editing
- 3D rendering
- software development
- workstation / AI experimentation

Each configuration should show intended workload, estimated total, major parts, performance evidence, upgrade path, power estimate, data confidence, and a “Configure your version” action. Include filters for budget, workload, platform, and resolution. Create loading, no-results, partial-data, and stale-price states.

### 3. Component catalog

Design a structured browsing page for:

- processors
- graphics cards
- motherboards
- memory
- primary and secondary storage
- power supplies
- cases

Include search, filters, sorting, comparison selection, compatibility-aware filtering, verified-data badges, source freshness, price status, and responsive list/grid options. Prefer information-rich rows or cards over generic ecommerce tiles.

### 4. Component detail page

Include:

- product name, manufacturer, exact manufacturer part number, and category
- key specifications
- compatibility facts
- benchmark evidence
- observed price history where authorized data exists
- provenance and last-observed time
- data confidence/quality
- compatible related parts
- add to current build
- compare action
- external purchase destinations

Affiliate destinations must be visibly separate from price evidence and use this disclosure:

“As an Amazon Associate I earn from qualifying purchases.”

Do not imply an Amazon price is known when the application only has a purchase link. Include a concise note that Amazon pricing and availability are confirmed on Amazon.

### 5. Guided PC builder / workbench

This is the core product. Design a clear multi-step workspace:

1. Platform
2. CPU
3. Motherboard
4. GPU
5. Primary storage
6. Secondary storage
7. Memory
8. Power supply
9. Case
10. Review and save

Include:

- progress navigator
- current-step heading and explanation
- searchable/filterable compatible candidates
- selected-part preview
- persistent build summary
- running estimated total
- estimated power consumption and headroom
- compatibility evidence
- warnings and hard conflicts
- reason/explanation for every constraint
- back, replace, clear, and continue actions
- mobile-friendly step navigation

Show locked future steps, empty states, valid choices, recommended choices, selected choices, loading, source-data limitations, soft warnings, and hard incompatibilities. The builder must not visually become a different product.

### 6. Build review

Provide a final auditable configuration:

- selected components by category
- compatibility checklist
- estimated power and recommended headroom
- total based on available evidence
- missing/stale price warnings
- performance/benchmark summary
- upgrade-path notes
- data/source summary
- save, duplicate, rename, share, and edit actions
- clearly disclosed purchase destinations

### 7. My builds

Design signed-in build management:

- saved-build cards or rows
- build name, workload, last updated, estimated total, compatibility state
- reopen, duplicate, rename, delete, and compare
- empty state encouraging the first build
- delete confirmation
- mobile layout

### 8. How it works

Explain the product simply:

- start from workload and budget
- establish platform
- resolve compatibility constraints
- review specifications and benchmark evidence
- inspect pricing/provenance limitations
- save and revise the build

Use diagrams or visual sequences, not only large text. Explain the difference between recommendations, compatibility rules, estimates, verified product data, observed prices, and affiliate destinations.

### 9. Authentication

Create cohesive sign-in and create-account screens with:

- email/password
- validation and error states
- password visibility
- loading
- forgot-password affordance if appropriate
- reassurance about saved builds and privacy

Keep these screens visually connected to the main product and avoid generic centered white-card layouts.

### 10. Profile and privacy

Include:

- account information
- analytics consent
- explanation of anonymous product analytics
- export/delete-account controls
- clear destructive-action confirmation

### 11. Affiliate disclosure

Create a readable legal/information page explaining:

- qualifying Amazon links may earn ForgeSavant a commission
- this does not change the user’s purchase price
- affiliate links do not determine compatibility recommendations
- price and availability are checked on the retailer

### 12. Admin — data quality

Create a serious operational dashboard, not a decorative analytics template.

Include:

- total canonical products
- verified manufacturer-part-number coverage
- provenance/source URL coverage
- live versus sample pricing
- duplicate identity groups
- orphaned mappings
- source freshness
- Open Icecat available/restricted/unavailable coverage
- benchmark coverage
- pipeline run status
- quarantine/failure queue
- readiness status for analytics and ML use cases

Use honest labels such as Ready, Limited, and Blocked. Explain why a capability is blocked and what evidence is missing.

### 13. Admin — product content

Design a review queue for normalized product content:

- component identity
- proposed image/specification changes
- current versus proposed values
- source and provenance
- confidence
- approve/reject
- batch selection
- filters and audit history

### 14. Admin — retailer offer imports

Create an upload/review/apply workflow:

- upload a JSON feed
- preview validation
- exact/ambiguous/unmatched results
- price, availability, currency, source, and observed time
- checksum/import batch
- warnings
- apply confirmation
- history

### 15. Admin — Amazon affiliate destinations

This is a separate workflow from prices.

Include:

- upload JSON mapping of exact ForgeSavant component IDs or verified MPNs to Amazon India ASINs
- associate tag status
- exact match, ambiguous, unmatched, invalid, and duplicate results
- preview before apply
- explicit statement that this import does not add prices, availability, images, or specifications
- import history
- compliant link/disclosure preview

### 16. Admin — product analytics

Create data-science-oriented product analytics:

- builder starts and completions
- step abandonment
- component/category interest
- saved-build activity
- consent-aware event coverage
- catalog/benchmark coverage
- data-quality trends
- price observation coverage
- model-readiness scorecards

Only visualize metrics that the product could actually collect. Include empty/insufficient-data states and metric definitions.

## Reusable design system and components

Create and consistently use:

- logo/brand mark
- responsive navigation
- buttons and icon buttons
- form controls
- tabs
- filter chips
- data-confidence badges
- compatibility statuses
- source/provenance labels
- product rows/cards
- comparison tables
- step navigator
- build-summary panel
- dialogs and confirmations
- toast/inline feedback
- empty, loading, error, stale, restricted, and partial-data states
- accessible chart styles
- admin tables and import review panels

Provide a compact design-system page showing color tokens, typography, spacing, icons, controls, statuses, and key component variants.

## Logo request

Design a new standalone symbol for **ForgeSavant**. Do not rely on the written name inside the logo.

The mark should have a real concept: combine the ideas of **forging/assembly**, **intelligence/decision-making**, and **modular PC components**. Explore a geometric symbol formed from interlocking component blocks, a spark/decision node, and a subtle forged “F” or combined “F/S” negative space. Both letters do not need to be immediately obvious, but the construction must be explainable and intentional.

Requirements:

- distinctive at 16–24 px
- recognizable in one color
- balanced as a browser favicon and app icon
- works in white, black, and safety yellow
- no generic shield, brain, lightning bolt, wrench, or recycling-style mark
- no tiny text
- no gradient required for recognition
- original and not visually similar to major PC hardware brands

Present:

- three concept directions with a one-sentence meaning for each
- recommended final mark
- symbol-only version
- horizontal lockup
- light/dark versions
- favicon/app-icon version
- clear-space and minimum-size guidance

## Deliverables

Produce a coherent high-fidelity product design, not isolated concept screens.

Deliver:

- desktop screens for all required pages
- mobile screens for the home page, catalog, component detail, builder, build review, My builds, sign-in, and major admin workflows
- reusable component library/design system
- all important states
- clickable primary flows:
  - visitor → builder → completed build → sign in/save
  - catalog → component detail → add to build
  - signed-in user → reopen and edit a saved build
  - admin → preview and apply affiliate link import
- logo exploration and final brand assets

Use realistic PC-component content and Indian rupee formatting, but label illustrative/sample prices honestly. Favor clarity, trust, evidence, and memorable art direction over decorative complexity.
