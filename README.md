# NSW Census Travel-to-Work Analytics (2011 / 2016 / 2021)

This repository contains a reproducible SQL view layer for analysing Australian Census **travel-to-work** patterns in **New South Wales (NSW)** across the 2011, 2016, and 2021 census years.

The core outputs support:
- Mode share trends (public transport, private vehicle, WFH, mixed modes)
- Origin–destination (OD) commuting flows between SA2s
- CBD dependency metrics (flows into the Sydney Inner City hub)
- A transparent 2021 bus-to-CBD candidate ranking

---

## Key Data Assumptions (important)

1. SA2 names are joinable via normalised text keys (`UPPER(TRIM(name))`).
2. Census `COUNT` fields represent comparable commuter counts within each year.
3. Non-informative travel categories are excluded from mode share calculations.
4. “Sydney Inner City” SA3 is a valid proxy for CBD destinations.
5. 2021 boundary change metadata provides context but not a full concordance.
6. Non-geographic OD destinations are excluded from analysis.
7. Thresholds applied to the 2021 bus-candidate shortlist reduce noise from small bases.

---

## Repository Contents

- SQL view layer (Oracle SQL) for dimensions, facts, QA, and metrics
- Downstream analysis-ready views for visualisation and scoring

---

## Design Principles

- Consistent SA2 geography across census years
- Explicit type alignment for UNION ALL compatibility
- Transparent QA checks at each stage
- Clear separation between raw, clean, and metric layers

---

## Author

Kevin Tran  
Master of Data Science, Monash University
