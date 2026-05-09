# Analysis incl. EDA

Exploratory data analysis of Montgomery County Police Department (MCPD) use-of-force records (2022 – March 2026). Source data is the `uof.details` table in a Neon PostgreSQL database derived from Montgomery County OpenData records; each row represents one officer–subject interaction, with `reportguid` identifying the interaction and `cr_or_event` identifying the incident.

---
## Goal 1: Race & Use of Force

**Q1 — Force type distribution by race**
The question was whether officers use different types of force depending on the race of the subject. A chi-square test of independence was run on a contingency table of subject race by force type, with Cramér's V computed to measure effect size. The result was statistically significant (p < 0.05), but Cramér's V was small, indicating that while the distributions are not identical across racial groups, the practical difference in force type selection by race is weak.

**Q2 — Officers per incident by race**
The question was whether incidents involving subjects of different races tend to involve different numbers of officers. Shapiro-Wilk tests confirmed the officer count distributions were non-normal across all racial groups, so a Kruskal-Wallis H test was used instead of ANOVA to compare medians across Black, Hispanic, White, and Asian subjects. The test determined whether any racial group was systematically associated with larger or smaller officer deployments per incident.

**Q3 — Racial representation relative to county population**
The question was how much each racial group is over- or under-represented in UoF incidents relative to their share of the general population. Each group's observed share of incidents was divided by their ACS 2023 5-year population estimate to produce a representation ratio, where 1.0 indicates parity. Black subjects had a ratio of approximately 2.8, meaning they appear in UoF incidents at nearly three times the rate their population share would predict, while White and Asian subjects were both under-represented.

---

## Goal 2: Factors Affecting UoF Outcomes

**Q4 — Substance and mental health flags and force type**
The question was whether a subject being flagged for alcohol, drug involvement, or possible mental illness is associated with different force outcomes. Primary force category per incident was derived using a severity-ordered priority rule across the 22 force columns, then a multinomial logistic regression was fit with the three binary flags as predictors. The model's in-sample accuracy was low, suggesting these flags alone are weak predictors of force type, though the coefficients indicate some directional associations worth examining further.

**Q5 — Force category distribution by district**
The question was whether the mix of force categories used varies meaningfully across Montgomery County's six patrol districts. A chi-square test of independence was run on a district-by-force-category contingency table, with Cramér's V measuring practical effect size, and results were visualized as a row-normalized heatmap. The test was statistically significant but Cramér's V was small, consistent with the heatmap showing physical force dominating at 84-87% in every district with only minor variation in CEW and firearm pointing rates.

**Q6 — Subject demographic distribution**
The question was which demographic groups — defined by race, gender, and age — appear most frequently as subjects in officer force incidents. Incident-level records were deduplicated, age was binned into standard groups, and counts were tabulated by race, gender, and age group and visualized as a population pyramid. Black males aged 25-34 were the single most common demographic group at 13% of all incidents, and males overall accounted for 74.4% of subjects.

---

## Goal 3: Temporal Trends (2022–2026)

**Q7 — Countywide monthly trend**
The question was whether the overall volume of UoF incidents has changed over the 2022–2026 period. Incidents were deduplicated by report ID, aggregated to monthly counts, and plotted as a bar chart with a 3-month centered rolling average overlaid to reduce month-to-month noise. No clear directional trend emerged — the series is broadly flat with seasonal fluctuation and no sustained increase or decrease.

**Q8 — District-level monthly trends**
The question was whether individual districts show different temporal patterns from the countywide trend. The same monthly aggregation was computed per district and visualized both as overlapping multi-line time series and as faceted small multiples, each panel showing the raw monthly bars with a 3-month rolling average. District-level trends are similarly flat on the whole, though the faceted view makes it easier to spot district-specific spikes that are obscured when all lines are plotted together.

## Outputs

| File | Question |
|------|----------|
| `images/q1_force_type_by_race.png` | Q1 stacked bar |
| `images/q2_officer_count_by_race.png` | Q2 box plots |
| `images/q3_representation_ratio.png` | Q3 side-by-side bars + ratio chart |
| `images/q4_force_by_substance_mental.png` | Q4 grouped bars (alcohol / drugs / mental illness) |
| `images/q5_force_by_district_heatmap.png` | Q5 row-normalized heatmap |
| `images/q6_population_pyramid.png` | Q6 population pyramid |
| `images/q7_overall_trend.png` | Q7 monthly trend + rolling average |
| `images/q8_trend_by_district_multiline.png` | Q8 multi-line overlay |
| `images/q8_trend_by_district_faceted.png` | Q8 per-district faceted panel |
