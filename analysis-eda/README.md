# analysis-eda

Exploratory data analysis of Montgomery County Police Department (MCPD) use-of-force records (2022 – March 2026). Source data is the `uof.details` table in a Neon PostgreSQL database; each row represents one officer–subject interaction, with `reportguid` identifying the incident and `officerguid` identifying the officer.

---

## Research Questions

The analysis is organized around three goals.

### Goal 1 — Race & Use of Force

**Q1 — Force type by subject race**
Chi-square test of independence (race × force type) with Cramér's V effect size; stacked bar chart showing the percentage breakdown of force types used against Black, Hispanic, and White subjects.

**Q2 — Officers per incident by subject race**
Shapiro-Wilk normality check → Kruskal-Wallis H test; box plots comparing the distribution of officer counts per incident across racial groups.

**Q3 — Representation ratio**
Subject race shares in UoF incidents are compared against 2023 ACS county population figures. A representation ratio > 1 indicates over-representation relative to population share; bar charts show observed vs. expected proportions side by side.

### Goal 2 — Factors Affecting UoF Outcomes

**Q4 — Substance use & mental illness vs. force type**
Each incident is assigned a primary force category using a severity-ordered hierarchy (firearm discharge → lethal threat → CEW → chemical/pepper → physical). A multinomial logistic regression (L2, lbfgs) models the association between subject alcohol use, drug use, and possible mental illness flags and the resulting force category. Grouped bar charts show the conditional force distribution for each flag.

**Q5 — Force type variance by district**
Chi-square test on a district × force-category contingency table; a row-normalized heatmap (% within district) surfaces which categories are disproportionately used in each patrol district.

**Q6 — Subject demographic profile**
Population pyramid (male/female by age band: <18 through 65+); ranked frequency tables for race, age group, and race × gender combinations across deduplicated incidents.

### Goal 3 — Temporal Trends (2022 – March 2026)

**Q7 — Overall incident trend**
Monthly incident counts (deduplicated to one row per incident) with a 3-month centered rolling average overlaid as a line; covers the full 2022–2026 window.

**Q8 — Trend by district**
Two complementary views: a single multi-line chart with all districts overlaid, and a 3-column faceted panel with per-district bar + rolling-average charts so local patterns are easier to read.

---

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

Published write-ups: [UoF EDA #1 (RPubs)](https://rpubs.com/mdesir8/uof-eda-1)

---

## Directory Layout

```
analysis-eda/
├── images/          # exported chart PNGs (see table above)
├── notebooks/
│   ├── UOF_scratch_work_1.ipynb   # early exploration
│   ├── UOF_scratch_work_2.ipynb   # early exploration
│   └── analysis_executed.ipynb    # full executed analysis
└── scripts/
    └── analysis_executed.ipynb    # copy used for export/publishing
```
