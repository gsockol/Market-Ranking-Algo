HVLP Global Gym Market Opportunity Model

Complete Technical & Engineering Specification (Expanded)

1. System Purpose (Formal Definition)

The system is a deterministic quantitative scoring engine that transforms heterogeneous macroeconomic and industry indicators into a comparable cross-country attractiveness index.

Formally:



A multi-stage transformation pipeline mapping country-level economic vectors → normalized feature space → weighted scalar ranking.

Mathematically:



f: R^n → [0,100]



Where:



input vector = country feature vector

output = composite attractiveness score

Constraints:



deterministic

reproducible

interpretable

overrideable without code edits

2. Design Principles (Engineering Constraints)

The system was explicitly designed around the following failures common in strategy models:

ProblemEngineering SolutionScale distortionlog transformsEmerging market suppressionnonlinear headroom transformOutlier dominancepercentile clippingAnalyst biasfixed weighting configNon-reproducibilityversion taggingHidden logicexplicit transformation pipeline

3. Repository Architecture (GitHub Structure)

Repository: Market-Ranking-Algo

Expected layout:



Market-Ranking-Algo/

│

├── main.py

├── config.py

├── requirements.txt

│

├── src/

│ ├── calculator.py

│ ├── normalizer.py

│ ├── data_loader.py (implicit or embedded)

│

├── data/

│ └── preweighted_input.csv

│

├── output/

│ ├── baseline_ranking.csv

│ └── baseline_scores_full.csv

│

└── notebooks/ (optional)



4. Data Model (Schema Definition)

Each row = one country.



Primary Key

country_name



Required Column Schema

Market Size

ColumnTypeUnitopportunity_usd_mfloatUSD millionspotential_market_sizefloatUSD millions

Penetration Variables

ColumnDescriptioncurrent_penetration% population gym membersfuture_penetrationassumed maturity levelconcentrationpopulation per gym

Derived:



penetration_headroom =

future_penetration − current_penetration



Growth Variables

Columngym_membership_cagr

Proxy logic:

If missing → GDP growth proxy.

Cost Structure

ColumnMeaninglabor_cost_indexnormalized wage proxyreal_estate_cost_indexcommercial rent proxycorporate_tax_ratestatutory

Demand Indicators

Columnyouth_population_pctmiddle_class_pctfitness_spend_proxy

Risk Indicators

Columnease_of_doing_businesspolitical_stabilityinflationcurrency_volatilityfinancing_access

5. Data Sources (Economic Provenance)

Variables originate from standardized macro datasets.



Primary Sources

Variable TypeSourceGDP, populationWorld Bank APIDemographicsUN Population DivisionInflationIMFGovernanceWorld Governance IndicatorsCost indicesNumbeo / OECD proxiesFitness penetrationIndustry reports + modeled estimates

Why APIs Are Used

APIs allow:



reproducibility

automated refresh

auditability

consistent baseline anchoring

The system intentionally supports:



--skip-api



to allow offline deterministic runs.

6. Data Loading Logic

Pipeline:



Load CSV

↓

Validate schema

↓

Apply overrides

↓

Compute derived metrics



Schema validation MUST confirm required columns exist before computation.

Failure mode observed:



KeyError: labor_cost_index



Meaning:



CSV schema mismatch

API merge failure

wrong dataset loaded

7. Derived Variable Engineering

7.1 Penetration Headroom

Economic logic:

Gym adoption follows S-curve diffusion.

Linear scoring undervalues early-stage markets.

Transform:



headroom_score = sqrt(headroom)



Effect:



boosts mid-growth countries

reduces extreme projections

7.2 Concentration Normalization

Problem:



people per gym



spans orders of magnitude.

Solution:



log(concentration)



then invert.

Interpretation:

Lower density → expansion capacity.

7.3 TAM Compression

Large economies dominate without correction.

Applied:



log(opportunity_usd_m)

log(potential_market_size)



This converts multiplicative scale into additive scale.

7.4 Operating Cost Composite

Built because raw cost variables are collinear.

Construction:



labor_score = invert(normalize(labor))

realestate_score = invert(normalize(real_estate))



operating_cost_composite =

0.6 * labor_score +

0.4 * realestate_score



Weights reflect gym cost structure:



labor dominant expense

rent secondary

7.5 Market Agility Bonus

Observation:

Smaller markets scale faster operationally.

Defined:



agility = 1 / sqrt(potential_market_size)



This introduces a controlled anti-size bias.

8. Normalization Engine (normalizer.py)

This is the core mathematical layer.

Pipeline is sequential and stateful.

Step 1 — Percentile Clipping

x ← clip(x, p05, p95)



Reduces influence of tail outliers.

Step 2 — Winsorization

Applied selectively to TAM variables.

Prevents US/India scale anchoring.

Step 3 — Pre-Transforms

Configured via:



PRE_TRANSFORMS = {

"log": [...],

"sqrt": [...]

}



Executed dynamically.

Step 4 — Z-score

z = (x − μ)/σ



Ensures comparability.

Step 5 — Percentile Mapping

percentile = CDF(z)

score = percentile * 100



Creates intuitive scoring range.

Step 6 — Inversion

For negative desirability variables:



score = 100 − score



Defined in:



INVERTED_VARIABLES



9. Weighting Engine (config.py)

Centralized configuration.

Example structure:





WEIGHTS = {

"opportunity": 0.23,

"headroom": 0.24,

...

}



Constraint enforced:



sum(weights) == 1.0



10. Composite Score Calculation

Inside calculator.py.

Vectorized implementation:



score_country =

Σ(weight_i × normalized_feature_i)



No loops preferred; pandas vectorization assumed.

11. Ranking Algorithm

df.sort_values("composite_score", ascending=False)



Ranks assigned sequentially.

12. Tier Assignment Logic

Quantile-based or threshold-based mapping:

Example:



>=75 → Tier 1

60–75 → Tier 2

45–60 → Tier 3

<45 → Tier 4



13. Override System Engineering

Overrides occur BEFORE normalization.

Order:



load data

→ apply CAGR overrides

→ apply penetration overrides

→ recompute headroom

→ normalize



Critical:

Overrides must invalidate cached derived fields.

14. Reproducibility Controls

Each run exports:



baseline_ranking.csv

baseline_scores_full.csv



Includes:



raw variables

normalized values

final weights

Allows forensic debugging.

15. Execution Lifecycle

main.py

│

├─ load data

├─ validate schema

├─ ask overrides

├─ calculate composites

├─ normalize

├─ weight aggregation

├─ rank

└─ export outputs



16. Failure Modes (Observed)

ErrorRoot CauseMissing columnCSV schema mismatchBrazil rank mismatchstale repo versionAPI timeoutWorld Bank endpoint instabilityKeyErrorwrong dataframe stageinfinite runblocking API call

17. Why Brazil Moves in Rankings (Model Behavior)

Brazil benefits from:



large TAM (log-adjusted)

strong headroom (sqrt boosted)

moderate costs

improving demand indicators

Portugal improves via:



favorable cost composite

governance stability

smaller agility penalty

18. System Classification

Technically this model is closest to:



Composite Indicator Framework (OECD methodology)

Multi-Criteria Decision Analysis (MCDA)

Weighted Index Construction

NOT:



machine learning

regression

predictive AI

It is a structured analytical index.

19. GUI Objective (Future Layer)

GUI is strictly orchestration — NOT computation.

Responsibilities:



collect overrides

trigger pipeline

render outputs

Computation remains backend Python.

20. Final System Definition

You have constructed:



A reproducible international expansion scoring engine combining macroeconomic ETL, statistical normalization, domain-informed feature engineering, and deterministic weighted aggregation.

It operationalizes strategic expansion analysis into code.



HVLP Global Gym Market Opportunity Model

Complete Technical & Engineering Specification (Expanded)

1. System Purpose (Formal Definition)

The system is a deterministic quantitative scoring engine that transforms heterogeneous macroeconomic and industry indicators into a comparable cross-country attractiveness index.

Formally:



A multi-stage transformation pipeline mapping country-level economic vectors → normalized feature space → weighted scalar ranking.

Mathematically:



f: R^n → [0,100]



Where:



input vector = country feature vector

output = composite attractiveness score

Constraints:



deterministic

reproducible

interpretable

overrideable without code edits

2. Design Principles (Engineering Constraints)

The system was explicitly designed around the following failures common in strategy models:

ProblemEngineering SolutionScale distortionlog transformsEmerging market suppressionnonlinear headroom transformOutlier dominancepercentile clippingAnalyst biasfixed weighting configNon-reproducibilityversion taggingHidden logicexplicit transformation pipeline

3. Repository Architecture (GitHub Structure)

Repository: Market-Ranking-Algo

Expected layout:



Market-Ranking-Algo/

│

├── main.py

├── config.py

├── requirements.txt

│

├── src/

│ ├── calculator.py

│ ├── normalizer.py

│ ├── data_loader.py (implicit or embedded)

│

├── data/

│ └── preweighted_input.csv

│

├── output/

│ ├── baseline_ranking.csv

│ └── baseline_scores_full.csv

│

└── notebooks/ (optional)



4. Data Model (Schema Definition)

Each row = one country.



Primary Key

country_name



Required Column Schema

Market Size

ColumnTypeUnitopportunity_usd_mfloatUSD millionspotential_market_sizefloatUSD millions

Penetration Variables

ColumnDescriptioncurrent_penetration% population gym membersfuture_penetrationassumed maturity levelconcentrationpopulation per gym

Derived:



penetration_headroom =

future_penetration − current_penetration



Growth Variables

Columngym_membership_cagr

Proxy logic:

If missing → GDP growth proxy.

Cost Structure

ColumnMeaninglabor_cost_indexnormalized wage proxyreal_estate_cost_indexcommercial rent proxycorporate_tax_ratestatutory

Demand Indicators

Columnyouth_population_pctmiddle_class_pctfitness_spend_proxy

Risk Indicators

Columnease_of_doing_businesspolitical_stabilityinflationcurrency_volatilityfinancing_access

5. Data Sources (Economic Provenance)

Variables originate from standardized macro datasets.



Primary Sources

Variable TypeSourceGDP, populationWorld Bank APIDemographicsUN Population DivisionInflationIMFGovernanceWorld Governance IndicatorsCost indicesNumbeo / OECD proxiesFitness penetrationIndustry reports + modeled estimates

Why APIs Are Used

APIs allow:



reproducibility

automated refresh

auditability

consistent baseline anchoring

The system intentionally supports:



--skip-api



to allow offline deterministic runs.

6. Data Loading Logic

Pipeline:



Load CSV

↓

Validate schema

↓

Apply overrides

↓

Compute derived metrics



Schema validation MUST confirm required columns exist before computation.

Failure mode observed:



KeyError: labor_cost_index



Meaning:



CSV schema mismatch

API merge failure

wrong dataset loaded

7. Derived Variable Engineering

7.1 Penetration Headroom

Economic logic:

Gym adoption follows S-curve diffusion.

Linear scoring undervalues early-stage markets.

Transform:



headroom_score = sqrt(headroom)



Effect:



boosts mid-growth countries

reduces extreme projections

7.2 Concentration Normalization

Problem:



people per gym



spans orders of magnitude.

Solution:



log(concentration)



then invert.

Interpretation:

Lower density → expansion capacity.

7.3 TAM Compression

Large economies dominate without correction.

Applied:



log(opportunity_usd_m)

log(potential_market_size)



This converts multiplicative scale into additive scale.

7.4 Operating Cost Composite

Built because raw cost variables are collinear.

Construction:



labor_score = invert(normalize(labor))

realestate_score = invert(normalize(real_estate))



operating_cost_composite =

0.6 * labor_score +

0.4 * realestate_score



Weights reflect gym cost structure:



labor dominant expense

rent secondary

7.5 Market Agility Bonus

Observation:

Smaller markets scale faster operationally.

Defined:



agility = 1 / sqrt(potential_market_size)



This introduces a controlled anti-size bias.

8. Normalization Engine (normalizer.py)

This is the core mathematical layer.

Pipeline is sequential and stateful.

Step 1 — Percentile Clipping

x ← clip(x, p05, p95)



Reduces influence of tail outliers.

Step 2 — Winsorization

Applied selectively to TAM variables.

Prevents US/India scale anchoring.

Step 3 — Pre-Transforms

Configured via:



PRE_TRANSFORMS = {

"log": [...],

"sqrt": [...]

}



Executed dynamically.

Step 4 — Z-score

z = (x − μ)/σ



Ensures comparability.

Step 5 — Percentile Mapping

percentile = CDF(z)

score = percentile * 100



Creates intuitive scoring range.

Step 6 — Inversion

For negative desirability variables:



score = 100 − score



Defined in:



INVERTED_VARIABLES



9. Weighting Engine (config.py)

Centralized configuration.

Example structure:





WEIGHTS = {

"opportunity": 0.23,

"headroom": 0.24,

...

}



Constraint enforced:



sum(weights) == 1.0



10. Composite Score Calculation

Inside calculator.py.

Vectorized implementation:



score_country =

Σ(weight_i × normalized_feature_i)



No loops preferred; pandas vectorization assumed.

11. Ranking Algorithm

df.sort_values("composite_score", ascending=False)



Ranks assigned sequentially.

12. Tier Assignment Logic

Quantile-based or threshold-based mapping:

Example:



>=75 → Tier 1

60–75 → Tier 2

45–60 → Tier 3

<45 → Tier 4



13. Override System Engineering

Overrides occur BEFORE normalization.

Order:



load data

→ apply CAGR overrides

→ apply penetration overrides

→ recompute headroom

→ normalize



Critical:

Overrides must invalidate cached derived fields.

14. Reproducibility Controls

Each run exports:



baseline_ranking.csv

baseline_scores_full.csv



Includes:



raw variables

normalized values

final weights

Allows forensic debugging.

15. Execution Lifecycle

main.py

│

├─ load data

├─ validate schema

├─ ask overrides

├─ calculate composites

├─ normalize

├─ weight aggregation

├─ rank

└─ export outputs



16. Failure Modes (Observed)

ErrorRoot CauseMissing columnCSV schema mismatchBrazil rank mismatchstale repo versionAPI timeoutWorld Bank endpoint instabilityKeyErrorwrong dataframe stageinfinite runblocking API call

17. Why Brazil Moves in Rankings (Model Behavior)

Brazil benefits from:



large TAM (log-adjusted)

strong headroom (sqrt boosted)

moderate costs

improving demand indicators

Portugal improves via:



favorable cost composite

governance stability

smaller agility penalty

18. System Classification

Technically this model is closest to:



Composite Indicator Framework (OECD methodology)

Multi-Criteria Decision Analysis (MCDA)

Weighted Index Construction

NOT:



machine learning

regression

predictive AI

It is a structured analytical index.

19. GUI Objective (Future Layer)

GUI is strictly orchestration — NOT computation.

Responsibilities:



collect overrides

trigger pipeline

render outputs

Computation remains backend Python.

20. Final System Definition

You have constructed:



A reproducible international expansion scoring engine combining macroeconomic ETL, statistical normalization, domain-informed feature engineering, and deterministic weighted aggregation.

It operationalizes strategic expansion analysis into code.
