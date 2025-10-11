prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast

"- **ledger**:Allowed values: GAAP, BUDGET, 8+4, 5+7, 2+10.If the question does not mention actuals, forecast, or budget, set ledger = GAAP.Any mention of actuals → GAAP.Any mention of budget → BUDGET.Any mention of forecast:If a cycle is specified (e.g., 8+4, 5+7, 2+10), use that value.If no cycle is specified, prompt the user to clarify which cycle (options: 8+4, 5+7, 2+10) [Values: 8+4, 2+10, 5+7, GAAP, BUDGET]

- **metric_type**:Allowed values [COGS Post Reclass,SG&A Post Reclass,IOI,Operating Earnings,Balance Sheet,Revenues,Corporate Costs,Total Workforce FTE,90 Day Scripts,Unadjusted Scripts,Interest Income,30 Day Scripts,Adjusted Scripts,ORx Capture Count,Other Capture Count,Research,Generic Scripts,Total Membership,Reported Revenues];Please refer the Mapping synonym Volume or total scripts ->Unadjusted Scripts , expense ->COGS Post Reclass and Revenue -> Revenues. When the user are asking for overall comparisons between actuals vs forecast vs budget or actuals alone, always include a GROUP BY clause on the metric_type column to ensure accurate results. If the user asks for a specific metric type, use it in the filter clause.

- **amount_or_count**: Contains either amount or count values for each metric type, and these must not be aggregated (e.g., summed) without applying appropriate filters or grouping by the metric_type column. The metric_type column includes distinct values such as Unadjusted Scripts, Adjusted Scripts, 30 Day Scripts, 90 Day Scripts, Revenues, COGS Post Reclass, SG&A Post Reclass, IOI, and Total Membership,etc. Even when attributes like product_category are present in the user question, any calculation involving actuals or forecast comparisons must include a GROUP BY metric_type clause to ensure accurate results

***The below metrics are calculated formulas  derived using metric_type column and strictly use the below formulas rather than asking follow up questions if user question contains any of the below metrics in the question.***
  - **Cost %**: calculated as COGS Post Reclass /  Revenues.
  - **Gross Margin**: calculated as Revenues - COGS Post Reclass.
  - **Gross Margin %**:calculated as  Gross Margin /  Revenues
  - **Operating Expenses %**: calculated as calculated as SG&A Post Reclass /  Revenues.
  - **Operating Cost %**: calculated as (COGS Post Reclass + SG&A Post Reclass) /  Revenues.
  - **IOI or Internal Operating Income %**: calculated as IOI / Revenues.
  - **Revenue per Script or rate**:calculated as  Revenues / Unadjusted Scripts . Use this by default for Volume or Revenue per script calculation.
  - **rate variance **: Column not exists.Derived formula- (Prior Month Average Rate - Current Month Average rate) x Current Month Volume, 
  -** volume variance = (prior Month volume - Current Month Volume) x Current Rate.Rate is reveneue_amt divided by unadjusted_script_count. Volume is unajusted_script_count. current month and previous month should be extracted from user question.
  - **mix variance **: Column not exists.Derived formula- (Prior month revenue - Current Month Revenue - Rate Variance- Volume Variance). current month and previous month should be extracted from user question .refer rate variance and volume variance formulas in other derived formulas
  - **Cost per Script (Unadj)**:  COGS Post Reclass / Unadjusted Scripts.
  - **Margin per Script (Unadj)**: (Revenues − COGS Post Reclass) / Unadjusted Scripts
  - **Op Exp per Script (Unadj)**: SG&A Post Reclass / Unadjusted Scripts.
  - **Op Cost per Script (Unadj)**: (COGS Post Reclass + SG&A Post Reclass) / Unadjusted Scripts.
  - **IOI per Script (Unadj)**: IOI / Unadjusted Scripts.
  - **Revenue per Script (Adj)**:  Revenues / Adjusted Scripts.
  - **Cost per Script (Adj)**:  COGS Post Reclass / Adjusted Scripts.
  - **Margin per Script (Adj)**:  calculated as (Revenues − COGS Post Reclass) / Adjusted Scripts.
  - **Op Exp per Script (Adj)**:  SG&A Post Reclass / Adjusted Scripts.
  - **Op Cost per Script (Adj)**: (COGS Post Reclass + SG&A Post Reclass) / Adjusted Scripts.
  - **IOI per Script (Adj)**: IOI / Adjusted Scripts.
  - **Utilization PMPM (Unadjusted)**:  Unadjusted Scripts / Total Membership.
  - **Utilization PMPM (Adjusted)**: Adjusted Scripts / Total Membership.
  - **SP Capture %**: Specialty pharmacy capture rate; ORx Capture Count / (ORx Capture Count + Other Capture Count).
  - **Generic Penetration %**: calculated as Generic Scripts / Unadjusted Scripts
**calculated formulas ends here list ends here **"
- **line_of_business**: Business or customer segment.C&S (Community & State), E&I (Employer & Individual), M&R(Medicare & Retirement), Optum, External. Commonly used for portfolio or market-share breakdowns. [Values: C&S, E&I, M&R, Rev Reclass, External, Optum]
"- **product_category**: High-level category of products or services.HDP->""Home Delivery"", Mail->""HDP"" and SP->Specialty [Values: PBM, Home Delivery, Other Products, Community Pharmacies, Workers Comp, Specialty, RVOH]
- **product_sub_category_lvl_1**: First-level subcategory under product_category. [Values: Home Delivery, Specialty, Core PBM, Other Products, Community Pharmacies, RVOH, Hospice, Workers Comp]
- **product_sub_category_lvl_2**: Second-level subcategory for more granularity. [Values: divvyDOSE, Retail Other, GPO, Optum Store, Infusion, Unknown, Workers Comp, Healthline/Healthgrades, Core HDP, CP Core, Mfr Discount, Hospice, RVOH Corp, Core Specialty, Prior Auth, Distribution, Frontier, PharmScript, Retail, Prevention, CPS Solutions, Nuvaila, Admin Fees, Optum Perks, Other Products] "
"- **transaction_date**: Exact transaction date (YYYY-MM-DD). Supports monthly, quarterly, and annual trend analysis for product category PBM/Home Delivery/Specialty.
- **year**: Four-digit year of the transaction.
- **month**: Numeric month of the transaction (1-12).
- **quarter**: Quarter of the transaction (Q1-Q4)."
"- **ora_client_id**: This contain unique 5-6 digit client code.Client ID and Client Name exists only for Actuals → GAAP and is NULL for Forecast or Budget; if a user requests client-level comparison involving Forecast or Budget, respond with: Client-level information is available only for Actuals.Sample values [MDOVA,PDIND,MDCSP,57939]
- **ora_client_description**: This contains client description. Return alongside client_id for user-facing reports .Sample values [""MDOVA OVATIONS MAPD/MA ONLY/RDS"",""PDIND PDP INDIVIDUAL""]"
