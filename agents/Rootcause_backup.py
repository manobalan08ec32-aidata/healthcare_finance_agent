llm_context
"The following fields comprise the GL string used for financial accounting and reporting:
- **fqa_cmpny_cd**: 3-digit company code component of the GL string. Use for company-level financial segmentation.
- **fqa_geo_cd**: 3-digit geography code component of the GL string. Use for geographic financial segmentation and reporting.
- **fqa_aflt_cd**: 3-digit affiliate code component of the GL string. Use for affiliate-level financial tracking.
- **fqa_lob_cd**: 3-digit line of business code component of the GL string. Use for LOB financial segmentation and P&L reporting.
- **fqa_acnt_cd**: 3-digit account code component of the GL string. Use for account-level GL mapping.This account is not related to carrier , account,employer_group_id.
- **fqa_dept_cd**: 3-digit department code component of the GL string. Use for departmental cost allocation and reporting.
- **fqa_prod_cd**: 3-digit product code component of the GL string. Use for product-level financial tracking in GL.This is not oracle product code.
- **fqa_sub_acnt_cd**: 3-digit sub-account code component of the GL string. Use for detailed sub-account level GL analysis.This is not related to carrier account group.

"
"- **oracle_prod_code**:This is not drug related product code.This is billing services from the Oracle system. Examples include PROD35, PROD57. Use for product-level categorization and analysis.
- **orcl_prod_desc**: Oracle product description providing detailed product names. Examples include ""E-Prescribing Admin Fee"", ""PMPM-VCS"", ""Prior Auth. Specialty"". Use for product identification and reporting.
- **actvty_category_cd**: Activity category code for operational classification. Examples include 02EPRES, 01PCMS. Use for activity-based analysis and categorization.

"
"-**billed_amount**: Represents the total billed amount, also referred to as billing revenue.
                        -If the user asks for total billed amount, do not apply a GROUP BY on the rev_src_type column.
                        -If the user asks for claim billed amount, filter using rev_src_type = ""CLAIM COST""
                        -If the user refers to claim fee or activity fee, treat it as a fee-based request and apply the appropriate rev_src_type filter.
- **rev_src_type**: Categorizes the revenue source.. Distinct Values include ""CLAIM FEE"", ""CLAIM COST"", and ""ACTIVITY FEE"". when the user asks for specific fee, use this column else aggregate all amount and no group by needed.

"
"- **inv_nbr**: The invoice number is a unique identifier used to distinguish individual invoices within the billing system.
                When a user requests an invoice number without specifying an invoice date, do not prompt for or infer a time period. The system should return the invoice number directly without requesting additional temporal context. . 
- **invoice_date**:(YYYY-MM-DD) format. Date when the invoice was generated and issued to the client. 
                    Use for invoice timing and aging analysis.when the user mentions invoice date then use this column.
- **invoice_gl_date**:(YYYY-MM-DD) format. General ledger posting date for the invoice. Use for financial period reporting and GL reconciliation. May differ from invoice_date.

"
"- **drug_name**: Name of the dispensed drug; use for molecule-level trends and financials.
- **therapy_class_name**: Therapeutic class of the drug. Choose class or specific drug based on the question. sample therapy class names are Oncology,GLP-1 Receptor Agonists,SGLT-2 Inhibitors & Combos,GLP-1 Anti-Obesity Agents.if the user is asking generically GLP-1 then use like operator to get all the GLP-1 related information
- **brand_vs_generic_ind**: Product type indicator. Drives brand/generic mix and Generic Dispense Rate and also use it filter clause in case of brand vs generic calculation using distinct values [Values: Brand, Generic]
- **gpi_no**: GPI code.
-**DRUG_MANUFCTR_NM**:Drug manufacturer; supports brand/manufacturer market-share views

"
"- **line_of_business_name**: Contains line of business (LOB) identifier such as External,C&S,E&I FI,E&I ASO,PDP,MAPD,E&I UMR and when the user asks plainly external then use this column for filtering and is distinct from client type. [Distinct Values: E&I-FI,E&I-ASO,C&S,PDP,MAPD,E&I-UMR,External]
"
"- **blng_entty_cd**: Billing entity code identifier. Examples include ORXUHC0036XX, ORXUHC0005XX. Use to identify which entity is performing the billing.
- **blng_entty_name**: Billing entity name providing descriptive entity information. Examples include ""UHC INSURANCE COMPANY"", ""UHCACIS FULLY INSURED"". Use for entity-level reporting and analysis.

"
"- **claim_nbr**: Contains unique claim numbers used to identify individual claims in the dataset. should not be used for any aggregation.
- **claim_seq_nbr**: Sequence of transactions within the same claim_nbr (e.g., original, reversal). Not used for aggregations.
- **claim_status_code**: Processing status of the claim. For utilization/financial metrics use P (Paid)and X (Reveresed). Paid and reversed together reflect net activity.Always Include P,X for derivation. [Values: P, X]"
"- **carrier_id**: Insurance carrier identifier in the billing system. 
- **account_id**: Account identifier used for account-level grouping and analysis within the billing hierarchy.
- **group_id**: Group identifier for organizing and aggregating accounts into logical groupings.
-**CAG**: calculated column .concatenate carrier id , account id and group id to derive CAG

"
"- **client_id**: This contain unique 5-6 digit client code.Sample values [ABCD,WXYZ,57939]
- **client_description**: This contains client description. Return alongside client_id for user-facing reports .Sample values [""MDOVA OVATIONS MAPD/MA ONLY/RDS"",""PDIND PDP INDIVIDUAL""]

"
