{
  "second_pass_investigation_config": {
    "version": "1.0",
    "investigation_type": "granular_operational_analysis",
    "description": "Multi-dimensional operational drill-through analysis building on first-pass strategic findings",
    
    "shared_table_metadata": {
      "claims_operational": {
        "table_name": "prd_optumrx_orxfdmprdsa.rag.claim_transaction_for_pharmacy_pbm",
        "description": "Claim-level operational data with therapeutic, demographic, geographic, and utilization details",
        "columns": [
          {
            "name": "product_category",
            "description": "Product category for each claim",
            "distinct_values": ["PBM", "Home Delivery", "Specialty"]
          },
          {
            "name": "line_of_business",
            "description": "Line of business identifier",
            "distinct_values": ["E&I FI", "E&I ASO", "C&S", "PDP", "MAPD", "E&I UMR", "External"]
          },
          {
            "name": "client_id",
            "description": "Unique client identifier"
          },
          {
            "name": "carrier_id",
            "description": "Insurance carrier identifier"
          },
          {
            "name": "member_id",
            "description": "Individual member identifier for utilization analysis"
          },
          {
            "name": "submit_date",
            "description": "Claim submission date (yyyy-MM-dd)"
          },
          {
            "name": "therapy_class_name",
            "description": "Therapeutic class of dispensed drug"
          },
          {
            "name": "drug_name",
            "description": "Name of dispensed drug"
          },
          {
            "name": "brand_vs_generic_ind",
            "description": "Brand or Generic indicator",
            "distinct_values": ["Brand", "Generic"]
          },
          {
            "name": "pharmacy_name",
            "description": "Dispensing pharmacy name"
          },
          {
            "name": "pharmacy_type",
            "description": "Internal vs external pharmacy type",
            "distinct_values": ["EXTERNAL-NON OPTUM OWNED", "INTERNAL OPTUM OWNED"]
          },
          {
            "name": "state_cd",
            "description": "Two-letter state code for geographic analysis"
          },
          {
            "name": "mbr_dt_of_brth",
            "description": "Member date of birth (yyyy-MM-dd) for age analysis"
          },
          {
            "name": "mbr_sex",
            "description": "Member gender",
            "distinct_values": ["M", "F"]
          },
          {
            "name": "revenue_amt",
            "description": "Revenue amount per claim"
          },
          {
            "name": "unadjusted_script_count",
            "description": "Raw script count per claim"
          },
          {
            "name": "revenue_per_script",
            "description": "Revenue per script derived metric"
          },
          {
            "name": "GDR_Ratio",
            "description": "Generic Dispense Rate ratio"
          }
        ]
      }
    },

    "investigation_dimensions": {
      "therapeutic": ["therapy_class_name", "drug_name", "brand_vs_generic_ind"],
      "brand_generic": ["brand_vs_generic_ind", "therapy_class_name"],
      "operational": ["pharmacy_type", "pharmacy_name", "state_cd"],
      "geographic": ["state_cd"],
      "demographic": ["mbr_dt_of_brth", "mbr_sex", "member_id"],
      "utilization": ["member_id", "unadjusted_script_count", "revenue_per_script"]
    },

    "analysis_templates": {
      "therapeutic": {
        "table_reference": "claims_operational",
        "dimensions": ["therapy_class_name", "drug_name", "brand_vs_generic_ind"],
        "metrics": ["revenue_amt", "unadjusted_script_count", "revenue_per_script"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 3,
        "sql_breakdown": ["therapy_class_variance", "drug_specific_analysis", "brand_generic_mix"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings (specific clients/LOBs identified). If first-pass found 'MDOVA client declining', filter all therapeutic analysis to client_id='MDOVA' only. INTENT-BASED ROW SELECTION: Extract variance direction from first-pass findings. If first-pass identifies declining entities, use ORDER BY variance ASC LIMIT 15-20 for worst-performing therapy classes. If first-pass identifies growing entities, use ORDER BY variance DESC LIMIT 15-20 for best-performing therapy classes. For mixed patterns, analyze both top 10 gainers and bottom 10 decliners. ROW VOLUME CONTROL: Therapy class analysis limited to 15-20 classes, drug analysis to 15 drugs, brand/generic analysis to 10-15 categories for manageable operational insights. MULTI-SQL APPROACH: Generate 3 focused SQLs: (1) Therapy class level variance analysis with period comparison, (2) Drug-specific deep dive within top therapy classes showing variance, (3) Brand vs generic analysis within therapy classes comparing periods. THERAPEUTIC DEEP-DIVE ANALYSIS: Generate comparative analysis (current period vs previous period) for therapy classes within first-pass identified entities. Identify which therapy classes and specific drugs are driving the variance patterns identified in first-pass. Focus on therapeutic category shifts, drug mix changes, and formulary impacts. BRAND vs GENERIC INTEGRATION: For each therapy class, analyze brand vs generic distribution changes between periods. Look for brand-to-generic switches that could explain revenue per script variance. Calculate therapy class level GDR changes over comparison periods. ENTITY STATUS ANALYSIS: Identify NEW therapy classes (zero prior period, current presence), DROP therapy classes (prior presence, zero current), EXISTING therapy classes with significant variance. SELECTION STRATEGY: Apply intent-based ordering with row limits. Focus on directionally appropriate therapy classes for investigation context. EXPECTED INSIGHTS: Therapeutic category explanations for first-pass variance. Drug formulary or clinical protocol changes. Brand/generic therapeutic substitution patterns."
      },

      "brand_generic": {
        "table_reference": "claims_operational", 
        "dimensions": ["brand_vs_generic_ind", "therapy_class_name"],
        "metrics": ["revenue_amt", "unadjusted_script_count", "GDR_Ratio", "revenue_per_script"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 2,
        "sql_breakdown": ["gdr_trend_analysis", "brand_switch_impact"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings. Apply same client/LOB filtering as identified in strategic analysis. INTENT-BASED ROW SELECTION: For declining revenue patterns, focus on therapy classes with increasing GDR (brand-to-generic switches causing revenue decline). For growing revenue patterns, focus on therapy classes maintaining brand utilization. Use ORDER BY based on investigation direction with LIMIT 10-15. ROW VOLUME CONTROL: Limit to 10-15 therapy classes for GDR analysis and top 15 brand-generic switches for impact analysis. DUAL-SQL APPROACH: Generate 2 focused SQLs: (1) GDR trend analysis by therapy class with period comparison, (2) Brand-to-generic switch revenue impact quantification comparing periods. GENERIC DISPENSE RATE FOCUS: Calculate GDR changes between comparison periods within the detected scope. Generate comparative analysis showing GDR shifts for therapy classes within first-pass identified entities. Identify therapy classes with significant GDR shifts (>5% change). BRAND TO GENERIC IMPACT ANALYSIS: Quantify revenue impact of brand-to-generic switches through period comparison. Calculate revenue per script differences between brand and generic within same therapy classes across periods. Identify high-impact brand switches contributing to overall variance. FORMULARY CHANGE DETECTION: Look for sudden GDR spikes indicating formulary or policy changes through period comparison analysis. Identify therapy classes where brand utilization dropped significantly. STRATEGIC INSIGHTS FOCUS: Which therapy classes show most significant brand-to-generic shifts between periods? Are generic switches volume-positive but revenue-negative? High-value brands being replaced by generics? SELECTION STRATEGY: Apply intent-based directional ordering with row limits focusing on therapy classes with significant GDR variance between periods."
      },

      "operational": {
        "table_reference": "claims_operational",
        "dimensions": ["pharmacy_type", "pharmacy_name", "state_cd"],
        "metrics": ["revenue_amt", "unadjusted_script_count", "revenue_per_script"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 2,
        "sql_breakdown": ["pharmacy_type_performance", "geographic_operational"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings. Focus operational analysis within identified client/LOB context only. INTENT-BASED ROW SELECTION: For declining entities, focus on worst-performing pharmacy types and states (ORDER BY variance ASC LIMIT 15-20). For growing entities, focus on best-performing operational areas (ORDER BY variance DESC LIMIT 15-20). For mixed patterns, analyze both top and bottom operational performers. ROW VOLUME CONTROL: Limit to 15 states for geographic analysis and 20 pharmacies for operational significance analysis. Balance internal vs external pharmacy representation. DUAL-SQL APPROACH: Generate 2 focused SQLs: (1) Pharmacy type performance analysis with period comparison (internal vs external), (2) Geographic operational patterns by state and pharmacy with period comparison. PHARMACY TYPE PERFORMANCE: Generate comparative analysis (current vs previous period) for pharmacy types within first-pass identified entities. Compare internal Optum-owned vs external pharmacy performance within scope. Identify if variance is concentrated in specific pharmacy types. Calculate revenue per script and script volume trends by pharmacy type across periods. GEOGRAPHIC CONCENTRATION: Identify states/regions driving variance patterns within the entity scope through period comparison. Look for geographic clustering of performance issues or improvements. OPERATIONAL EFFICIENCY INSIGHTS: Compare pharmacy type efficiency metrics (revenue per script, script processing volume) across periods. Identify operational bottlenecks or performance advantages. NETWORK OPTIMIZATION SIGNALS: Are internal pharmacies outperforming external in certain regions? Should pharmacy network mix be optimized? SELECTION STRATEGY: Apply intent-based directional ordering with row limits. Top 15 states by variance impact, top 20 pharmacies by operational significance based on investigation direction."
      },

      "geographic": {
        "table_reference": "claims_operational",
        "dimensions": ["state_cd"],
        "metrics": ["revenue_amt", "unadjusted_script_count"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 1,
        "sql_breakdown": ["state_variance_analysis"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings. Analyze geographic patterns within identified client/LOB context only. INTENT-BASED ROW SELECTION: For declining entities, focus on worst-performing states (ORDER BY variance ASC LIMIT 15). For growing entities, focus on best-performing states (ORDER BY variance DESC LIMIT 15). For mixed patterns, include both top 8 and bottom 8 states. ROW VOLUME CONTROL: Limit to 15 states maximum for focused geographic analysis with regional clustering capability. SINGLE-SQL APPROACH: Generate comprehensive state-level analysis with regional clustering in one query using CASE statements for regional groupings and period comparison. GEOGRAPHIC VARIANCE CONCENTRATION: Generate comparative analysis (current vs previous period) for states within first-pass identified entities. Identify which states contribute most to variance patterns identified in first-pass. Look for regional clustering of similar performance patterns. STATE-LEVEL MARKET DYNAMICS: Compare state-level performance trends within the entity scope across periods. Identify states with disproportionate variance contribution (>5% of total). REGIONAL PATTERN RECOGNITION: Are variance patterns concentrated in specific geographic regions (West Coast, Southeast, etc.)? Do certain states show opposite trends (some growing, others declining) through period comparison? MARKET SHARE IMPLICATIONS: Within entity scope, identify states where market position may be strengthening or weakening through period comparison. REGULATORY OR COMPETITIVE FACTORS: Flag states with unusual patterns that might indicate regulatory changes or competitive pressures through period analysis. SELECTION STRATEGY: Apply intent-based directional ordering with 15-state limit. Focus on states most relevant to investigation direction while including both high-performing and underperforming states for balanced analysis."
      },

      "demographic": {
        "table_reference": "claims_operational",
        "dimensions": ["mbr_dt_of_brth", "mbr_sex", "member_id"],
        "metrics": ["revenue_amt", "unadjusted_script_count"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 1,
        "sql_breakdown": ["demographic_utilization_analysis"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings. Analyze demographic patterns within identified client/LOB context only. INTENT-BASED ROW SELECTION: For declining entities, focus on demographic segments showing worst performance (ORDER BY variance ASC LIMIT 10-15). For growing entities, focus on best-performing demographic segments (ORDER BY variance DESC LIMIT 10-15). For mixed patterns, analyze both top and bottom demographic segments. ROW VOLUME CONTROL: Limit to 10-15 demographic segments (age cohorts and gender combinations) for manageable analysis while ensuring significant coverage. SINGLE-SQL APPROACH: Generate comprehensive demographic analysis combining age cohorts and gender patterns in one query with member-level aggregations and period comparison. AGE SEGMENTATION ANALYSIS: Generate comparative analysis (current vs previous period) for age cohorts within first-pass identified entities. Create age cohorts (18-34, 35-54, 55-64, 65+) and analyze variance by age group. Identify which age segments driving variance patterns from first-pass. MEMBER STRENGTH AND CONCENTRATION: Calculate member counts and average revenue per member by demographic segments across periods. Identify high-value member segments showing variance patterns. MEMBER UTILIZATION INTENSITY: Analyze scripts per member and revenue per member trends by demographic groups across periods. Identify if variance is driven by member behavior changes or population mix shifts. GENDER-BASED UTILIZATION PATTERNS: Compare male vs female utilization patterns within entity scope across periods. Identify gender-specific therapeutic or utilization trends contributing to variance. DEMOGRAPHIC RISK ASSESSMENT: Which demographic segments show concerning utilization or revenue trends based on investigation direction through period comparison? SELECTION STRATEGY: Apply intent-based directional ordering with demographic segment limits. Focus on age segments with significant variance impact based on investigation direction."
      },

      "utilization": {
        "table_reference": "claims_operational",
        "dimensions": ["member_id"],
        "metrics": ["revenue_amt", "unadjusted_script_count", "revenue_per_script"],
        "filters": {
          "claim_status_code": "['P','X']",
          "product_category": "USER_INTENT_DRIVEN"
        },
        "date_column": "submit_date",
        "sql_count": 2,
        "sql_breakdown": ["volume_price_decomposition", "member_utilization_patterns"],
        "llm_instructions": "CONTEXTUAL SCOPING: Inherit entity scope from first-pass findings. Analyze utilization patterns within identified client/LOB context only. INTENT-BASED ROW SELECTION: For declining revenue entities, focus on members with largest utilization drops or concerning patterns (ORDER BY variance ASC LIMIT 20). For growing entities, focus on members with highest utilization increases (ORDER BY variance DESC LIMIT 20). For mixed patterns, analyze both high-growth and declining utilization members. ROW VOLUME CONTROL: Limit to 20 high-impact members for utilization pattern analysis and focus on member segments representing significant utilization variance. DUAL-SQL APPROACH: Generate 2 focused SQLs: (1) Volume vs price variance decomposition with period comparison, (2) Member-level utilization pattern analysis with period comparison. VOLUME vs PRICE VARIANCE DECOMPOSITION: Generate comparative analysis (current vs previous period) separating variance into script volume changes vs revenue per script changes within first-pass identified entities. Identify whether variance is primarily volume-driven or price-driven within entity scope. MEMBER-LEVEL UTILIZATION INTENSITY: Calculate scripts per member and revenue per member trends across periods within first-pass identified entities. Identify high-utilization members driving variance patterns. UTILIZATION PATTERN SEGMENTATION: Segment members by utilization intensity (low, medium, high utilizers) and analyze variance contribution by utilization segment across periods. HIGH-VALUE MEMBER ANALYSIS: Identify members with annual pharmaceutical spend >$10K within first-pass entities. Analyze retention, churn, and utilization changes among high-value members based on investigation direction through period comparison. MEMBER UTILIZATION LIFECYCLE: Track member utilization intensity changes between periods within first-pass identified entities. Identify members moving between utilization tiers. UTILIZATION-BASED TARGETING: Which utilization segments require clinical intervention based on investigation direction through period analysis? Members showing concerning utilization spikes or drops? SELECTION STRATEGY: Apply intent-based directional ordering with member limits. Focus on members with significant annual spend variance or utilization change based on investigation direction."
      }
    },

    "global_llm_instructions": {
      "second_pass_orchestration": "You are conducting SECOND-PASS granular operational analysis building on first-pass strategic findings. The investigation scope is inherited from first-pass entity identification (specific clients/LOBs/carriers). Focus on operational and behavioral drivers that explain WHY the strategic variance occurred through comparative period analysis. Use multi-dimensional analysis to provide actionable operational insights.",
      
      "dimensional_intelligence": "Select analysis dimensions based on user intent and first-pass findings. If user requests specific dimension (therapeutic, geographic, etc.), focus on that dimension plus 1-2 complementary dimensions. If user requests comprehensive analysis, analyze all relevant dimensions but prioritize based on first-pass variance patterns.",
      
      "operational_focus": "Second-pass analysis should explain operational drivers behind first-pass strategic findings through comparative period analysis. Focus on controllable business levers: therapeutic mix optimization, pharmacy network performance, member utilization management, geographic market strategies, brand/generic formulary decisions.",
      
      "cross_dimensional_correlation": "Look for correlations across dimensions: Do certain therapy classes concentrate in specific geographies? Are demographic segments driving brand vs generic preferences? Do pharmacy types serve different utilization patterns? Identify multi-dimensional insights that provide comprehensive operational understanding.",
      
      "actionable_insight_prioritization": "Prioritize insights that suggest clear business actions through period comparison analysis. Therapeutic formulary adjustments, pharmacy network optimization, member clinical interventions, geographic market strategies, brand/generic policy changes. Focus on insights that operations teams can act upon.",
      
      "variance_continuation_logic": "Build narrative continuity with first-pass findings. Use phrases like 'Building on the finding that Client X declined...', 'Drilling deeper into the MAPD LOB variance...', 'Operational analysis reveals that...'. Connect operational insights back to strategic variance patterns through comparative analysis."
    }
  }
}
