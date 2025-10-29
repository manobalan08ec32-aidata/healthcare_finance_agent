**CHECK 5: Temporal validation**
Current date: October 29, 2025
If question mentions specific dates/periods:
- Past dates (before Oct 2025) → ✓ Valid
- Current/recent dates (2025 year-to-date) → ✓ Valid  
- Near-future dates (within 12 months) → ✓ Valid (forecast context)
- Far-future dates (beyond Nov 2026) → ❌ Clarify intent

Output: ✓ Valid timeframe | ❌ Invalid date
```

Then add "Temporal: [status]" to the PRE-VALIDATION output format.

## **Fix 2: Amount & Percentage Formatting (replace rule 12 in TASK 2)**

Replace the existing rule 12 with this concise version:
```
12. COLUMN NAMING & PRECISION RULES
- Amount columns: suffix with _amount, ROUND(value, 0) - zero decimals
- Percentage columns: suffix with _percent, ROUND(value, 3) - three decimals
- Examples:
  * SUM(amount) → ROUND(SUM(amount), 0) AS total_revenue_amount
  * (cost/revenue)*100 → ROUND((cost/revenue)*100, 3) AS cost_ratio_percent
- Apply to all numeric outputs; non-numeric columns keep original names
```

## **Alternative Compact Version (saves more tokens):**

If you want even shorter, combine into rule 12:
```
12. FORMATTING & NAMING
**Numeric columns:**
- Amounts/Counts/Totals: ROUND(value, 0) AS name_amount
- Percentages/Ratios (÷ operations × 100): ROUND(value, 3) AS name_percent
- Examples: total_revenue_amount, cost_ratio_percent, script_count_amount

**Ordering:** ORDER BY date columns DESC only. Use business-relevant names.
