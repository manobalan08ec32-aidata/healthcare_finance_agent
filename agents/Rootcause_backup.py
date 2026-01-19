async def search_column_values(
        self,
        search_terms: List[str],
        max_columns: int = 7,
        max_values_per_column: int = 5
    ) -> List[str]:
        """
        Search column values with regex word boundary matching.
        Groups results by column_name and value, showing which tables contain each value.
        
        Single word: Exact match OR prefix only (no contains)
        Multi-word: 
            - Tier 1: Exact match
            - Tier 2: Prefix match
            - Tier 3: Contains full phrase
            - Tier 4: ALL words with boundary
            - Tier 5: FIRST N-1 words (fallback, only if Tier 1-4 empty)
            - Tier 6: FIRST 3 words (fallback, only if Tier 1-5 empty)
        """
        
        if not search_terms:
            return []
        
        # Escape single quotes for SQL
        def escape_sql(val):
            return val.replace("'", "''")
        
        # Build word boundary regex pattern for a word
        def word_boundary_pattern(word):
            """
            Creates regex pattern for word with boundary.
            Allows optional 's' for plurals.
            """
            escaped = escape_sql(word)
            return f"(^|[^a-z]){escaped}(s)?($|[^a-z0-9])"
        
        # Build combined RLIKE condition for list of words
        def build_rlike_condition(words):
            conditions = [f"value_lower RLIKE '{word_boundary_pattern(w)}'" for w in words]
            return " AND ".join(conditions)
        
        # Process search terms
        search_items = []
        
        for term in search_terms:
            term_lower = term.strip().lower()
            if not term_lower:
                continue
            
            words = term_lower.split()
            is_multi_word = len(words) > 1
            
            search_items.append({
                "phrase": term_lower,
                "words": words,
                "is_multi_word": is_multi_word,
                "word_count": len(words)
            })
        
        if not search_items:
            return []
        
        print(f"🔍 Search items: {[s['phrase'] for s in search_items]}")
        
        # Build SQL conditions for WHERE clause
        all_conditions = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            word_count = item["word_count"]
            phrase_escaped = escape_sql(phrase)
            
            if is_multi_word:
                # Multi-word: exact, prefix, contains, and word boundary tiers
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
                all_conditions.append(f"value_lower LIKE '%{phrase_escaped}%'")
                
                # Tier 4: All words with boundary
                all_conditions.append(f"({build_rlike_condition(words)})")
                
                # Tier 5: First N-1 words (for 3+ words)
                if word_count >= 3:
                    first_n_minus_1 = words[:word_count - 1]
                    all_conditions.append(f"({build_rlike_condition(first_n_minus_1)})")
                
                # Tier 6: First 3 words (for 5+ words)
                if word_count >= 5:
                    first_3 = words[:3]
                    all_conditions.append(f"({build_rlike_condition(first_3)})")
            else:
                # Single word: exact and prefix ONLY (no contains %word%)
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
        
        where_clause = " OR ".join(all_conditions)
        
        # Build tier CASE statement
        tier_cases = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            word_count = item["word_count"]
            phrase_escaped = escape_sql(phrase)
            
            # Tier 1: Exact (both single and multi-word)
            tier_cases.append(f"WHEN value_lower = '{phrase_escaped}' THEN 1")
            
            # Tier 2: Prefix (both single and multi-word)
            tier_cases.append(f"WHEN value_lower LIKE '{phrase_escaped}%' THEN 2")
            
            if is_multi_word:
                # Tier 3: Contains full phrase (multi-word only)
                tier_cases.append(f"WHEN value_lower LIKE '%{phrase_escaped}%' THEN 3")
                
                # Tier 4: All words with boundary
                tier_cases.append(f"WHEN {build_rlike_condition(words)} THEN 4")
                
                # Tier 5: First N-1 words (for 3+ words)
                if word_count >= 3:
                    first_n_minus_1 = words[:word_count - 1]
                    tier_cases.append(f"WHEN {build_rlike_condition(first_n_minus_1)} THEN 5")
                
                # Tier 6: First 3 words (for 5+ words)
                if word_count >= 5:
                    first_3 = words[:3]
                    tier_cases.append(f"WHEN {build_rlike_condition(first_3)} THEN 6")
            # Single word: No Tier 3-6, will fall to ELSE 7 if not exact/prefix
        
        tier_case_statement = "CASE " + " ".join(tier_cases) + " ELSE 7 END"
        
        # Build query - now includes table_name
        query = f"""
        SELECT 
            column_name,
            table_name,
            value_lower,
            {tier_case_statement} as tier
        FROM prd_optumrx_orxfdmprdsa.rag.column_values_index
        WHERE {where_clause}
        ORDER BY tier, column_name, value_lower
        """
        
        print(f"🔍 Executing search query...")
        print(f"📝 Query preview: {query[:500]}...")
        
        try:
            # Execute query
            result_data = await self.execute_sql_async_audit(query)
            
            # Convert to list
            if hasattr(result_data, 'collect'):
                rows = [row.asDict() for row in result_data.collect()]
            elif isinstance(result_data, list):
                rows = result_data
            else:
                print(f"❌ Unexpected result type: {type(result_data)}")
                return []
            
            if not rows:
                print("❌ No matches found")
                return []
            
            # Convert tier to int (may come as string from SQL)
            for row in rows:
                row["tier"] = int(row["tier"])
            
            # Filter out Tier 7 (no match)
            rows = [r for r in rows if r["tier"] <= 6]
            
            if not rows:
                print("❌ No valid matches found")
                return []
            
            print(f"✅ Found {len(rows)} total matches before tier filtering")
            
            # Apply exclusive tier group logic
            rows = self._apply_tier_group_filter(rows)
            
            if not rows:
                print("❌ No matches after tier group filtering")
                return []
            
            print(f"✅ Found {len(rows)} matches after tier filtering")
            
            # Group by (column_name, value_lower) to collect tables
            value_data = {}
            
            for row in rows:
                col = row["column_name"]
                val = row["value_lower"]
                table = row["table_name"]
                tier = row["tier"]
                
                key = (col, val)
                
                if key not in value_data:
                    value_data[key] = {
                        "tier": tier,
                        "tables": []
                    }
                
                # Update tier if better
                if tier < value_data[key]["tier"]:
                    value_data[key]["tier"] = tier
                
                # Add table if not already present
                if table not in value_data[key]["tables"]:
                    value_data[key]["tables"].append(table)
            
            # Group by column for output
            column_groups = {}
            
            for (col, val), info in value_data.items():
                if col not in column_groups:
                    column_groups[col] = {
                        "best_tier": info["tier"],
                        "values": []
                    }
                
                # Update best tier for column
                if info["tier"] < column_groups[col]["best_tier"]:
                    column_groups[col]["best_tier"] = info["tier"]
                
                # Add value with its tables and tier
                column_groups[col]["values"].append({
                    "value": val,
                    "tables": sorted(info["tables"]),
                    "tier": info["tier"]
                })
            
            # Sort columns by best tier, then column name
            sorted_columns = sorted(
                column_groups.items(),
                key=lambda x: (x[1]["best_tier"], x[0])
            )
            
            # Format output
            results = []
            for col_name, col_info in sorted_columns[:max_columns]:
                # Sort values by tier, then alphabetically
                sorted_values = sorted(
                    col_info["values"], 
                    key=lambda x: (x["tier"], x["value"])
                )
                
                # Take top N values
                top_values = sorted_values[:max_values_per_column]
                
                # Format: value1, value2 (table1, table2)
                value_strs = []
                for v_info in top_values:
                    tables_str = ", ".join(v_info["tables"])
                    value_strs.append(f"{v_info['value']} ({tables_str})")
                
                values_combined = ", ".join(value_strs)
                
                results.append(f"{col_name} - {values_combined}")
                print(f"   Tier {col_info['best_tier']}: {col_name} → {value_strs}")
            
            print(f"✅ Search complete: {len(results)} columns")
            return results
            
        except Exception as e:
            print(f"❌ Error in search_column_values: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
