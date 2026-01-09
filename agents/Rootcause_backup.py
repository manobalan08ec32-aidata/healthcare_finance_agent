async def search_column_values(
        self,
        search_terms: List[str],
        max_columns: int = 7,
        max_values_per_column: int = 5
    ) -> List[str]:
        """
        Search column values with regex word boundary matching.
        
        Single word: Simple LIKE
        Multi-word: 
            - Tier 1: Exact match
            - Tier 2: Prefix match
            - Tier 3: Contains full phrase
            - Tier 4: RLIKE with word boundary for each word
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
            Pattern: (^|[^a-z])word(s)?([^a-z]|$)
            """
            escaped = escape_sql(word)
            return f"(^|[^a-z]){escaped}(s)?([^a-z]|$)"
        
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
                "is_multi_word": is_multi_word
            })
        
        if not search_items:
            return []
        
        print(f"🔍 Search items: {[s['phrase'] for s in search_items]}")
        
        # Build SQL conditions
        all_conditions = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            phrase_escaped = escape_sql(phrase)
            
            if is_multi_word:
                # Multi-word: exact, prefix, contains, regex
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
                all_conditions.append(f"value_lower LIKE '%{phrase_escaped}%'")
                
                # Regex: all words with boundary
                regex_conditions = [f"value_lower RLIKE '{word_boundary_pattern(w)}'" for w in words]
                combined_regex = " AND ".join(regex_conditions)
                all_conditions.append(f"({combined_regex})")
            else:
                # Single word: exact, prefix, contains
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
                all_conditions.append(f"value_lower LIKE '%{phrase_escaped}%'")
        
        where_clause = " OR ".join(all_conditions)
        
        # Build tier CASE statement
        tier_cases = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            phrase_escaped = escape_sql(phrase)
            
            # Tier 1: Exact
            tier_cases.append(f"WHEN value_lower = '{phrase_escaped}' THEN 1")
            
            # Tier 2: Prefix
            tier_cases.append(f"WHEN value_lower LIKE '{phrase_escaped}%' THEN 2")
            
            # Tier 3: Contains full phrase
            tier_cases.append(f"WHEN value_lower LIKE '%{phrase_escaped}%' THEN 3")
            
            if is_multi_word:
                # Tier 4: All words with boundary (any order)
                regex_conditions = [f"value_lower RLIKE '{word_boundary_pattern(w)}'" for w in words]
                combined_regex = " AND ".join(regex_conditions)
                tier_cases.append(f"WHEN {combined_regex} THEN 4")
        
        tier_case_statement = "CASE " + " ".join(tier_cases) + " ELSE 5 END"
        
        # Build query
        query = f"""
        SELECT 
            column_name,
            value_lower,
            {tier_case_statement} as tier
        FROM prd_optumrx_orxfdmprdsa.rag.column_values_index
        WHERE {where_clause}
        ORDER BY column_name, tier, value_lower
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
            
            # Filter out Tier 5 (shouldn't match but just in case)
            rows = [r for r in rows if r["tier"] <= 4]
            
            if not rows:
                print("❌ No valid matches found")
                return []
            
            print(f"✅ Found {len(rows)} matches")
            
            # Group by column
            column_data = {}
            
            for row in rows:
                col = row["column_name"]
                val = row["value_lower"]
                tier = row["tier"]
                
                if col not in column_data:
                    column_data[col] = {"best_tier": tier, "values": []}
                
                # Update best tier
                if tier < column_data[col]["best_tier"]:
                    column_data[col]["best_tier"] = tier
                
                # Add value if not duplicate
                existing_values = [v[0] for v in column_data[col]["values"]]
                if val not in existing_values:
                    column_data[col]["values"].append((val, tier))
            
            # Sort columns by best tier, then column name
            sorted_columns = sorted(
                column_data.items(),
                key=lambda x: (x[1]["best_tier"], x[0])
            )
            
            # Format output
            results = []
            for col_name, col_info in sorted_columns[:max_columns]:
                # Sort values by tier, then alphabetically
                sorted_values = sorted(col_info["values"], key=lambda x: (x[1], x[0]))
                
                # Take top N values
                top_values = [v[0] for v in sorted_values[:max_values_per_column]]
                values_str = ", ".join(top_values)
                
                results.append(f"{col_name} - {values_str}")
                print(f"   Tier {col_info['best_tier']}: {col_name} → {top_values}")
            
            print(f"✅ Search complete: {len(results)} columns")
            return results
            
        except Exception as e:
            print(f"❌ Error in search_column_values: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
