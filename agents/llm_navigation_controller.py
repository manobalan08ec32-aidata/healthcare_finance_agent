from typing import Dict, List, Optional
import json
from state_schema import AgentState
from databricks_client import DatabricksClient

class LLMNavigationController:
    """LLM-powered navigation controller for intelligent routing decisions"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def route_user_query(self, state: AgentState) -> Dict[str, any]:
        """Main routing logic using LLM intelligence"""
        
        # 1. Analyze current question with full context
        analysis_result = self._analyze_question_with_context(state)
        
        # 2. Make routing decision based on analysis
        routing_decision = self._make_routing_decision(analysis_result, state)
        
        # 3. Determine memory management actions
        memory_actions = self._determine_memory_actions(routing_decision, state)
        
        return {
            **routing_decision,
            'memory_actions': memory_actions,
            'analysis_details': analysis_result
        }
    
    def _analyze_question_with_context(self, state: AgentState) -> Dict:
        """Use LLM to comprehensively analyze the question and context"""
        
        # Build context for LLM analysis
        conversation_context = self._build_conversation_context(state)
        
        analysis_prompt = f"""
        Healthcare Finance Question Analysis Task:
        
        Current Question: "{state['user_question']}"
        
        Conversation History: {conversation_context}
        
        Previous Query Results Available: {bool(state.get('query_results'))}
        Previous Dataset Used: {state.get('selected_dataset', 'None')}
        Previous Flow Type: {state.get('flow_type', 'None')}
        
        Analyze this question across multiple dimensions:
        
        1. QUESTION TYPE CLASSIFICATION:
           - Descriptive ("what", "how much", "show me") - seeks data/facts
           - Analytical ("why", "what caused", "root cause") - seeks explanations
           - Comparative ("vs", "compared to", "difference") - seeks comparisons
           - Exploratory ("trend", "pattern", "insights") - seeks discoveries
           - Follow-up (builds on previous question) - continues analysis
        
        2. CONTEXT ANALYSIS:
           - Is this a completely new question or related to previous?
           - Does it reference previous results implicitly?
           - Is there a context switch (new topic/focus area)?
           - What type of transition is this?
        
        3. INTENT DETECTION:
           - What is the user really trying to understand?
           - What level of analysis depth is needed?
           - Are there implicit comparison periods or benchmarks?
           - Is variance analysis likely needed?
        
        4. COMPLEXITY ASSESSMENT:
           - Simple lookup vs complex analysis
           - Single dataset vs multi-dataset needs
           - Immediate answer vs multi-step investigation
        
        5. HEALTHCARE FINANCE CONTEXT:
           - Claims analysis, financial variance, operational metrics?
           - Time period implications (quarterly, monthly, seasonal)
           - Stakeholder perspective (finance team, operations, executives)
        
        Respond with detailed JSON analysis:
        {{
            "question_classification": {{
                "primary_type": "descriptive|analytical|comparative|exploratory|follow_up",
                "confidence": 0.95,
                "reasoning": "detailed explanation",
                "secondary_types": ["other applicable types"]
            }},
            "context_analysis": {{
                "relationship_to_previous": "new_topic|builds_on_previous|context_switch|refinement",
                "transition_type": "none|what_to_why|why_to_what|topic_change|drill_down",
                "context_preservation_needed": true/false,
                "reasoning": "explanation of context relationship"
            }},
            "intent_analysis": {{
                "user_goal": "specific description of what user wants to achieve",
                "analysis_depth": "surface|detailed|comprehensive|investigative",
                "time_sensitivity": "current|historical|trending|comparative",
                "variance_analysis_likely": true/false,
                "implicit_comparisons": ["any implied comparisons detected"]
            }},
            "complexity_assessment": {{
                "complexity_level": "simple|moderate|complex|multi_step",
                "estimated_steps": 2,
                "multi_dataset_needed": true/false,
                "requires_domain_expertise": true/false
            }},
            "healthcare_finance_context": {{
                "domain_area": "claims|financial|operational|compliance",
                "stakeholder_level": "analyst|manager|executive",
                "business_criticality": "routine|important|critical",
                "regulatory_implications": true/false
            }}
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": analysis_prompt}
            ])
            
            return json.loads(llm_response)
            
        except json.JSONDecodeError as e:
            # Fallback analysis if JSON parsing fails
            return self._fallback_analysis(state)
        except Exception as e:
            raise Exception(f"Question analysis failed: {str(e)}")
    
    def _make_routing_decision(self, analysis: Dict, state: AgentState) -> Dict:
        """Use LLM to make intelligent routing decisions"""
        
        routing_prompt = f"""
        Healthcare Finance Agent Routing Decision:
        
        Question Analysis: {json.dumps(analysis, indent=2)}
        
        Current State:
        - Previous Results Available: {bool(state.get('query_results'))}
        - Current Dataset: {state.get('selected_dataset', 'None')}
        - Previous Agent: {state.get('current_agent', 'None')}
        - Conversation Length: {len(state.get('conversation_history', []))}
        
        Available Agents:
        1. router_agent: Dataset selection for new questions or dataset changes
        2. sql_template_agent: Find existing SQL patterns for query generation
        3. sql_agent: Generate and execute SQL queries
        4. variance_detection_agent: Analyze results for significant variances
        5. root_cause_agent: Investigate causes of variances using knowledge graph
        6. follow_up_agent: Generate relevant next questions
        
        Routing Rules:
        - NEW QUESTIONS (no previous context): Start with router_agent
        - FOLLOW-UP QUESTIONS (building on previous): May skip router if same dataset works
        - WHY QUESTIONS with previous results: Go to variance_detection_agent
        - WHY QUESTIONS without context: Start with router_agent for exploration
        - CONTEXT SWITCHES: Start with router_agent
        - REFINEMENTS: Continue with sql_agent if same dataset appropriate
        
        Make routing decision considering:
        1. Most efficient path to answer user's question
        2. Reusability of previous context and results
        3. Need for new dataset selection vs continuing with current
        4. Complexity and multi-step analysis requirements
        
        Respond with JSON:
        {{
            "next_agent": "specific_agent_name",
            "routing_reasoning": "detailed explanation of why this agent is optimal",
            "confidence": 0.95,
            "alternative_paths": [
                {{"agent": "alternative_agent", "reason": "why this could also work"}}
            ],
            "expected_flow": ["agent1", "agent2", "agent3"],
            "skip_agents": ["agents that can be skipped and why"],
            "special_instructions": {{
                "preserve_context": true/false,
                "dataset_selection_needed": true/false,
                "variance_analysis_likely": true/false,
                "root_cause_expected": true/false
            }}
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": routing_prompt}
            ])
            
            routing_decision = json.loads(llm_response)
            
            # Add metadata from analysis
            routing_decision.update({
                'flow_type': analysis.get('question_classification', {}).get('primary_type'),
                'transition_type': analysis.get('context_analysis', {}).get('transition_type'),
                'complexity_level': analysis.get('complexity_assessment', {}).get('complexity_level')
            })
            
            return routing_decision
            
        except json.JSONDecodeError:
            return self._fallback_routing_decision(analysis, state)
        except Exception as e:
            raise Exception(f"Routing decision failed: {str(e)}")
    
    def _determine_memory_actions(self, routing_decision: Dict, state: AgentState) -> Dict:
        """Use LLM to determine optimal memory management"""
        
        memory_prompt = f"""
        Memory Management Decision for Healthcare Finance Agent:
        
        Routing Decision: {json.dumps(routing_decision, indent=2)}
        
        Current State Memory:
        - Query Results: {bool(state.get('query_results'))}
        - Dataset Selected: {state.get('selected_dataset')}
        - SQL Generated: {bool(state.get('generated_sql'))}
        - Root Cause Context: {bool(state.get('root_cause_context'))}
        - Conversation History Length: {len(state.get('conversation_history', []))}
        
        Memory Management Options:
        1. PRESERVE_ALL: Keep all current context and results
        2. EXTRACT_INSIGHTS_AND_CLEAR: Save insights but clear heavy data
        3. CLEAR_TRANSIENT: Clear temporary results but keep patterns
        4. START_FRESH: Clear everything except user preferences
        5. SELECTIVE_PRESERVE: Keep specific elements based on next agent needs
        
        Consider:
        - Will the next agent benefit from current results?
        - Is this a context switch requiring fresh start?
        - Are there valuable insights to preserve before clearing?
        - Memory efficiency vs context preservation trade-off
        
        Decide optimal memory action:
        {{
            "memory_action": "PRESERVE_ALL|EXTRACT_INSIGHTS_AND_CLEAR|CLEAR_TRANSIENT|START_FRESH|SELECTIVE_PRESERVE",
            "reasoning": "detailed explanation of memory management choice",
            "preserve_elements": ["specific state elements to keep"],
            "clear_elements": ["specific state elements to clear"],
            "extract_insights": {{
                "dataset_preferences": true/false,
                "query_patterns": true/false,
                "user_behavior": true/false,
                "successful_approaches": true/false
            }},
            "memory_efficiency_score": 0.85
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": memory_prompt}
            ])
            
            return json.loads(llm_response)
            
        except Exception as e:
            # Safe fallback
            return {
                "memory_action": "PRESERVE_ALL",
                "reasoning": "Fallback to safe memory preservation",
                "preserve_elements": ["all"],
                "clear_elements": [],
                "extract_insights": {},
                "memory_efficiency_score": 0.5
            }
    
    def _build_conversation_context(self, state: AgentState) -> str:
        """Build concise conversation context for LLM analysis"""
        
        history = state.get('conversation_history', [])
        
        if not history:
            return "No previous conversation history"
        
        # Get last 3 conversations for context
        recent_history = history[-3:]
        
        context_parts = []
        for i, conv in enumerate(recent_history, 1):
            context_parts.append(f"""
            Previous Question {i}: "{conv.get('question', 'Unknown')}"
            Type: {conv.get('question_type', 'Unknown')}
            Dataset Used: {conv.get('dataset_used', 'Unknown')}
            Success: {conv.get('success', False)}
            """)
        
        return "\n".join(context_parts)
    
    def _fallback_analysis(self, state: AgentState) -> Dict:
        """Fallback analysis when LLM parsing fails"""
        
        question = state['user_question'].lower()
        
        # Simple heuristics as fallback
        if any(word in question for word in ['what', 'show', 'how much', 'how many']):
            primary_type = 'descriptive'
        elif any(word in question for word in ['why', 'cause', 'reason']):
            primary_type = 'analytical'
        elif any(word in question for word in ['compare', 'vs', 'versus']):
            primary_type = 'comparative'
        else:
            primary_type = 'exploratory'
        
        return {
            "question_classification": {
                "primary_type": primary_type,
                "confidence": 0.6,
                "reasoning": "Fallback analysis using simple heuristics"
            },
            "context_analysis": {
                "relationship_to_previous": "new_topic",
                "transition_type": "none",
                "context_preservation_needed": False
            },
            "intent_analysis": {
                "analysis_depth": "moderate",
                "variance_analysis_likely": False
            }
        }
    
    def _fallback_routing_decision(self, analysis: Dict, state: AgentState) -> Dict:
        """Fallback routing when LLM decision fails"""
        
        question_type = analysis.get('question_classification', {}).get('primary_type', 'descriptive')
        
        if question_type in ['analytical'] and state.get('query_results'):
            next_agent = 'variance_detection_agent'
        elif not state.get('selected_dataset'):
            next_agent = 'router_agent'
        else:
            next_agent = 'sql_agent'
        
        return {
            "next_agent": next_agent,
            "routing_reasoning": "Fallback routing using simple logic",
            "confidence": 0.6,
            "flow_type": question_type
        }

# Example usage
if __name__ == "__main__":
    db_client = DatabricksClient()
    nav_controller = LLMNavigationController(db_client)
    
    # Test various question types
    test_questions = [
        "What are Q3 pharmacy claims costs?",
        "Why did emergency visits spike in July?", 
        "Show me a breakdown of medical costs by age group",
        "What's driving the variance in our forecast?",
        "Compare this quarter's utilization to last year",
        "That's interesting - why are 65+ members driving costs?"
    ]
    
    for question in test_questions:
        print(f"\n🔍 Testing: {question}")
        
        state = AgentState(
            user_question=question,
            session_id="test_session"
        )
        
        try:
            routing = nav_controller.route_user_query(state)
            print(f"✅ Route: {routing['next_agent']}")
            print(f"📝 Type: {routing['flow_type']}")
            print(f"🎯 Reasoning: {routing['routing_reasoning'][:100]}...")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")