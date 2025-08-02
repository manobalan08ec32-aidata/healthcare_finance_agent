from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

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
        
        return {
            **routing_decision,
            'analysis_details': analysis_result
        }
    
    def _analyze_question_with_context(self, state: AgentState) -> Dict:
        """Use LLM to comprehensively analyze the question and context"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        
        analysis_prompt = f"""
        Healthcare Finance Question Analysis Task:
        
        Current Question: "{user_question}"
        
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
                "context_preservation_needed": true,
                "reasoning": "explanation of context relationship"
            }},
            "intent_analysis": {{
                "user_goal": "specific description of what user wants to achieve",
                "analysis_depth": "surface|detailed|comprehensive|investigative",
                "time_sensitivity": "current|historical|trending|comparative",
                "variance_analysis_likely": true,
                "implicit_comparisons": ["any implied comparisons detected"]
            }},
            "complexity_assessment": {{
                "complexity_level": "simple|moderate|complex|multi_step",
                "estimated_steps": 2,
                "multi_dataset_needed": true,
                "requires_domain_expertise": true
            }},
            "healthcare_finance_context": {{
                "domain_area": "claims|financial|operational|compliance",
                "stakeholder_level": "analyst|manager|executive",
                "business_criticality": "routine|important|critical",
                "regulatory_implications": true
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
                "preserve_context": true,
                "dataset_selection_needed": true,
                "variance_analysis_likely": false,
                "root_cause_expected": false
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
    
    def _fallback_analysis(self, state: AgentState) -> Dict:
        """Fallback analysis when LLM parsing fails"""
        
        question = state.get('current_question', state.get('original_question', '')).lower()
        
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