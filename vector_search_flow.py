def get_workflow_for_session(session_id: str):
    """Get or create workflow instance for current session with proper user isolation"""
    
    # Store in session state instead of shared cache
    workflow_key = f"workflow_instance_{session_id}"
    
    if workflow_key not in st.session_state:
        print(f"🔧 Creating new workflow for session: {session_id}")
        try:
            print("🔧 Step 1: Creating DatabricksClient...")
            db_client = DatabricksClient()
            print("✅ Step 1: DatabricksClient created successfully")
            
            print("🔧 Step 2: Creating HealthcareFinanceWorkflow...")
            workflow = HealthcareFinanceWorkflow(db_client)
            print("✅ Step 2: HealthcareFinanceWorkflow created successfully")
            
            st.session_state[workflow_key] = workflow
            print(f"✅ Workflow created and cached for session: {session_id}")
        except Exception as e:
            print(f"❌ DETAILED ERROR in workflow creation: {str(e)}")
            print(f"❌ ERROR TYPE: {type(e).__name__}")
            import traceback
            print(f"❌ FULL TRACEBACK: {traceback.format_exc()}")
            return None
    else:
        print(f"♻️ Using existing workflow for session: {session_id}")
    
    return st.session_state[workflow_key]
