# Create placeholder with user-friendly node tracking messages
if name not in ('__end__', 'workflow_end') and name and name not in node_placeholders:
    node_placeholders[name] = status_region.empty()
    
    # User-friendly messages for different nodes
    if name == 'navigation_controller':
        node_placeholders[name].info("🔍 Validating your question...")
    elif name == 'router_agent':
        node_placeholders[name].info("🎯 Finding the right dataset for your question...")
        # Start progressive status updates in background
        asyncio.create_task(show_progressive_router_status(node_placeholders[name], state))
    elif name == 'strategy_planner_agent':
        node_placeholders[name].info("🧠 Planning strategic analysis...")
    elif name == 'drillthrough_planner_agent':
        node_placeholders[name].info("🔧 Preparing drillthrough analysis...")
    else:
        node_placeholders[name].info(f"▶️ {name} running...")

async def show_progressive_router_status(placeholder, state):
    """Show progressive status updates for router_agent node"""
    try:
        # Initial message (already shown at line 184)
        await asyncio.sleep(4)  # Wait 4 seconds
        
        # Check if we're still processing (placeholder exists)
        if placeholder and 'router_agent' in node_placeholders:
            placeholder.info("⚙️ Generating SQL query based on metadata...")
        
        await asyncio.sleep(2)  # Wait 2 more seconds
        
        # Check again before showing next message
        if placeholder and 'router_agent' in node_placeholders:
            placeholder.info("🔄 Executing SQL query...")
    except Exception as e:
        print(f"⚠️ Progressive status update error: {e}")

# Handle user_friendly_message (show once, don't persist)
                    user_friendly_msg = state.get('user_friendly_message')
                    if user_friendly_msg:
                        # Show temporarily using a status placeholder, don't add to messages
                        if 'navigation_controller' in node_placeholders:
                            node_placeholders['navigation_controller'].info(f"💬 {user_friendly_msg}")
                        print(f"💬 Displayed user-friendly message (not persisted): {user_friendly_msg}")
