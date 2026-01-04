"""
Enterprise Logging Utility for DANA Chatbot
Lightweight logger with Azure Application Insights integration and user context
"""
import logging
import os
import sys
from typing import Optional

# Global flag to track if Azure handler is initialized
_azure_handler_initialized = False

def setup_logger(name: str) -> logging.Logger:
    """
    Create a lightweight logger for console + Azure Application Insights
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured logger instance
    """
    global _azure_handler_initialized
    
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Console Handler - Always enabled
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    logger.addHandler(console_handler)
    
    # Azure Application Insights - Only once, only if connection string exists
    connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if connection_string and not _azure_handler_initialized:
        try:
            from opencensus.ext.azure.log_exporter import AzureLogHandler
            azure_handler = AzureLogHandler(connection_string=connection_string)
            azure_handler.setLevel(logging.INFO)
            logger.addHandler(azure_handler)
            _azure_handler_initialized = True
        except ImportError:
            pass  # opencensus not installed, skip silently
        except Exception:
            pass  # Azure connection failed, skip silently
    
    return logger


def log_with_user_context(
    logger: logging.Logger,
    level: str,
    message: str,
    user_email: Optional[str] = None,
    session_id: Optional[str] = None,
    **extra_fields
):
    """
    Log a message with user context as custom dimensions
    
    Args:
        logger: Logger instance
        level: 'info', 'warning', 'error'
        message: Log message
        user_email: User's email for filtering
        session_id: Session ID for tracking conversation flow
        **extra_fields: Any additional fields to log
    """
    # Build custom dimensions for App Insights filtering
    custom_dimensions = {}
    if user_email:
        custom_dimensions['user_email'] = user_email
    if session_id:
        custom_dimensions['session_id'] = session_id
    if extra_fields:
        custom_dimensions.update(extra_fields)
    
    # Format message with context for console
    context_parts = []
    if user_email:
        context_parts.append(f"user={user_email}")
    if session_id:
        context_parts.append(f"session={session_id[:8]}...")
    
    console_message = f"{message}"
    if context_parts:
        console_message = f"[{', '.join(context_parts)}] {message}"
    
    # Log with extra for Azure custom dimensions
    extra = {'custom_dimensions': custom_dimensions} if custom_dimensions else {}
    
    if level == 'info':
        logger.info(console_message, extra=extra)
    elif level == 'warning':
        logger.warning(console_message, extra=extra)
    elif level == 'error':
        logger.error(console_message, extra=extra, exc_info=True)
