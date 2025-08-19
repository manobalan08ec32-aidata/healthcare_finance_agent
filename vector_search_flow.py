.followup-buttons-container {
    display: flex !important;
    flex-direction: row !important; /* Arrange buttons horizontally */
    flex-wrap: wrap !important;     /* Allow buttons to wrap to the next line */
    gap: 8px !important;
    align-items: center !important;
    justify-content: flex-start !important;
    width: 95% !important;
    margin: 0.5rem 0 !important;
    padding: 0 !important;
}
.stButton > button {
    background-color: white !important;
    color: #007bff !important;
    border: 1px solid #007bff !important;
    border-radius: 16px !important; /* Make them more pill-shaped */
    padding: 4px 12px !important;   /* Reduce vertical and horizontal padding */
    margin: 0 !important;
    width: auto !important;         /* Button width fits the text */
    text-align: center !important;
    font-size: 13px !important;     /* Slightly smaller font */
    line-height: 1.2 !important;
    transition: all 0.2s ease !important;
    height: auto !important;
    min-height: 28px !important;
    white-space: nowrap !important; /* Keep text on a single line */
}

if __name__ != "__main__":
    # This is the part Gunicorn looks for
    from streamlit.web.server.server import Server
    
    # Create a server instance
    server = Server()
    
    # This is the 'app' object Gunicorn will run
    app = server.get_app()

pip install -r requirements.txt && gunicorn -w 1 --threads 8 -k uvicorn.workers.UvicornWorker main:app --timeout 600
