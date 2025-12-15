"""Simple runner for the MCP FastAPI app using uvicorn.

Usage: python -m snapred.mcp_main
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run("snapred.mcp_server:app", host="127.0.0.1", port=8000, log_level="info")
