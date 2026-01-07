"""
Base module for ERP routers - shared dependencies and utilities
"""
from fastapi import Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional

# Shared security and database references
db = None
_auth_dependency = None
security = HTTPBearer(auto_error=False)  # Changed to False for query param fallback

def init_router_dependencies(database, auth_dependency):
    """Initialize shared dependencies for all routers"""
    global db, _auth_dependency
    db = database
    _auth_dependency = auth_dependency

def get_db():
    """Get database instance"""
    return db

async def get_erp_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    token: Optional[str] = Query(None)  # Support token via query param for PDF downloads
):
    """
    Authentication dependency for ERP routes.
    Supports both Authorization header and query param token.
    """
    # Try to get token from either source
    auth_token = None
    
    if credentials:
        auth_token = credentials.credentials
    elif token:
        auth_token = token
    
    if not auth_token:
        raise HTTPException(status_code=401, detail="Authentication required")
    
    if not _auth_dependency:
        raise HTTPException(status_code=500, detail="Auth not configured")
    
    try:
        # Create a fake credentials object for the auth dependency
        class FakeCredentials:
            def __init__(self, token):
                self.credentials = token
        
        return await _auth_dependency(FakeCredentials(auth_token))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

