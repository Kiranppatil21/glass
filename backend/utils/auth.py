"""
Authentication utilities - Password hashing and JWT token management
"""
import bcrypt
import jwt
import os
from datetime import datetime, timezone, timedelta

JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def create_token(user_id: str, email: str, role: str, name: str = "") -> str:
    """Create a JWT token for authenticated user"""
    payload = {
        'user_id': user_id,
        'email': email,
        'role': role,
        'name': name,
        'exp': datetime.now(timezone.utc) + timedelta(days=7)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')


def decode_token(token: str) -> dict:
    """Decode and verify a JWT token"""
    return jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
