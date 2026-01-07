"""
AI Chat Support Router
Customer support chatbot for glass manufacturing inquiries
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
from emergentintegrations.llm.chat import LlmChat, UserMessage
import os
from dotenv import load_dotenv

load_dotenv()

chat_router = APIRouter(prefix="/chat", tags=["AI Chat Support"])

# Store chat sessions in memory (in production, use database)
chat_sessions = {}

SYSTEM_PROMPT = """You are a friendly and helpful customer support assistant for Lucumaa Glass, a premium glass manufacturing company in India.

## About Lucumaa Glass:
- We manufacture high-quality toughened glass, laminated glass, decorative glass, and specialty glass products
- Factory & Corporate Office: Same location (customer can visit)
- Phone: 9284701985
- Email: book@lucumaaglass.in (bookings), info@lucumaaglass.in (information), sales@lucumaaglass.in (sales/purchase)

## Our Products:
1. **Toughened Glass** - 5-12mm thickness, 5x stronger than normal glass, ideal for doors, windows, shower enclosures
2. **Laminated Safety Glass** - PVB interlayer, ideal for skylights, balustrades, security applications
3. **Decorative Glass** - Frosted, tinted, patterned options for privacy and aesthetics
4. **Mirror Glass** - Silver and aluminum coated mirrors
5. **Double Glazed Units (DGU)** - Energy efficient, sound insulation

## Pricing:
- Pricing depends on glass type, thickness, size, and quantity
- Wholesale discounts available for bulk orders (50+ sqft: 10% off, 100+ sqft: 15% off, 500+ sqft: 20% off)
- Custom quotes available for large projects

## Order Process:
1. Browse products or use our customization tool
2. Select glass type, dimensions, and quantity
3. Get instant pricing
4. Pay advance (25-100% based on order value)
5. Production (3-7 days)
6. Quality check & dispatch
7. Track order via QR code or order number

## Guidelines for responses:
- Be warm, professional, and helpful
- Keep responses concise but informative
- Always provide contact options for complex queries
- Suggest relevant products based on customer needs
- If you don't know something specific, direct them to contact sales team
- Use ₹ for prices (Indian Rupees)
- Respond in the same language the customer uses (Hindi/English)
"""

class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str


@chat_router.post("/message", response_model=ChatResponse)
async def send_chat_message(chat_msg: ChatMessage):
    """Send a message to the AI support chatbot"""
    
    api_key = os.environ.get("EMERGENT_LLM_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="AI service not configured")
    
    # Generate or use existing session ID
    session_id = chat_msg.session_id or f"chat_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    
    try:
        # Initialize chat with session
        chat = LlmChat(
            api_key=api_key,
            session_id=session_id,
            system_message=SYSTEM_PROMPT
        ).with_model("openai", "gpt-4o-mini")
        
        # Load previous messages if session exists
        if session_id in chat_sessions:
            for msg in chat_sessions[session_id]:
                if msg["role"] == "user":
                    chat.add_user_message(msg["content"])
                else:
                    chat.add_assistant_message(msg["content"])
        
        # Send new message
        user_message = UserMessage(text=chat_msg.message)
        response = await chat.send_message(user_message)
        
        # Store in session
        if session_id not in chat_sessions:
            chat_sessions[session_id] = []
        
        chat_sessions[session_id].append({"role": "user", "content": chat_msg.message})
        chat_sessions[session_id].append({"role": "assistant", "content": response})
        
        # Limit session history
        if len(chat_sessions[session_id]) > 20:
            chat_sessions[session_id] = chat_sessions[session_id][-20:]
        
        return ChatResponse(response=response, session_id=session_id)
        
    except Exception as e:
        print(f"Chat error: {e}")
        # Fallback response
        return ChatResponse(
            response="I apologize, but I'm having trouble connecting right now. Please contact us directly at 9284701985 or email sales@lucumaaglass.in for assistance.",
            session_id=session_id
        )


@chat_router.delete("/session/{session_id}")
async def clear_chat_session(session_id: str):
    """Clear a chat session"""
    if session_id in chat_sessions:
        del chat_sessions[session_id]
    return {"message": "Session cleared"}
