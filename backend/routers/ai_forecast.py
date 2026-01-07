"""
AI Demand Forecasting Router
Analyzes order history and predicts future demand using AI
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, List
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
from routers.base import get_erp_user, get_db
from emergentintegrations.llm.chat import LlmChat, UserMessage
import os
import json

forecast_router = APIRouter(prefix="/forecast", tags=["AI Demand Forecasting"])

FORECAST_SYSTEM_PROMPT = """You are a business analytics AI for Lucumaa Glass, a glass manufacturing company. 
Your job is to analyze order data and provide actionable demand forecasts and insights.

When given order data, analyze it and provide:
1. Overall demand trends (growing/declining/stable)
2. Peak periods and seasonal patterns
3. Top-performing products
4. Regional demand patterns if available
5. Recommendations for inventory and production planning
6. Predicted demand for next month

Format your response as JSON with the following structure:
{
  "summary": "Brief executive summary",
  "trend": "growing/declining/stable",
  "trend_percentage": number,
  "peak_periods": ["list of peak periods"],
  "top_products": [{"name": "product", "demand": number}],
  "recommendations": ["list of recommendations"],
  "next_month_prediction": {
    "estimated_orders": number,
    "estimated_revenue": number,
    "high_demand_products": ["list"]
  },
  "insights": ["additional insights"]
}

Be specific with numbers and percentages. Base predictions on the actual data provided."""


class ForecastResponse(BaseModel):
    summary: str
    trend: str
    trend_percentage: float
    peak_periods: List[str]
    top_products: List[dict]
    recommendations: List[str]
    next_month_prediction: dict
    insights: List[str]
    data_period: dict
    generated_at: str


@forecast_router.get("/demand")
async def get_demand_forecast(
    days: int = 90,
    current_user: dict = Depends(get_erp_user)
):
    """Get AI-powered demand forecast based on order history"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance", "manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    api_key = os.environ.get("EMERGENT_LLM_KEY")
    
    if not api_key:
        raise HTTPException(status_code=500, detail="AI service not configured")
    
    # Get order data for the specified period
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    
    orders = await db.orders.find(
        {"created_at": {"$gte": start_date}},
        {"_id": 0}
    ).to_list(1000)
    
    if len(orders) < 5:
        # Not enough data - return basic response
        return {
            "summary": "Insufficient data for AI forecasting. Need at least 5 orders.",
            "trend": "unknown",
            "trend_percentage": 0,
            "peak_periods": [],
            "top_products": [],
            "recommendations": ["Collect more order data for accurate forecasting"],
            "next_month_prediction": {
                "estimated_orders": 0,
                "estimated_revenue": 0,
                "high_demand_products": []
            },
            "insights": ["Start tracking orders to enable AI forecasting"],
            "data_period": {"start": start_date, "end": datetime.now().strftime("%Y-%m-%d"), "order_count": len(orders)},
            "generated_at": datetime.now(timezone.utc).isoformat()
        }
    
    # Prepare order summary for AI
    order_summary = {
        "total_orders": len(orders),
        "period": f"{days} days",
        "orders_by_date": {},
        "orders_by_product": {},
        "total_revenue": 0,
        "avg_order_value": 0
    }
    
    for order in orders:
        # By date
        date = order.get("created_at", "")[:10]
        if date:
            order_summary["orders_by_date"][date] = order_summary["orders_by_date"].get(date, 0) + 1
        
        # By product
        product = order.get("glass_type", order.get("product_name", "Unknown"))
        order_summary["orders_by_product"][product] = order_summary["orders_by_product"].get(product, 0) + 1
        
        # Revenue
        order_summary["total_revenue"] += order.get("total_price", 0)
    
    order_summary["avg_order_value"] = order_summary["total_revenue"] / len(orders) if orders else 0
    
    # Call AI for analysis
    try:
        chat = LlmChat(
            api_key=api_key,
            system_message=FORECAST_SYSTEM_PROMPT
        ).with_model("openai", "gpt-4o-mini")
        
        prompt = f"""Analyze this order data and provide demand forecast:

Order Summary:
- Total Orders: {order_summary['total_orders']}
- Period: {order_summary['period']}
- Total Revenue: ₹{order_summary['total_revenue']:,.0f}
- Average Order Value: ₹{order_summary['avg_order_value']:,.0f}

Orders by Date (last 30 entries):
{json.dumps(dict(list(order_summary['orders_by_date'].items())[-30:]), indent=2)}

Orders by Product:
{json.dumps(order_summary['orders_by_product'], indent=2)}

Provide your analysis in the JSON format specified."""

        response = await chat.send_message(UserMessage(text=prompt))
        
        # Parse JSON response
        try:
            # Extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                forecast_data = json.loads(response[json_start:json_end])
            else:
                raise ValueError("No JSON found")
        except:
            # Fallback structured response
            forecast_data = {
                "summary": response[:500],
                "trend": "stable",
                "trend_percentage": 0,
                "peak_periods": [],
                "top_products": [{"name": k, "demand": v} for k, v in list(order_summary["orders_by_product"].items())[:5]],
                "recommendations": ["Review the detailed analysis above"],
                "next_month_prediction": {
                    "estimated_orders": int(len(orders) / (days/30)),
                    "estimated_revenue": order_summary["total_revenue"] / (days/30),
                    "high_demand_products": list(order_summary["orders_by_product"].keys())[:3]
                },
                "insights": [response[:200]]
            }
        
        forecast_data["data_period"] = {
            "start": start_date,
            "end": datetime.now().strftime("%Y-%m-%d"),
            "order_count": len(orders)
        }
        forecast_data["generated_at"] = datetime.now(timezone.utc).isoformat()
        
        return forecast_data
        
    except Exception as e:
        print(f"AI Forecast error: {e}")
        # Return basic statistical analysis
        return {
            "summary": f"Based on {len(orders)} orders in the last {days} days, total revenue is ₹{order_summary['total_revenue']:,.0f}",
            "trend": "stable",
            "trend_percentage": 0,
            "peak_periods": [],
            "top_products": [{"name": k, "demand": v} for k, v in sorted(order_summary["orders_by_product"].items(), key=lambda x: -x[1])[:5]],
            "recommendations": ["AI analysis temporarily unavailable. Using basic statistics."],
            "next_month_prediction": {
                "estimated_orders": int(len(orders) / (days/30)),
                "estimated_revenue": int(order_summary["total_revenue"] / (days/30)),
                "high_demand_products": list(order_summary["orders_by_product"].keys())[:3]
            },
            "insights": [f"Average {len(orders)/(days/30):.1f} orders per month"],
            "data_period": {"start": start_date, "end": datetime.now().strftime("%Y-%m-%d"), "order_count": len(orders)},
            "generated_at": datetime.now(timezone.utc).isoformat()
        }


@forecast_router.get("/stats")
async def get_order_statistics(
    current_user: dict = Depends(get_erp_user)
):
    """Get basic order statistics for dashboard"""
    if current_user.get("role") not in ["super_admin", "admin", "owner", "finance", "manager"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Get stats for different periods
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    month_ago = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    year_ago = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    
    # Today's orders
    today_orders = await db.orders.count_documents({"created_at": {"$regex": f"^{today}"}})
    
    # This week
    week_orders = await db.orders.count_documents({"created_at": {"$gte": week_ago}})
    
    # This month
    month_orders = await db.orders.count_documents({"created_at": {"$gte": month_ago}})
    
    # This year
    year_orders = await db.orders.count_documents({"created_at": {"$gte": year_ago}})
    
    # Revenue calculations
    month_pipeline = [
        {"$match": {"created_at": {"$gte": month_ago}}},
        {"$group": {"_id": None, "total": {"$sum": "$total_price"}}}
    ]
    month_revenue = await db.orders.aggregate(month_pipeline).to_list(1)
    
    # Top products this month
    product_pipeline = [
        {"$match": {"created_at": {"$gte": month_ago}}},
        {"$group": {"_id": "$glass_type", "count": {"$sum": 1}, "revenue": {"$sum": "$total_price"}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    top_products = await db.orders.aggregate(product_pipeline).to_list(5)
    
    return {
        "orders": {
            "today": today_orders,
            "this_week": week_orders,
            "this_month": month_orders,
            "this_year": year_orders
        },
        "revenue": {
            "this_month": month_revenue[0]["total"] if month_revenue else 0
        },
        "top_products": [{"name": p["_id"] or "Unknown", "count": p["count"], "revenue": p["revenue"]} for p in top_products],
        "generated_at": now.isoformat()
    }
