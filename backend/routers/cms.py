"""
CMS (Content Management System) Router
Manage website content: pages, banners, menus, blogs, SEO
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel
from routers.base import get_erp_user, get_db
import uuid
import os
import re

cms_router = APIRouter(prefix="/cms", tags=["CMS"])


# ================== MODELS ==================

class PageCreate(BaseModel):
    title: str
    slug: str
    content: str
    meta_title: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    status: str = "draft"  # draft, published
    template: str = "default"

class BannerCreate(BaseModel):
    title: str
    subtitle: Optional[str] = None
    image_url: str
    link_url: Optional[str] = None
    button_text: Optional[str] = None
    position: str = "home_hero"  # home_hero, home_secondary, page_header
    order: int = 0
    active: bool = True

class MenuItemCreate(BaseModel):
    title: str
    url: str
    parent_id: Optional[str] = None
    order: int = 0
    target: str = "_self"  # _self, _blank
    menu_location: str = "header"  # header, footer

class BlogPostCreate(BaseModel):
    title: str
    slug: str
    excerpt: Optional[str] = None
    content: str
    featured_image: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = []
    meta_title: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    status: str = "draft"

class ContactInfoUpdate(BaseModel):
    phone: Optional[str] = None
    email_booking: Optional[str] = None
    email_info: Optional[str] = None
    email_sales: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    google_maps_url: Optional[str] = None
    whatsapp: Optional[str] = None
    working_hours: Optional[str] = None


# ================== PAGES ==================

@cms_router.get("/pages")
async def get_pages(
    status: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all pages"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    query = {"type": "page"}
    if status:
        query["status"] = status
    
    pages = await db.cms_content.find(query, {"_id": 0}).sort("created_at", -1).to_list(100)
    return {"pages": pages}


@cms_router.post("/pages")
async def create_page(
    page: PageCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a new page"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Check slug uniqueness
    existing = await db.cms_content.find_one({"type": "page", "slug": page.slug})
    if existing:
        raise HTTPException(status_code=400, detail="Slug already exists")
    
    page_doc = {
        "id": str(uuid.uuid4()),
        "type": "page",
        **page.dict(),
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.cms_content.insert_one(page_doc)
    page_doc.pop("_id", None)
    
    return {"message": "Page created", "page": page_doc}


@cms_router.put("/pages/{page_id}")
async def update_page(
    page_id: str,
    page: PageCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Update a page"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    result = await db.cms_content.update_one(
        {"id": page_id, "type": "page"},
        {"$set": {
            **page.dict(),
            "updated_by": current_user.get("name"),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Page not found")
    
    return {"message": "Page updated"}


@cms_router.delete("/pages/{page_id}")
async def delete_page(
    page_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete a page"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    result = await db.cms_content.delete_one({"id": page_id, "type": "page"})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Page not found")
    
    return {"message": "Page deleted"}


# ================== BANNERS ==================

@cms_router.get("/banners")
async def get_banners(
    position: Optional[str] = None,
    current_user: dict = Depends(get_erp_user)
):
    """Get all banners"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    query = {"type": "banner"}
    if position:
        query["position"] = position
    
    banners = await db.cms_content.find(query, {"_id": 0}).sort("order", 1).to_list(50)
    return {"banners": banners}


@cms_router.post("/banners")
async def create_banner(
    banner: BannerCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a new banner"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    banner_doc = {
        "id": str(uuid.uuid4()),
        "type": "banner",
        **banner.dict(),
        "created_by": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.cms_content.insert_one(banner_doc)
    
    return {"message": "Banner created", "banner": {k: v for k, v in banner_doc.items() if k != "_id"}}


@cms_router.put("/banners/{banner_id}")
async def update_banner(
    banner_id: str,
    banner: BannerCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Update a banner"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    result = await db.cms_content.update_one(
        {"id": banner_id, "type": "banner"},
        {"$set": {**banner.dict(), "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Banner not found")
    
    return {"message": "Banner updated"}


@cms_router.delete("/banners/{banner_id}")
async def delete_banner(
    banner_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete a banner"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    result = await db.cms_content.delete_one({"id": banner_id, "type": "banner"})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Banner not found")
    
    return {"message": "Banner deleted"}


# ================== MENU ==================

@cms_router.get("/menu")
async def get_menu(
    location: str = "header",
    current_user: dict = Depends(get_erp_user)
):
    """Get menu items"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    menu_items = await db.cms_content.find(
        {"type": "menu_item", "menu_location": location},
        {"_id": 0}
    ).sort("order", 1).to_list(50)
    
    return {"menu_items": menu_items}


@cms_router.post("/menu")
async def create_menu_item(
    item: MenuItemCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a menu item"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    menu_doc = {
        "id": str(uuid.uuid4()),
        "type": "menu_item",
        **item.dict(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.cms_content.insert_one(menu_doc)
    
    return {"message": "Menu item created", "item": {k: v for k, v in menu_doc.items() if k != "_id"}}


@cms_router.put("/menu/{item_id}")
async def update_menu_item(
    item_id: str,
    item: MenuItemCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Update a menu item"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    result = await db.cms_content.update_one(
        {"id": item_id, "type": "menu_item"},
        {"$set": item.dict()}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    
    return {"message": "Menu item updated"}


@cms_router.delete("/menu/{item_id}")
async def delete_menu_item(
    item_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete a menu item"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    result = await db.cms_content.delete_one({"id": item_id, "type": "menu_item"})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Menu item not found")
    
    return {"message": "Menu item deleted"}


# ================== BLOG ==================

@cms_router.get("/blog")
async def get_blog_posts(
    status: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 20,
    current_user: dict = Depends(get_erp_user)
):
    """Get blog posts"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    query = {"type": "blog_post"}
    if status:
        query["status"] = status
    if category:
        query["category"] = category
    
    posts = await db.cms_content.find(query, {"_id": 0}).sort("created_at", -1).to_list(limit)
    return {"posts": posts}


@cms_router.post("/blog")
async def create_blog_post(
    post: BlogPostCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Create a blog post"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    # Check slug uniqueness
    existing = await db.cms_content.find_one({"type": "blog_post", "slug": post.slug})
    if existing:
        raise HTTPException(status_code=400, detail="Slug already exists")
    
    # Auto-generate meta if not provided
    if not post.meta_title:
        post.meta_title = post.title
    if not post.meta_description:
        post.meta_description = post.excerpt or post.content[:160]
    
    post_doc = {
        "id": str(uuid.uuid4()),
        "type": "blog_post",
        **post.dict(),
        "author": current_user.get("name"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "views": 0
    }
    
    await db.cms_content.insert_one(post_doc)
    
    return {"message": "Blog post created", "post": {k: v for k, v in post_doc.items() if k != "_id"}}


@cms_router.put("/blog/{post_id}")
async def update_blog_post(
    post_id: str,
    post: BlogPostCreate,
    current_user: dict = Depends(get_erp_user)
):
    """Update a blog post"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    result = await db.cms_content.update_one(
        {"id": post_id, "type": "blog_post"},
        {"$set": {
            **post.dict(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Blog post not found")
    
    return {"message": "Blog post updated"}


@cms_router.delete("/blog/{post_id}")
async def delete_blog_post(
    post_id: str,
    current_user: dict = Depends(get_erp_user)
):
    """Delete a blog post"""
    if current_user.get("role") not in ["super_admin", "admin"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    result = await db.cms_content.delete_one({"id": post_id, "type": "blog_post"})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Blog post not found")
    
    return {"message": "Blog post deleted"}


# ================== CONTACT INFO ==================

@cms_router.get("/contact-info")
async def get_contact_info():
    """Get contact information (public endpoint)"""
    db = get_db()
    info = await db.cms_content.find_one({"type": "contact_info"}, {"_id": 0})
    
    if not info:
        # Default contact info
        return {
            "phone": "9284701985",
            "email_booking": "book@lucumaaglass.in",
            "email_info": "info@lucumaaglass.in",
            "email_sales": "sales@lucumaaglass.in",
            "address": "Lucumaa Glass Manufacturing",
            "city": "Pune",
            "state": "Maharashtra",
            "pincode": "",
            "google_maps_url": "",
            "whatsapp": "919284701985",
            "working_hours": "Monday - Saturday, 9:00 AM - 7:00 PM"
        }
    
    return info


@cms_router.put("/contact-info")
async def update_contact_info(
    info: ContactInfoUpdate,
    current_user: dict = Depends(get_erp_user)
):
    """Update contact information"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    
    update_data = {k: v for k, v in info.dict().items() if v is not None}
    update_data["type"] = "contact_info"
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.cms_content.update_one(
        {"type": "contact_info"},
        {"$set": update_data},
        upsert=True
    )
    
    return {"message": "Contact info updated"}


# ================== SEO & SITEMAP ==================

@cms_router.get("/sitemap")
async def generate_sitemap():
    """Generate sitemap.xml content"""
    db = get_db()
    
    base_url = "https://lucumaaglass.in"
    
    # Static pages
    static_pages = [
        {"url": "/", "priority": "1.0", "changefreq": "weekly"},
        {"url": "/products", "priority": "0.9", "changefreq": "weekly"},
        {"url": "/industries", "priority": "0.8", "changefreq": "monthly"},
        {"url": "/how-it-works", "priority": "0.8", "changefreq": "monthly"},
        {"url": "/pricing", "priority": "0.8", "changefreq": "weekly"},
        {"url": "/contact", "priority": "0.7", "changefreq": "monthly"},
        {"url": "/customize", "priority": "0.9", "changefreq": "weekly"}
    ]
    
    # Dynamic pages
    pages = await db.cms_content.find(
        {"type": "page", "status": "published"},
        {"_id": 0, "slug": 1, "updated_at": 1}
    ).to_list(100)
    
    # Blog posts
    posts = await db.cms_content.find(
        {"type": "blog_post", "status": "published"},
        {"_id": 0, "slug": 1, "updated_at": 1}
    ).to_list(500)
    
    # Products
    products = await db.products.find({}, {"_id": 0, "id": 1}).to_list(100)
    
    sitemap_entries = []
    
    # Add static pages
    for page in static_pages:
        sitemap_entries.append({
            "loc": f"{base_url}{page['url']}",
            "priority": page["priority"],
            "changefreq": page["changefreq"]
        })
    
    # Add dynamic pages
    for page in pages:
        sitemap_entries.append({
            "loc": f"{base_url}/{page['slug']}",
            "lastmod": page.get("updated_at", "")[:10],
            "priority": "0.7",
            "changefreq": "monthly"
        })
    
    # Add blog posts
    for post in posts:
        sitemap_entries.append({
            "loc": f"{base_url}/blog/{post['slug']}",
            "lastmod": post.get("updated_at", "")[:10],
            "priority": "0.6",
            "changefreq": "monthly"
        })
    
    # Add products
    for product in products:
        sitemap_entries.append({
            "loc": f"{base_url}/products/{product['id']}",
            "priority": "0.8",
            "changefreq": "weekly"
        })
    
    return {"entries": sitemap_entries, "count": len(sitemap_entries)}


@cms_router.get("/blog/categories")
async def get_blog_categories(current_user: dict = Depends(get_erp_user)):
    """Get unique blog categories"""
    if current_user.get("role") not in ["super_admin", "admin", "owner"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    db = get_db()
    categories = await db.cms_content.distinct("category", {"type": "blog_post"})
    return {"categories": [c for c in categories if c]}


# ================== PUBLIC BLOG ENDPOINTS ==================

@cms_router.get("/public/blog")
async def get_public_blog_posts(
    category: Optional[str] = None,
    limit: int = 20,
    page: int = 1
):
    """Get published blog posts (public endpoint)"""
    db = get_db()
    query = {"type": "blog_post", "status": "published"}
    if category:
        query["category"] = category
    
    skip = (page - 1) * limit
    posts = await db.cms_content.find(query, {"_id": 0}).sort("created_at", -1).skip(skip).to_list(limit)
    total = await db.cms_content.count_documents(query)
    
    # Get unique categories for filter
    categories = await db.cms_content.distinct("category", {"type": "blog_post", "status": "published"})
    
    return {
        "posts": posts,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit,
        "categories": [c for c in categories if c]
    }


@cms_router.get("/public/blog/{slug}")
async def get_public_blog_post(slug: str):
    """Get a single published blog post by slug (public endpoint)"""
    db = get_db()
    post = await db.cms_content.find_one(
        {"type": "blog_post", "slug": slug, "status": "published"},
        {"_id": 0}
    )
    
    if not post:
        raise HTTPException(status_code=404, detail="Blog post not found")
    
    # Increment view count
    await db.cms_content.update_one(
        {"type": "blog_post", "slug": slug},
        {"$inc": {"views": 1}}
    )
    
    # Get related posts (same category)
    related = []
    if post.get("category"):
        related = await db.cms_content.find(
            {
                "type": "blog_post",
                "status": "published",
                "category": post["category"],
                "slug": {"$ne": slug}
            },
            {"_id": 0, "title": 1, "slug": 1, "featured_image": 1, "excerpt": 1, "created_at": 1}
        ).sort("created_at", -1).to_list(3)
    
    return {"post": post, "related": related}


@cms_router.get("/public/categories")
async def get_public_categories():
    """Get all blog categories with post counts (public endpoint)"""
    db = get_db()
    pipeline = [
        {"$match": {"type": "blog_post", "status": "published", "category": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    categories = await db.cms_content.aggregate(pipeline).to_list(50)
    return {"categories": [{"name": c["_id"], "count": c["count"]} for c in categories]}

