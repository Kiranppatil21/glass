"""
Sitemap and RSS Feed Generator for SEO
"""
from fastapi import APIRouter
from fastapi.responses import Response
from datetime import datetime, timezone
from routers.base import get_db

sitemap_router = APIRouter(tags=["SEO"])

@sitemap_router.get("/sitemap.xml")
async def generate_sitemap():
    """Generate XML sitemap for SEO"""
    db = get_db()
    
    # Base URLs
    base_url = "https://lucumaaglass.in"
    urls = [
        {"loc": f"{base_url}/", "priority": "1.0", "changefreq": "daily"},
        {"loc": f"{base_url}/products", "priority": "0.9", "changefreq": "weekly"},
        {"loc": f"{base_url}/customize", "priority": "0.9", "changefreq": "weekly"},
        {"loc": f"{base_url}/industries", "priority": "0.8", "changefreq": "monthly"},
        {"loc": f"{base_url}/how-it-works", "priority": "0.8", "changefreq": "monthly"},
        {"loc": f"{base_url}/pricing", "priority": "0.8", "changefreq": "weekly"},
        {"loc": f"{base_url}/blog", "priority": "0.8", "changefreq": "daily"},
        {"loc": f"{base_url}/contact", "priority": "0.7", "changefreq": "monthly"},
    ]
    
    # Add blog posts
    blog_posts = await db.cms_content.find(
        {"type": "blog_post", "status": "published"},
        {"slug": 1, "updated_at": 1, "created_at": 1, "_id": 0}
    ).to_list(100)
    
    for post in blog_posts:
        urls.append({
            "loc": f"{base_url}/blog/{post['slug']}",
            "priority": "0.7",
            "changefreq": "weekly",
            "lastmod": post.get('updated_at', post.get('created_at', ''))[:10] if post.get('updated_at') or post.get('created_at') else None
        })
    
    # Add CMS pages
    pages = await db.cms_content.find(
        {"type": "page", "status": "published"},
        {"slug": 1, "updated_at": 1, "_id": 0}
    ).to_list(50)
    
    for page in pages:
        urls.append({
            "loc": f"{base_url}/{page['slug']}",
            "priority": "0.6",
            "changefreq": "monthly"
        })
    
    # Generate XML
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    
    for url in urls:
        xml += '  <url>\n'
        xml += f'    <loc>{url["loc"]}</loc>\n'
        if url.get("lastmod"):
            xml += f'    <lastmod>{url["lastmod"]}</lastmod>\n'
        xml += f'    <changefreq>{url["changefreq"]}</changefreq>\n'
        xml += f'    <priority>{url["priority"]}</priority>\n'
        xml += '  </url>\n'
    
    xml += '</urlset>'
    
    return Response(content=xml, media_type="application/xml")


@sitemap_router.get("/robots.txt")
async def robots_txt():
    """Generate robots.txt"""
    content = """User-agent: *
Allow: /
Disallow: /erp/
Disallow: /portal/
Disallow: /login
Disallow: /api/

Sitemap: https://lucumaaglass.in/sitemap.xml
"""
    return Response(content=content, media_type="text/plain")


@sitemap_router.get("/rss.xml")
async def generate_rss():
    """Generate RSS feed for blog"""
    db = get_db()
    base_url = "https://lucumaaglass.in"
    
    posts = await db.cms_content.find(
        {"type": "blog_post", "status": "published"},
        {"_id": 0}
    ).sort("created_at", -1).to_list(20)
    
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
    xml += '  <channel>\n'
    xml += '    <title>Lucumaa Glass Blog</title>\n'
    xml += f'    <link>{base_url}/blog</link>\n'
    xml += '    <description>Latest articles about glass manufacturing, industry trends, and guides from Lucumaa Glass</description>\n'
    xml += '    <language>en-in</language>\n'
    xml += f'    <lastBuildDate>{datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>\n'
    xml += f'    <atom:link href="{base_url}/rss.xml" rel="self" type="application/rss+xml"/>\n'
    
    for post in posts:
        xml += '    <item>\n'
        xml += f'      <title>{post.get("title", "")}</title>\n'
        xml += f'      <link>{base_url}/blog/{post.get("slug", "")}</link>\n'
        xml += f'      <guid>{base_url}/blog/{post.get("slug", "")}</guid>\n'
        xml += f'      <description><![CDATA[{post.get("excerpt", post.get("content", "")[:200])}]]></description>\n'
        if post.get("created_at"):
            try:
                dt = datetime.fromisoformat(post["created_at"].replace("Z", "+00:00"))
                xml += f'      <pubDate>{dt.strftime("%a, %d %b %Y %H:%M:%S +0000")}</pubDate>\n'
            except:
                pass
        if post.get("category"):
            xml += f'      <category>{post["category"]}</category>\n'
        xml += '    </item>\n'
    
    xml += '  </channel>\n'
    xml += '</rss>'
    
    return Response(content=xml, media_type="application/rss+xml")
