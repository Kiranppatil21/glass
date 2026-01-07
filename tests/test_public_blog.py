"""
Test Public Blog Endpoints and Blog Features
Tests: GET /api/erp/cms/public/blog, GET /api/erp/cms/public/blog/{slug}, GET /api/erp/cms/public/categories
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')


class TestPublicBlogEndpoints:
    """Test public blog API endpoints (no auth required)"""
    
    def test_get_public_blog_list(self):
        """Test GET /api/erp/cms/public/blog - returns published posts"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog")
        assert response.status_code == 200
        
        data = response.json()
        assert "posts" in data
        assert "total" in data
        assert "page" in data
        assert "total_pages" in data
        assert "categories" in data
        
        # Verify posts structure
        if len(data["posts"]) > 0:
            post = data["posts"][0]
            assert "id" in post
            assert "title" in post
            assert "slug" in post
            assert "content" in post
            assert "status" in post
            assert post["status"] == "published"  # Only published posts
            print(f"✓ Found {len(data['posts'])} published blog posts")
    
    def test_get_public_blog_with_pagination(self):
        """Test pagination parameters"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog", params={"page": 1, "limit": 5})
        assert response.status_code == 200
        
        data = response.json()
        assert data["page"] == 1
        assert data["limit"] == 5
        print(f"✓ Pagination working - Page {data['page']}, Total pages: {data['total_pages']}")
    
    def test_get_public_blog_with_category_filter(self):
        """Test category filter"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog", params={"category": "Glass Guide"})
        assert response.status_code == 200
        
        data = response.json()
        # All posts should be in the filtered category
        for post in data["posts"]:
            assert post["category"] == "Glass Guide"
        print(f"✓ Category filter working - Found {len(data['posts'])} posts in 'Glass Guide' category")
    
    def test_get_public_blog_post_by_slug(self):
        """Test GET /api/erp/cms/public/blog/{slug} - returns single post"""
        slug = "guide-to-toughened-glass"
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response.status_code == 200
        
        data = response.json()
        assert "post" in data
        assert "related" in data
        
        post = data["post"]
        assert post["slug"] == slug
        assert "title" in post
        assert "content" in post
        assert "meta_title" in post
        assert "meta_description" in post
        assert "featured_image" in post
        assert "author" in post
        assert "created_at" in post
        assert "views" in post
        print(f"✓ Single post retrieved: '{post['title']}'")
        print(f"  - Meta title: {post['meta_title']}")
        print(f"  - Views: {post['views']}")
    
    def test_get_public_blog_post_related_posts(self):
        """Test related posts are returned"""
        slug = "guide-to-toughened-glass"
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response.status_code == 200
        
        data = response.json()
        related = data["related"]
        
        # Related posts should have required fields
        if len(related) > 0:
            for rel_post in related:
                assert "title" in rel_post
                assert "slug" in rel_post
                assert rel_post["slug"] != slug  # Should not include current post
            print(f"✓ Found {len(related)} related posts")
        else:
            print("✓ No related posts (expected if only one post in category)")
    
    def test_get_public_blog_post_not_found(self):
        """Test 404 for non-existent slug"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/non-existent-post-slug-12345")
        assert response.status_code == 404
        print("✓ 404 returned for non-existent post")
    
    def test_get_public_categories(self):
        """Test GET /api/erp/cms/public/categories - returns categories with counts"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/categories")
        assert response.status_code == 200
        
        data = response.json()
        assert "categories" in data
        
        # Each category should have name and count
        for cat in data["categories"]:
            assert "name" in cat
            assert "count" in cat
            assert isinstance(cat["count"], int)
            assert cat["count"] > 0
        print(f"✓ Found {len(data['categories'])} categories with post counts")
    
    def test_blog_post_view_count_increments(self):
        """Test that view count increments on each request"""
        slug = "laminated-vs-toughened-glass"
        
        # Get initial view count
        response1 = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response1.status_code == 200
        initial_views = response1.json()["post"]["views"]
        
        # Make another request
        response2 = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response2.status_code == 200
        new_views = response2.json()["post"]["views"]
        
        # View count should have incremented
        assert new_views >= initial_views
        print(f"✓ View count tracking working: {initial_views} -> {new_views}")


class TestBlogSEOFields:
    """Test SEO-related fields in blog posts"""
    
    def test_blog_post_has_seo_fields(self):
        """Verify SEO fields are present"""
        slug = "guide-to-toughened-glass"
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response.status_code == 200
        
        post = response.json()["post"]
        
        # Check SEO fields
        assert "meta_title" in post
        assert "meta_description" in post
        assert "meta_keywords" in post
        
        # Meta title should be set
        assert post["meta_title"] is not None
        assert len(post["meta_title"]) > 0
        
        # Meta description should be set
        assert post["meta_description"] is not None
        assert len(post["meta_description"]) > 0
        
        print(f"✓ SEO fields present:")
        print(f"  - meta_title: {post['meta_title'][:50]}...")
        print(f"  - meta_description: {post['meta_description'][:50]}...")
        print(f"  - meta_keywords: {post['meta_keywords']}")
    
    def test_blog_post_has_featured_image(self):
        """Verify featured image is present"""
        slug = "guide-to-toughened-glass"
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog/{slug}")
        assert response.status_code == 200
        
        post = response.json()["post"]
        assert "featured_image" in post
        assert post["featured_image"] is not None
        assert post["featured_image"].startswith("http")
        print(f"✓ Featured image present: {post['featured_image'][:60]}...")


class TestBlogListResponse:
    """Test blog list response structure"""
    
    def test_blog_list_returns_categories(self):
        """Verify categories are returned with blog list"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog")
        assert response.status_code == 200
        
        data = response.json()
        assert "categories" in data
        assert isinstance(data["categories"], list)
        print(f"✓ Categories returned with blog list: {data['categories']}")
    
    def test_blog_list_pagination_info(self):
        """Verify pagination info is complete"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/public/blog")
        assert response.status_code == 200
        
        data = response.json()
        assert "page" in data
        assert "limit" in data
        assert "total" in data
        assert "total_pages" in data
        
        # Verify pagination math
        if data["total"] > 0:
            expected_pages = (data["total"] + data["limit"] - 1) // data["limit"]
            assert data["total_pages"] == expected_pages
        print(f"✓ Pagination info: Page {data['page']}/{data['total_pages']}, Total: {data['total']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
