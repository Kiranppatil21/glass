"""
Test Suite for Shopping Cart, AI Demand Forecasting, and CMS Features
Tests: Cart functionality, /api/erp/forecast/*, /api/erp/cms/* APIs
"""
import pytest
import requests
import os
import time

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com')

# Test credentials
ADMIN_EMAIL = "admin@lucumaa.in"
ADMIN_PASSWORD = "adminpass"

class TestAuthSetup:
    """Authentication setup for ERP tests"""
    
    @pytest.fixture(scope="class")
    def admin_token(self):
        """Get admin authentication token"""
        response = requests.post(
            f"{BASE_URL}/api/auth/login",
            json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        )
        if response.status_code == 200:
            return response.json().get("token")
        pytest.skip(f"Admin login failed: {response.status_code} - {response.text}")
    
    @pytest.fixture(scope="class")
    def admin_headers(self, admin_token):
        """Get headers with admin auth token"""
        return {
            "Authorization": f"Bearer {admin_token}",
            "Content-Type": "application/json"
        }


class TestProducts(TestAuthSetup):
    """Test Products API for cart functionality"""
    
    def test_get_products(self):
        """GET /api/products - Get all products"""
        response = requests.get(f"{BASE_URL}/api/products")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        products = response.json()
        assert isinstance(products, list), "Products should be a list"
        assert len(products) > 0, "Should have at least one product"
        
        # Verify product structure
        product = products[0]
        assert "id" in product, "Product should have id"
        assert "name" in product, "Product should have name"
        print(f"✅ GET /api/products - Found {len(products)} products")


class TestAIForecast(TestAuthSetup):
    """Test AI Demand Forecasting APIs"""
    
    def test_get_order_stats(self, admin_headers):
        """GET /api/erp/forecast/stats - Get order statistics"""
        response = requests.get(
            f"{BASE_URL}/api/erp/forecast/stats",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "orders" in data, "Response should have orders"
        assert "revenue" in data, "Response should have revenue"
        assert "generated_at" in data, "Response should have generated_at"
        
        # Verify orders structure
        orders = data["orders"]
        assert "today" in orders, "Orders should have today count"
        assert "this_week" in orders, "Orders should have this_week count"
        assert "this_month" in orders, "Orders should have this_month count"
        assert "this_year" in orders, "Orders should have this_year count"
        
        print(f"✅ GET /api/erp/forecast/stats - Orders: today={orders['today']}, week={orders['this_week']}, month={orders['this_month']}")
    
    def test_get_demand_forecast(self, admin_headers):
        """GET /api/erp/forecast/demand - Get AI demand forecast"""
        response = requests.get(
            f"{BASE_URL}/api/erp/forecast/demand",
            headers=admin_headers,
            params={"days": 90}
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "summary" in data, "Response should have summary"
        assert "trend" in data, "Response should have trend"
        assert "data_period" in data, "Response should have data_period"
        assert "generated_at" in data, "Response should have generated_at"
        
        print(f"✅ GET /api/erp/forecast/demand - Trend: {data['trend']}, Summary: {data['summary'][:100]}...")
    
    def test_forecast_unauthorized(self):
        """GET /api/erp/forecast/stats - Should require auth"""
        response = requests.get(f"{BASE_URL}/api/erp/forecast/stats")
        assert response.status_code in [401, 403], f"Expected 401/403, got {response.status_code}"
        print("✅ Forecast API requires authentication")


class TestCMSPages(TestAuthSetup):
    """Test CMS Pages API"""
    
    def test_get_pages(self, admin_headers):
        """GET /api/erp/cms/pages - Get all pages"""
        response = requests.get(
            f"{BASE_URL}/api/erp/cms/pages",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "pages" in data, "Response should have pages"
        print(f"✅ GET /api/erp/cms/pages - Found {len(data['pages'])} pages")
    
    def test_create_page(self, admin_headers):
        """POST /api/erp/cms/pages - Create a new page"""
        page_data = {
            "title": "TEST_Page_Title",
            "slug": f"test-page-{int(time.time())}",
            "content": "This is test page content for automated testing.",
            "meta_title": "Test Page SEO Title",
            "meta_description": "Test page meta description",
            "status": "draft"
        }
        
        response = requests.post(
            f"{BASE_URL}/api/erp/cms/pages",
            headers=admin_headers,
            json=page_data
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "page" in data, "Response should have page"
        assert data["page"]["title"] == page_data["title"], "Title should match"
        assert data["page"]["slug"] == page_data["slug"], "Slug should match"
        
        # Store page ID for cleanup
        TestCMSPages.created_page_id = data["page"]["id"]
        print(f"✅ POST /api/erp/cms/pages - Created page: {data['page']['id']}")
        return data["page"]["id"]
    
    def test_delete_page(self, admin_headers):
        """DELETE /api/erp/cms/pages/{id} - Delete a page"""
        if not hasattr(TestCMSPages, 'created_page_id'):
            pytest.skip("No page created to delete")
        
        response = requests.delete(
            f"{BASE_URL}/api/erp/cms/pages/{TestCMSPages.created_page_id}",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print(f"✅ DELETE /api/erp/cms/pages - Deleted page: {TestCMSPages.created_page_id}")


class TestCMSBanners(TestAuthSetup):
    """Test CMS Banners API"""
    
    def test_get_banners(self, admin_headers):
        """GET /api/erp/cms/banners - Get all banners"""
        response = requests.get(
            f"{BASE_URL}/api/erp/cms/banners",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "banners" in data, "Response should have banners"
        print(f"✅ GET /api/erp/cms/banners - Found {len(data['banners'])} banners")
    
    def test_create_banner(self, admin_headers):
        """POST /api/erp/cms/banners - Create a new banner"""
        banner_data = {
            "title": "TEST_Banner_Title",
            "subtitle": "Test banner subtitle",
            "image_url": "https://example.com/test-banner.jpg",
            "link_url": "/products",
            "button_text": "Shop Now",
            "position": "home_hero",
            "order": 99,
            "active": True
        }
        
        response = requests.post(
            f"{BASE_URL}/api/erp/cms/banners",
            headers=admin_headers,
            json=banner_data
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "banner" in data, "Response should have banner"
        assert data["banner"]["title"] == banner_data["title"], "Title should match"
        
        TestCMSBanners.created_banner_id = data["banner"]["id"]
        print(f"✅ POST /api/erp/cms/banners - Created banner: {data['banner']['id']}")
    
    def test_delete_banner(self, admin_headers):
        """DELETE /api/erp/cms/banners/{id} - Delete a banner"""
        if not hasattr(TestCMSBanners, 'created_banner_id'):
            pytest.skip("No banner created to delete")
        
        response = requests.delete(
            f"{BASE_URL}/api/erp/cms/banners/{TestCMSBanners.created_banner_id}",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print(f"✅ DELETE /api/erp/cms/banners - Deleted banner: {TestCMSBanners.created_banner_id}")


class TestCMSMenu(TestAuthSetup):
    """Test CMS Menu API"""
    
    def test_get_menu(self, admin_headers):
        """GET /api/erp/cms/menu - Get menu items"""
        response = requests.get(
            f"{BASE_URL}/api/erp/cms/menu",
            headers=admin_headers,
            params={"location": "header"}
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "menu_items" in data, "Response should have menu_items"
        print(f"✅ GET /api/erp/cms/menu - Found {len(data['menu_items'])} menu items")
    
    def test_create_menu_item(self, admin_headers):
        """POST /api/erp/cms/menu - Create a menu item"""
        menu_data = {
            "title": "TEST_Menu_Item",
            "url": "/test-page",
            "order": 99,
            "target": "_self",
            "menu_location": "header"
        }
        
        response = requests.post(
            f"{BASE_URL}/api/erp/cms/menu",
            headers=admin_headers,
            json=menu_data
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "item" in data, "Response should have item"
        
        TestCMSMenu.created_menu_id = data["item"]["id"]
        print(f"✅ POST /api/erp/cms/menu - Created menu item: {data['item']['id']}")
    
    def test_delete_menu_item(self, admin_headers):
        """DELETE /api/erp/cms/menu/{id} - Delete a menu item"""
        if not hasattr(TestCMSMenu, 'created_menu_id'):
            pytest.skip("No menu item created to delete")
        
        response = requests.delete(
            f"{BASE_URL}/api/erp/cms/menu/{TestCMSMenu.created_menu_id}",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print(f"✅ DELETE /api/erp/cms/menu - Deleted menu item: {TestCMSMenu.created_menu_id}")


class TestCMSBlog(TestAuthSetup):
    """Test CMS Blog API"""
    
    def test_get_blog_posts(self, admin_headers):
        """GET /api/erp/cms/blog - Get blog posts"""
        response = requests.get(
            f"{BASE_URL}/api/erp/cms/blog",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "posts" in data, "Response should have posts"
        print(f"✅ GET /api/erp/cms/blog - Found {len(data['posts'])} blog posts")
    
    def test_create_blog_post(self, admin_headers):
        """POST /api/erp/cms/blog - Create a blog post"""
        blog_data = {
            "title": "TEST_Blog_Post",
            "slug": f"test-blog-{int(time.time())}",
            "excerpt": "This is a test blog post excerpt.",
            "content": "This is the full content of the test blog post for automated testing.",
            "category": "Testing",
            "tags": ["test", "automation"],
            "status": "draft"
        }
        
        response = requests.post(
            f"{BASE_URL}/api/erp/cms/blog",
            headers=admin_headers,
            json=blog_data
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "post" in data, "Response should have post"
        assert data["post"]["title"] == blog_data["title"], "Title should match"
        
        TestCMSBlog.created_blog_id = data["post"]["id"]
        print(f"✅ POST /api/erp/cms/blog - Created blog post: {data['post']['id']}")
    
    def test_delete_blog_post(self, admin_headers):
        """DELETE /api/erp/cms/blog/{id} - Delete a blog post"""
        if not hasattr(TestCMSBlog, 'created_blog_id'):
            pytest.skip("No blog post created to delete")
        
        response = requests.delete(
            f"{BASE_URL}/api/erp/cms/blog/{TestCMSBlog.created_blog_id}",
            headers=admin_headers
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        print(f"✅ DELETE /api/erp/cms/blog - Deleted blog post: {TestCMSBlog.created_blog_id}")


class TestCMSContactInfo(TestAuthSetup):
    """Test CMS Contact Info API"""
    
    def test_get_contact_info_public(self):
        """GET /api/erp/cms/contact-info - Public endpoint"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/contact-info")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        # Should have contact fields
        assert "phone" in data or "email_booking" in data, "Should have contact info"
        print(f"✅ GET /api/erp/cms/contact-info (public) - Phone: {data.get('phone', 'N/A')}")
    
    def test_update_contact_info(self, admin_headers):
        """PUT /api/erp/cms/contact-info - Update contact info"""
        contact_data = {
            "phone": "9284701985",
            "email_booking": "book@lucumaaglass.in",
            "email_info": "info@lucumaaglass.in",
            "email_sales": "sales@lucumaaglass.in",
            "address": "Lucumaa Glass Manufacturing",
            "city": "Pune",
            "state": "Maharashtra",
            "working_hours": "Monday - Saturday, 9:00 AM - 7:00 PM"
        }
        
        response = requests.put(
            f"{BASE_URL}/api/erp/cms/contact-info",
            headers=admin_headers,
            json=contact_data
        )
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "message" in data, "Response should have message"
        print(f"✅ PUT /api/erp/cms/contact-info - Updated contact info")


class TestCMSSitemap(TestAuthSetup):
    """Test CMS Sitemap API"""
    
    def test_get_sitemap(self):
        """GET /api/erp/cms/sitemap - Get sitemap entries"""
        response = requests.get(f"{BASE_URL}/api/erp/cms/sitemap")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        
        data = response.json()
        assert "entries" in data, "Response should have entries"
        assert "count" in data, "Response should have count"
        print(f"✅ GET /api/erp/cms/sitemap - Found {data['count']} sitemap entries")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
