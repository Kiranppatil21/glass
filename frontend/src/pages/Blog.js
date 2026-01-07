import React, { useState, useEffect } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { Helmet } from 'react-helmet-async';
import { Calendar, User, Eye, Tag, ArrowRight, Search, Filter } from 'lucide-react';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const Blog = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [posts, setPosts] = useState([]);
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [pagination, setPagination] = useState({ page: 1, total_pages: 1 });
  
  const selectedCategory = searchParams.get('category') || '';
  const currentPage = parseInt(searchParams.get('page') || '1');

  useEffect(() => {
    fetchPosts();
  }, [selectedCategory, currentPage]);

  const fetchPosts = async () => {
    try {
      setLoading(true);
      const params = { page: currentPage, limit: 9 };
      if (selectedCategory) params.category = selectedCategory;
      
      const response = await axios.get(`${API_URL}/api/erp/cms/public/blog`, { params });
      setPosts(response.data.posts || []);
      setCategories(response.data.categories || []);
      setPagination({
        page: response.data.page,
        total_pages: response.data.total_pages,
        total: response.data.total
      });
    } catch (error) {
      console.error('Failed to fetch blog posts:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCategoryChange = (category) => {
    if (category) {
      setSearchParams({ category, page: '1' });
    } else {
      setSearchParams({});
    }
  };

  const handlePageChange = (page) => {
    const params = { page: page.toString() };
    if (selectedCategory) params.category = selectedCategory;
    setSearchParams(params);
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    return new Date(dateStr).toLocaleDateString('en-IN', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  return (
    <>
      <Helmet>
        <title>Blog | Lucumaa Glass - Glass Manufacturing Insights</title>
        <meta name="description" content="Read the latest articles about glass manufacturing, industry trends, installation guides, and more from Lucumaa Glass experts." />
        <meta name="keywords" content="glass manufacturing blog, toughened glass, laminated glass, glass industry news" />
        <link rel="canonical" href="https://lucumaaglass.in/blog" />
      </Helmet>

      <div className="min-h-screen bg-slate-50" data-testid="blog-page">
        {/* Hero Section */}
        <div className="bg-gradient-to-br from-teal-600 to-teal-800 text-white py-16 md:py-24">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h1 className="text-3xl md:text-5xl font-bold mb-4">Our Blog</h1>
            <p className="text-lg md:text-xl text-teal-100 max-w-2xl mx-auto">
              Insights, guides, and updates from the world of glass manufacturing
            </p>
          </div>
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          {/* Category Filter */}
          {categories.length > 0 && (
            <div className="mb-8 flex flex-wrap items-center gap-2">
              <Filter className="w-5 h-5 text-slate-500" />
              <Button
                variant={!selectedCategory ? 'default' : 'outline'}
                size="sm"
                onClick={() => handleCategoryChange('')}
                className={!selectedCategory ? 'bg-teal-600 hover:bg-teal-700' : ''}
              >
                All
              </Button>
              {categories.map((cat) => (
                <Button
                  key={cat}
                  variant={selectedCategory === cat ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => handleCategoryChange(cat)}
                  className={selectedCategory === cat ? 'bg-teal-600 hover:bg-teal-700' : ''}
                >
                  {cat}
                </Button>
              ))}
            </div>
          )}

          {/* Loading State */}
          {loading ? (
            <div className="text-center py-16">
              <div className="animate-spin w-12 h-12 border-4 border-teal-600 border-t-transparent rounded-full mx-auto mb-4"></div>
              <p className="text-slate-600">Loading posts...</p>
            </div>
          ) : posts.length === 0 ? (
            /* Empty State */
            <div className="text-center py-16">
              <div className="w-24 h-24 bg-slate-100 rounded-full flex items-center justify-center mx-auto mb-6">
                <Search className="w-12 h-12 text-slate-400" />
              </div>
              <h2 className="text-2xl font-bold text-slate-900 mb-2">No Posts Found</h2>
              <p className="text-slate-600 mb-6">
                {selectedCategory 
                  ? `No posts found in "${selectedCategory}" category.`
                  : 'No blog posts have been published yet.'}
              </p>
              {selectedCategory && (
                <Button onClick={() => handleCategoryChange('')} variant="outline">
                  View All Posts
                </Button>
              )}
            </div>
          ) : (
            /* Blog Posts Grid */
            <>
              <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8">
                {posts.map((post) => (
                  <Link key={post.id} to={`/blog/${post.slug}`}>
                    <Card className="h-full hover:shadow-xl transition-all duration-300 hover:-translate-y-1 overflow-hidden group">
                      {/* Featured Image */}
                      <div className="aspect-video bg-slate-200 overflow-hidden">
                        {post.featured_image ? (
                          <img
                            src={post.featured_image}
                            alt={post.title}
                            className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
                          />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-teal-100 to-teal-200">
                            <span className="text-4xl font-bold text-teal-600">{post.title?.charAt(0)}</span>
                          </div>
                        )}
                      </div>
                      
                      <CardContent className="p-5 md:p-6">
                        {/* Category */}
                        {post.category && (
                          <span className="inline-flex items-center gap-1 text-xs font-medium text-teal-600 bg-teal-50 px-2 py-1 rounded-full mb-3">
                            <Tag className="w-3 h-3" />
                            {post.category}
                          </span>
                        )}
                        
                        {/* Title */}
                        <h2 className="text-lg md:text-xl font-bold text-slate-900 mb-2 line-clamp-2 group-hover:text-teal-600 transition-colors">
                          {post.title}
                        </h2>
                        
                        {/* Excerpt */}
                        <p className="text-slate-600 text-sm mb-4 line-clamp-2">
                          {post.excerpt || post.content?.slice(0, 120)}...
                        </p>
                        
                        {/* Meta */}
                        <div className="flex items-center gap-4 text-xs text-slate-500">
                          <span className="flex items-center gap-1">
                            <Calendar className="w-3 h-3" />
                            {formatDate(post.created_at)}
                          </span>
                          {post.author && (
                            <span className="flex items-center gap-1">
                              <User className="w-3 h-3" />
                              {post.author}
                            </span>
                          )}
                          {post.views > 0 && (
                            <span className="flex items-center gap-1">
                              <Eye className="w-3 h-3" />
                              {post.views}
                            </span>
                          )}
                        </div>
                        
                        {/* Read More */}
                        <div className="mt-4 pt-4 border-t">
                          <span className="text-teal-600 font-medium text-sm flex items-center gap-1 group-hover:gap-2 transition-all">
                            Read More <ArrowRight className="w-4 h-4" />
                          </span>
                        </div>
                      </CardContent>
                    </Card>
                  </Link>
                ))}
              </div>

              {/* Pagination */}
              {pagination.total_pages > 1 && (
                <div className="mt-12 flex justify-center items-center gap-2">
                  <Button
                    variant="outline"
                    onClick={() => handlePageChange(currentPage - 1)}
                    disabled={currentPage <= 1}
                  >
                    Previous
                  </Button>
                  
                  <div className="flex items-center gap-1">
                    {Array.from({ length: pagination.total_pages }, (_, i) => i + 1).map((page) => (
                      <Button
                        key={page}
                        variant={page === currentPage ? 'default' : 'outline'}
                        size="sm"
                        onClick={() => handlePageChange(page)}
                        className={page === currentPage ? 'bg-teal-600' : ''}
                      >
                        {page}
                      </Button>
                    ))}
                  </div>
                  
                  <Button
                    variant="outline"
                    onClick={() => handlePageChange(currentPage + 1)}
                    disabled={currentPage >= pagination.total_pages}
                  >
                    Next
                  </Button>
                </div>
              )}
            </>
          )}
        </div>

        {/* CTA Section */}
        <div className="bg-teal-600 text-white py-12 md:py-16">
          <div className="max-w-4xl mx-auto px-4 text-center">
            <h2 className="text-2xl md:text-3xl font-bold mb-4">Need Custom Glass Solutions?</h2>
            <p className="text-teal-100 mb-6">
              Contact our experts for personalized recommendations and quotes
            </p>
            <Link to="/contact">
              <Button className="bg-white text-teal-600 hover:bg-teal-50 px-8 py-3">
                Get in Touch
              </Button>
            </Link>
          </div>
        </div>
      </div>
    </>
  );
};

export default Blog;
