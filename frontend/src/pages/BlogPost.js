import React, { useState, useEffect } from 'react';
import { Link, useParams, useNavigate } from 'react-router-dom';
import { Helmet } from 'react-helmet-async';
import ReactMarkdown from 'react-markdown';
import { 
  Calendar, User, Eye, Tag, ArrowLeft, Share2, 
  Facebook, Twitter, Linkedin, Copy, Check, Clock
} from 'lucide-react';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';
import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const BlogPost = () => {
  const { slug } = useParams();
  const navigate = useNavigate();
  const [post, setPost] = useState(null);
  const [related, setRelated] = useState([]);
  const [loading, setLoading] = useState(true);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    fetchPost();
    window.scrollTo(0, 0);
  }, [slug]);

  const fetchPost = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_URL}/api/erp/cms/public/blog/${slug}`);
      setPost(response.data.post);
      setRelated(response.data.related || []);
    } catch (error) {
      console.error('Failed to fetch blog post:', error);
      if (error.response?.status === 404) {
        navigate('/blog');
      }
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    return new Date(dateStr).toLocaleDateString('en-IN', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  const calculateReadTime = (content) => {
    if (!content) return '1 min';
    const words = content.split(/\s+/).length;
    const minutes = Math.ceil(words / 200);
    return `${minutes} min read`;
  };

  const shareUrl = typeof window !== 'undefined' ? window.location.href : '';
  const shareTitle = post?.title || '';

  const handleShare = (platform) => {
    const urls = {
      facebook: `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(shareUrl)}`,
      twitter: `https://twitter.com/intent/tweet?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(shareTitle)}`,
      linkedin: `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(shareUrl)}`,
      whatsapp: `https://wa.me/?text=${encodeURIComponent(shareTitle + ' ' + shareUrl)}`
    };
    
    if (urls[platform]) {
      window.open(urls[platform], '_blank', 'width=600,height=400');
    }
  };

  const handleCopyLink = () => {
    navigator.clipboard.writeText(shareUrl);
    setCopied(true);
    toast.success('Link copied to clipboard!');
    setTimeout(() => setCopied(false), 2000);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <div className="text-center">
          <div className="animate-spin w-12 h-12 border-4 border-teal-600 border-t-transparent rounded-full mx-auto mb-4"></div>
          <p className="text-slate-600">Loading article...</p>
        </div>
      </div>
    );
  }

  if (!post) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50">
        <div className="text-center">
          <h1 className="text-2xl font-bold text-slate-900 mb-4">Post Not Found</h1>
          <Link to="/blog">
            <Button className="bg-teal-600 hover:bg-teal-700">Back to Blog</Button>
          </Link>
        </div>
      </div>
    );
  }

  return (
    <>
      <Helmet>
        <title>{post.meta_title || post.title} | Lucumaa Glass Blog</title>
        <meta name="description" content={post.meta_description || post.excerpt || post.content?.slice(0, 160)} />
        <meta name="keywords" content={post.meta_keywords || `${post.category}, glass manufacturing, lucumaa glass`} />
        <link rel="canonical" href={`https://lucumaaglass.in/blog/${post.slug}`} />
        
        {/* Open Graph */}
        <meta property="og:title" content={post.meta_title || post.title} />
        <meta property="og:description" content={post.meta_description || post.excerpt} />
        <meta property="og:type" content="article" />
        <meta property="og:url" content={`https://lucumaaglass.in/blog/${post.slug}`} />
        {post.featured_image && <meta property="og:image" content={post.featured_image} />}
        
        {/* Twitter Card */}
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={post.meta_title || post.title} />
        <meta name="twitter:description" content={post.meta_description || post.excerpt} />
        {post.featured_image && <meta name="twitter:image" content={post.featured_image} />}
        
        {/* Article Schema */}
        <script type="application/ld+json">
          {JSON.stringify({
            "@context": "https://schema.org",
            "@type": "BlogPosting",
            "headline": post.title,
            "description": post.excerpt || post.meta_description,
            "image": post.featured_image,
            "author": {
              "@type": "Person",
              "name": post.author || "Lucumaa Glass"
            },
            "publisher": {
              "@type": "Organization",
              "name": "Lucumaa Glass",
              "logo": {
                "@type": "ImageObject",
                "url": "https://lucumaaglass.in/logo.png"
              }
            },
            "datePublished": post.created_at,
            "dateModified": post.updated_at || post.created_at
          })}
        </script>
      </Helmet>

      <div className="min-h-screen bg-slate-50" data-testid="blog-post-page">
        {/* Hero/Header */}
        <div className="bg-gradient-to-br from-slate-900 to-slate-800 text-white">
          <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12 md:py-20">
            {/* Back Link */}
            <Link to="/blog" className="inline-flex items-center gap-2 text-teal-400 hover:text-teal-300 mb-6 transition-colors">
              <ArrowLeft className="w-4 h-4" />
              Back to Blog
            </Link>
            
            {/* Category */}
            {post.category && (
              <Link to={`/blog?category=${encodeURIComponent(post.category)}`}>
                <span className="inline-flex items-center gap-1 text-sm font-medium text-teal-400 bg-teal-400/10 px-3 py-1 rounded-full mb-4 hover:bg-teal-400/20 transition-colors">
                  <Tag className="w-3 h-3" />
                  {post.category}
                </span>
              </Link>
            )}
            
            {/* Title */}
            <h1 className="text-3xl md:text-4xl lg:text-5xl font-bold leading-tight mb-6">
              {post.title}
            </h1>
            
            {/* Meta Info */}
            <div className="flex flex-wrap items-center gap-4 md:gap-6 text-slate-400 text-sm">
              {post.author && (
                <span className="flex items-center gap-2">
                  <User className="w-4 h-4" />
                  {post.author}
                </span>
              )}
              <span className="flex items-center gap-2">
                <Calendar className="w-4 h-4" />
                {formatDate(post.created_at)}
              </span>
              <span className="flex items-center gap-2">
                <Clock className="w-4 h-4" />
                {calculateReadTime(post.content)}
              </span>
              {post.views > 0 && (
                <span className="flex items-center gap-2">
                  <Eye className="w-4 h-4" />
                  {post.views} views
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Featured Image */}
        {post.featured_image && (
          <div className="max-w-5xl mx-auto px-4 -mt-8 md:-mt-12">
            <img
              src={post.featured_image}
              alt={post.title}
              className="w-full h-64 md:h-96 object-cover rounded-xl shadow-2xl"
            />
          </div>
        )}

        {/* Content */}
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
          <div className="grid lg:grid-cols-[1fr_280px] gap-8">
            {/* Main Content */}
            <article className="prose prose-lg max-w-none prose-headings:text-slate-900 prose-p:text-slate-700 prose-a:text-teal-600">
              {/* Excerpt */}
              {post.excerpt && (
                <p className="text-xl text-slate-600 font-medium leading-relaxed mb-8 border-l-4 border-teal-500 pl-4">
                  {post.excerpt}
                </p>
              )}
              
              {/* Content - Rendered as Markdown */}
              <div className="text-slate-700 leading-relaxed">
                <ReactMarkdown
                  components={{
                    h1: ({node, ...props}) => <h1 className="text-3xl font-bold text-slate-900 mt-8 mb-4" {...props} />,
                    h2: ({node, ...props}) => <h2 className="text-2xl font-bold text-slate-900 mt-6 mb-3" {...props} />,
                    h3: ({node, ...props}) => <h3 className="text-xl font-bold text-slate-900 mt-5 mb-2" {...props} />,
                    p: ({node, ...props}) => <p className="mb-4 text-slate-700 leading-relaxed" {...props} />,
                    ul: ({node, ...props}) => <ul className="list-disc pl-6 mb-4 space-y-2" {...props} />,
                    ol: ({node, ...props}) => <ol className="list-decimal pl-6 mb-4 space-y-2" {...props} />,
                    li: ({node, ...props}) => <li className="text-slate-700" {...props} />,
                    strong: ({node, ...props}) => <strong className="font-semibold text-slate-900" {...props} />,
                    a: ({node, ...props}) => <a className="text-teal-600 hover:underline" {...props} />,
                    blockquote: ({node, ...props}) => <blockquote className="border-l-4 border-teal-500 pl-4 italic text-slate-600 my-4" {...props} />,
                  }}
                >
                  {post.content}
                </ReactMarkdown>
              </div>
            </article>

            {/* Sidebar */}
            <aside className="space-y-6">
              {/* Share Card */}
              <Card className="sticky top-24">
                <CardContent className="p-4 md:p-6">
                  <h3 className="font-bold text-slate-900 mb-4 flex items-center gap-2">
                    <Share2 className="w-4 h-4" />
                    Share this article
                  </h3>
                  <div className="grid grid-cols-4 gap-2">
                    <button
                      onClick={() => handleShare('facebook')}
                      className="p-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                      aria-label="Share on Facebook"
                    >
                      <Facebook className="w-5 h-5 mx-auto" />
                    </button>
                    <button
                      onClick={() => handleShare('twitter')}
                      className="p-3 bg-sky-500 text-white rounded-lg hover:bg-sky-600 transition-colors"
                      aria-label="Share on Twitter"
                    >
                      <Twitter className="w-5 h-5 mx-auto" />
                    </button>
                    <button
                      onClick={() => handleShare('linkedin')}
                      className="p-3 bg-blue-700 text-white rounded-lg hover:bg-blue-800 transition-colors"
                      aria-label="Share on LinkedIn"
                    >
                      <Linkedin className="w-5 h-5 mx-auto" />
                    </button>
                    <button
                      onClick={handleCopyLink}
                      className="p-3 bg-slate-200 text-slate-700 rounded-lg hover:bg-slate-300 transition-colors"
                      aria-label="Copy link"
                    >
                      {copied ? <Check className="w-5 h-5 mx-auto text-green-600" /> : <Copy className="w-5 h-5 mx-auto" />}
                    </button>
                  </div>
                  
                  {/* WhatsApp */}
                  <button
                    onClick={() => handleShare('whatsapp')}
                    className="w-full mt-3 p-3 bg-green-500 text-white rounded-lg hover:bg-green-600 transition-colors flex items-center justify-center gap-2"
                  >
                    Share on WhatsApp
                  </button>
                </CardContent>
              </Card>
            </aside>
          </div>
        </div>

        {/* Related Posts */}
        {related.length > 0 && (
          <div className="bg-white py-12 md:py-16">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
              <h2 className="text-2xl md:text-3xl font-bold text-slate-900 mb-8">Related Articles</h2>
              <div className="grid md:grid-cols-3 gap-6">
                {related.map((relatedPost) => (
                  <Link key={relatedPost.slug} to={`/blog/${relatedPost.slug}`}>
                    <Card className="h-full hover:shadow-lg transition-shadow overflow-hidden group">
                      <div className="aspect-video bg-slate-100">
                        {relatedPost.featured_image ? (
                          <img
                            src={relatedPost.featured_image}
                            alt={relatedPost.title}
                            className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                          />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-teal-100 to-teal-200">
                            <span className="text-3xl font-bold text-teal-600">{relatedPost.title?.charAt(0)}</span>
                          </div>
                        )}
                      </div>
                      <CardContent className="p-4">
                        <h3 className="font-bold text-slate-900 line-clamp-2 group-hover:text-teal-600 transition-colors">
                          {relatedPost.title}
                        </h3>
                        <p className="text-sm text-slate-500 mt-2">{formatDate(relatedPost.created_at)}</p>
                      </CardContent>
                    </Card>
                  </Link>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* CTA */}
        <div className="bg-teal-600 text-white py-12">
          <div className="max-w-4xl mx-auto px-4 text-center">
            <h2 className="text-2xl md:text-3xl font-bold mb-4">Looking for Premium Glass Solutions?</h2>
            <p className="text-teal-100 mb-6">
              Get in touch with our experts for customized glass manufacturing
            </p>
            <div className="flex flex-wrap justify-center gap-4">
              <Link to="/contact">
                <Button className="bg-white text-teal-600 hover:bg-teal-50 px-6">
                  Contact Us
                </Button>
              </Link>
              <Link to="/products">
                <Button variant="outline" className="border-white text-white hover:bg-white/10 px-6">
                  View Products
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default BlogPost;
