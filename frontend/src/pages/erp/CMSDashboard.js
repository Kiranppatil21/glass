import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { 
  FileText, Image, Menu, BookOpen, Phone, Globe, Plus, 
  Edit, Trash2, Eye, Save, X, Search, ExternalLink
} from 'lucide-react';
import erpApi from '../../utils/erpApi';
import { toast } from 'sonner';

const CMSDashboard = () => {
  const [activeTab, setActiveTab] = useState('pages');
  const [loading, setLoading] = useState(true);
  const [pages, setPages] = useState([]);
  const [banners, setBanners] = useState([]);
  const [menuItems, setMenuItems] = useState([]);
  const [blogPosts, setBlogPosts] = useState([]);
  const [contactInfo, setContactInfo] = useState({});
  const [showModal, setShowModal] = useState(false);
  const [editItem, setEditItem] = useState(null);
  const [formData, setFormData] = useState({});

  useEffect(() => {
    fetchData();
  }, [activeTab]);

  const fetchData = async () => {
    try {
      setLoading(true);
      if (activeTab === 'pages') {
        const res = await erpApi.cms.getPages();
        setPages(res.data.pages || []);
      } else if (activeTab === 'banners') {
        const res = await erpApi.cms.getBanners();
        setBanners(res.data.banners || []);
      } else if (activeTab === 'menu') {
        const res = await erpApi.cms.getMenu('header');
        setMenuItems(res.data.menu_items || []);
      } else if (activeTab === 'blog') {
        const res = await erpApi.cms.getBlogPosts();
        setBlogPosts(res.data.posts || []);
      } else if (activeTab === 'contact') {
        const res = await erpApi.cms.getContactInfo();
        setContactInfo(res.data);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
      toast.error('Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { id: 'pages', label: 'Pages', icon: FileText },
    { id: 'banners', label: 'Banners', icon: Image },
    { id: 'menu', label: 'Menu', icon: Menu },
    { id: 'blog', label: 'Blog', icon: BookOpen },
    { id: 'contact', label: 'Contact Info', icon: Phone },
  ];

  const handleCreate = () => {
    setEditItem(null);
    if (activeTab === 'pages') {
      setFormData({ title: '', slug: '', content: '', meta_title: '', meta_description: '', status: 'draft' });
    } else if (activeTab === 'banners') {
      setFormData({ title: '', subtitle: '', image_url: '', link_url: '', button_text: '', position: 'home_hero', active: true });
    } else if (activeTab === 'menu') {
      setFormData({ title: '', url: '', order: 0, target: '_self', menu_location: 'header' });
    } else if (activeTab === 'blog') {
      setFormData({ title: '', slug: '', excerpt: '', content: '', featured_image: '', category: '', status: 'draft', meta_title: '', meta_description: '' });
    }
    setShowModal(true);
  };

  const handleEdit = (item) => {
    setEditItem(item);
    setFormData({ ...item });
    setShowModal(true);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('Are you sure you want to delete this item?')) return;
    try {
      if (activeTab === 'pages') await erpApi.cms.deletePage(id);
      else if (activeTab === 'banners') await erpApi.cms.deleteBanner(id);
      else if (activeTab === 'menu') await erpApi.cms.deleteMenuItem(id);
      else if (activeTab === 'blog') await erpApi.cms.deleteBlogPost(id);
      toast.success('Deleted successfully');
      fetchData();
    } catch (error) {
      toast.error('Failed to delete');
    }
  };

  const handleSave = async () => {
    try {
      if (activeTab === 'pages') {
        if (editItem) await erpApi.cms.updatePage(editItem.id, formData);
        else await erpApi.cms.createPage(formData);
      } else if (activeTab === 'banners') {
        if (editItem) await erpApi.cms.updateBanner(editItem.id, formData);
        else await erpApi.cms.createBanner(formData);
      } else if (activeTab === 'menu') {
        if (editItem) await erpApi.cms.updateMenuItem(editItem.id, formData);
        else await erpApi.cms.createMenuItem(formData);
      } else if (activeTab === 'blog') {
        if (editItem) await erpApi.cms.updateBlogPost(editItem.id, formData);
        else await erpApi.cms.createBlogPost(formData);
      }
      toast.success(editItem ? 'Updated successfully' : 'Created successfully');
      setShowModal(false);
      fetchData();
    } catch (error) {
      toast.error('Failed to save');
    }
  };

  const handleContactSave = async () => {
    try {
      await erpApi.cms.updateContactInfo(contactInfo);
      toast.success('Contact info updated');
    } catch (error) {
      toast.error('Failed to update contact info');
    }
  };

  const generateSlug = (title) => {
    return title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
  };

  return (
    <div className="p-6 space-y-6" data-testid="cms-dashboard">
      {/* Header */}
      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 flex items-center gap-3">
            <Globe className="w-8 h-8 text-teal-600" />
            Content Management System
          </h1>
          <p className="text-slate-600 mt-1">Manage website content, pages, banners, and blog posts</p>
        </div>
        {activeTab !== 'contact' && (
          <Button onClick={handleCreate} className="bg-teal-600 hover:bg-teal-700" data-testid="add-content-btn">
            <Plus className="w-4 h-4 mr-2" /> Add {activeTab.slice(0, -1)}
          </Button>
        )}
      </div>

      {/* Tabs */}
      <div className="flex gap-2 border-b overflow-x-auto pb-2">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-t-lg whitespace-nowrap transition-colors ${
              activeTab === tab.id 
                ? 'bg-teal-600 text-white' 
                : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
            }`}
            data-testid={`tab-${tab.id}`}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      {loading ? (
        <div className="text-center py-12 text-slate-500">Loading...</div>
      ) : (
        <>
          {/* Pages Tab */}
          {activeTab === 'pages' && (
            <div className="grid gap-4">
              {pages.length === 0 ? (
                <Card className="p-8 text-center">
                  <FileText className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-500">No pages created yet</p>
                </Card>
              ) : (
                pages.map((page) => (
                  <Card key={page.id} className="hover:shadow-md transition-shadow">
                    <CardContent className="p-4 flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <FileText className="w-8 h-8 text-teal-600" />
                        <div>
                          <h3 className="font-semibold text-slate-900">{page.title}</h3>
                          <p className="text-sm text-slate-500">/{page.slug}</p>
                        </div>
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          page.status === 'published' ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-600'
                        }`}>
                          {page.status}
                        </span>
                      </div>
                      <div className="flex gap-2">
                        <Button variant="outline" size="sm" onClick={() => handleEdit(page)}>
                          <Edit className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" className="text-red-600" onClick={() => handleDelete(page.id)}>
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))
              )}
            </div>
          )}

          {/* Banners Tab */}
          {activeTab === 'banners' && (
            <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
              {banners.length === 0 ? (
                <Card className="col-span-full p-8 text-center">
                  <Image className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-500">No banners created yet</p>
                </Card>
              ) : (
                banners.map((banner) => (
                  <Card key={banner.id} className="overflow-hidden">
                    <div className="h-32 bg-slate-100">
                      {banner.image_url && (
                        <img src={banner.image_url} alt={banner.title} className="w-full h-full object-cover" />
                      )}
                    </div>
                    <CardContent className="p-4">
                      <h3 className="font-semibold text-slate-900">{banner.title}</h3>
                      <p className="text-sm text-slate-500">{banner.position}</p>
                      <div className="flex gap-2 mt-3">
                        <Button variant="outline" size="sm" onClick={() => handleEdit(banner)}>
                          <Edit className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" className="text-red-600" onClick={() => handleDelete(banner.id)}>
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))
              )}
            </div>
          )}

          {/* Menu Tab */}
          {activeTab === 'menu' && (
            <div className="space-y-4">
              {menuItems.length === 0 ? (
                <Card className="p-8 text-center">
                  <Menu className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-500">No menu items created yet</p>
                </Card>
              ) : (
                menuItems.map((item) => (
                  <Card key={item.id} className="hover:shadow-md transition-shadow">
                    <CardContent className="p-4 flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <span className="w-8 h-8 rounded bg-slate-100 flex items-center justify-center text-sm font-bold">
                          {item.order}
                        </span>
                        <div>
                          <h3 className="font-semibold text-slate-900">{item.title}</h3>
                          <p className="text-sm text-slate-500 flex items-center gap-1">
                            {item.url}
                            {item.target === '_blank' && <ExternalLink className="w-3 h-3" />}
                          </p>
                        </div>
                      </div>
                      <div className="flex gap-2">
                        <Button variant="outline" size="sm" onClick={() => handleEdit(item)}>
                          <Edit className="w-4 h-4" />
                        </Button>
                        <Button variant="outline" size="sm" className="text-red-600" onClick={() => handleDelete(item.id)}>
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))
              )}
            </div>
          )}

          {/* Blog Tab */}
          {activeTab === 'blog' && (
            <div className="grid md:grid-cols-2 gap-4">
              {blogPosts.length === 0 ? (
                <Card className="col-span-full p-8 text-center">
                  <BookOpen className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                  <p className="text-slate-500">No blog posts created yet</p>
                </Card>
              ) : (
                blogPosts.map((post) => (
                  <Card key={post.id} className="hover:shadow-md transition-shadow">
                    <CardContent className="p-4">
                      <div className="flex justify-between items-start mb-2">
                        <h3 className="font-semibold text-slate-900">{post.title}</h3>
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          post.status === 'published' ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-600'
                        }`}>
                          {post.status}
                        </span>
                      </div>
                      <p className="text-sm text-slate-500 mb-2">/{post.slug}</p>
                      {post.category && (
                        <span className="text-xs bg-blue-100 text-blue-700 px-2 py-1 rounded">{post.category}</span>
                      )}
                      <p className="text-sm text-slate-600 mt-2 line-clamp-2">{post.excerpt}</p>
                      <div className="flex gap-2 mt-3">
                        <Button variant="outline" size="sm" onClick={() => handleEdit(post)}>
                          <Edit className="w-4 h-4 mr-1" /> Edit
                        </Button>
                        <Button variant="outline" size="sm" className="text-red-600" onClick={() => handleDelete(post.id)}>
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))
              )}
            </div>
          )}

          {/* Contact Info Tab */}
          {activeTab === 'contact' && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Phone className="w-5 h-5 text-teal-600" />
                  Business Contact Information
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid md:grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Phone</label>
                    <input
                      type="text"
                      value={contactInfo.phone || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, phone: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">WhatsApp</label>
                    <input
                      type="text"
                      value={contactInfo.whatsapp || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, whatsapp: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Booking Email</label>
                    <input
                      type="email"
                      value={contactInfo.email_booking || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, email_booking: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Info Email</label>
                    <input
                      type="email"
                      value={contactInfo.email_info || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, email_info: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Sales Email</label>
                    <input
                      type="email"
                      value={contactInfo.email_sales || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, email_sales: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Working Hours</label>
                    <input
                      type="text"
                      value={contactInfo.working_hours || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, working_hours: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div className="md:col-span-2">
                    <label className="block text-sm font-medium text-slate-700 mb-1">Address</label>
                    <input
                      type="text"
                      value={contactInfo.address || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, address: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">City</label>
                    <input
                      type="text"
                      value={contactInfo.city || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, city: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">State</label>
                    <input
                      type="text"
                      value={contactInfo.state || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, state: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Pincode</label>
                    <input
                      type="text"
                      value={contactInfo.pincode || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, pincode: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1">Google Maps URL</label>
                    <input
                      type="text"
                      value={contactInfo.google_maps_url || ''}
                      onChange={(e) => setContactInfo({ ...contactInfo, google_maps_url: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                </div>
                <div className="mt-6">
                  <Button onClick={handleContactSave} className="bg-teal-600 hover:bg-teal-700">
                    <Save className="w-4 h-4 mr-2" /> Save Contact Info
                  </Button>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="p-4 border-b flex justify-between items-center">
              <h2 className="text-lg font-bold">
                {editItem ? 'Edit' : 'Create'} {activeTab.slice(0, -1)}
              </h2>
              <button onClick={() => setShowModal(false)} className="p-2 hover:bg-slate-100 rounded-lg">
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="p-4 space-y-4">
              {/* Page Form */}
              {activeTab === 'pages' && (
                <>
                  <div>
                    <label className="block text-sm font-medium mb-1">Title *</label>
                    <input
                      type="text"
                      value={formData.title || ''}
                      onChange={(e) => setFormData({ ...formData, title: e.target.value, slug: generateSlug(e.target.value) })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">URL Slug *</label>
                    <input
                      type="text"
                      value={formData.slug || ''}
                      onChange={(e) => setFormData({ ...formData, slug: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Content *</label>
                    <textarea
                      value={formData.content || ''}
                      onChange={(e) => setFormData({ ...formData, content: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg h-40"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Meta Title (SEO)</label>
                    <input
                      type="text"
                      value={formData.meta_title || ''}
                      onChange={(e) => setFormData({ ...formData, meta_title: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Meta Description (SEO)</label>
                    <textarea
                      value={formData.meta_description || ''}
                      onChange={(e) => setFormData({ ...formData, meta_description: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg h-20"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Status</label>
                    <select
                      value={formData.status || 'draft'}
                      onChange={(e) => setFormData({ ...formData, status: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    >
                      <option value="draft">Draft</option>
                      <option value="published">Published</option>
                    </select>
                  </div>
                </>
              )}

              {/* Banner Form */}
              {activeTab === 'banners' && (
                <>
                  <div>
                    <label className="block text-sm font-medium mb-1">Title *</label>
                    <input
                      type="text"
                      value={formData.title || ''}
                      onChange={(e) => setFormData({ ...formData, title: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Subtitle</label>
                    <input
                      type="text"
                      value={formData.subtitle || ''}
                      onChange={(e) => setFormData({ ...formData, subtitle: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Image URL *</label>
                    <input
                      type="text"
                      value={formData.image_url || ''}
                      onChange={(e) => setFormData({ ...formData, image_url: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Link URL</label>
                    <input
                      type="text"
                      value={formData.link_url || ''}
                      onChange={(e) => setFormData({ ...formData, link_url: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Button Text</label>
                    <input
                      type="text"
                      value={formData.button_text || ''}
                      onChange={(e) => setFormData({ ...formData, button_text: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Position</label>
                    <select
                      value={formData.position || 'home_hero'}
                      onChange={(e) => setFormData({ ...formData, position: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    >
                      <option value="home_hero">Home Hero</option>
                      <option value="home_secondary">Home Secondary</option>
                      <option value="page_header">Page Header</option>
                    </select>
                  </div>
                </>
              )}

              {/* Menu Form */}
              {activeTab === 'menu' && (
                <>
                  <div>
                    <label className="block text-sm font-medium mb-1">Title *</label>
                    <input
                      type="text"
                      value={formData.title || ''}
                      onChange={(e) => setFormData({ ...formData, title: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">URL *</label>
                    <input
                      type="text"
                      value={formData.url || ''}
                      onChange={(e) => setFormData({ ...formData, url: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Order</label>
                    <input
                      type="number"
                      value={formData.order || 0}
                      onChange={(e) => setFormData({ ...formData, order: parseInt(e.target.value) })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Open in</label>
                    <select
                      value={formData.target || '_self'}
                      onChange={(e) => setFormData({ ...formData, target: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    >
                      <option value="_self">Same Tab</option>
                      <option value="_blank">New Tab</option>
                    </select>
                  </div>
                </>
              )}

              {/* Blog Form */}
              {activeTab === 'blog' && (
                <>
                  <div>
                    <label className="block text-sm font-medium mb-1">Title *</label>
                    <input
                      type="text"
                      value={formData.title || ''}
                      onChange={(e) => setFormData({ ...formData, title: e.target.value, slug: generateSlug(e.target.value) })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">URL Slug *</label>
                    <input
                      type="text"
                      value={formData.slug || ''}
                      onChange={(e) => setFormData({ ...formData, slug: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Excerpt</label>
                    <textarea
                      value={formData.excerpt || ''}
                      onChange={(e) => setFormData({ ...formData, excerpt: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg h-20"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Content *</label>
                    <textarea
                      value={formData.content || ''}
                      onChange={(e) => setFormData({ ...formData, content: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg h-40"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Featured Image URL</label>
                    <input
                      type="text"
                      value={formData.featured_image || ''}
                      onChange={(e) => setFormData({ ...formData, featured_image: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Category</label>
                    <input
                      type="text"
                      value={formData.category || ''}
                      onChange={(e) => setFormData({ ...formData, category: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">Status</label>
                    <select
                      value={formData.status || 'draft'}
                      onChange={(e) => setFormData({ ...formData, status: e.target.value })}
                      className="w-full px-3 py-2 border rounded-lg"
                    >
                      <option value="draft">Draft</option>
                      <option value="published">Published</option>
                    </select>
                  </div>
                </>
              )}
            </div>
            <div className="p-4 border-t flex justify-end gap-2">
              <Button variant="outline" onClick={() => setShowModal(false)}>Cancel</Button>
              <Button onClick={handleSave} className="bg-teal-600 hover:bg-teal-700">
                <Save className="w-4 h-4 mr-2" /> Save
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default CMSDashboard;
