import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import * as BABYLON from '@babylonjs/core';
import '@babylonjs/loaders';
import { AdvancedDynamicTexture, TextBlock, Control } from '@babylonjs/gui';
import earcut from 'earcut';
import { 
  Loader2, ShoppingCart, Share2, Clock, Eye, Edit3, 
  CheckCircle, AlertCircle, Copy, MessageCircle
} from 'lucide-react';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Cutout colors
const CUTOUT_COLORS = {
  SH: { normal: '#3B82F6', highlight: '#60A5FA' },
  R: { normal: '#22C55E', highlight: '#4ADE80' },
  T: { normal: '#F59E0B', highlight: '#FBBF24' },
  HX: { normal: '#8B5CF6', highlight: '#A78BFA' },
  HR: { normal: '#EC4899', highlight: '#F472B6' },
};

const CUTOUT_SHAPES = [
  { id: 'SH', name: 'Hole', label: 'H' },
  { id: 'R', name: 'Rectangle', label: 'R' },
  { id: 'T', name: 'Triangle', label: 'T' },
  { id: 'HX', name: 'Hexagon', label: 'HX' },
  { id: 'HR', name: 'Heart', label: 'HR' },
];

const SharedConfig = () => {
  const { shareId } = useParams();
  const navigate = useNavigate();
  const canvasRef = useRef(null);
  const engineRef = useRef(null);
  const sceneRef = useRef(null);
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sharedConfig, setSharedConfig] = useState(null);
  const [copied, setCopied] = useState(false);

  // Fetch shared config
  useEffect(() => {
    fetchSharedConfig();
  }, [shareId]);

  const fetchSharedConfig = async () => {
    try {
      const response = await fetch(`${API_URL}/api/erp/glass-config/share/${shareId}`);
      if (response.ok) {
        const data = await response.json();
        setSharedConfig(data);
      } else if (response.status === 410) {
        setError('This shared link has expired');
      } else if (response.status === 404) {
        setError('Configuration not found');
      } else {
        setError('Failed to load configuration');
      }
    } catch (err) {
      setError('Failed to load configuration');
    } finally {
      setLoading(false);
    }
  };

  // Initialize 3D preview
  useEffect(() => {
    if (!canvasRef.current || !sharedConfig) return;
    if (engineRef.current) return;

    const engine = new BABYLON.Engine(canvasRef.current, true, { antialias: true });
    engineRef.current = engine;

    const scene = new BABYLON.Scene(engine);
    scene.clearColor = new BABYLON.Color4(0.96, 0.97, 0.98, 1);
    sceneRef.current = scene;

    const camera = new BABYLON.ArcRotateCamera(
      'camera', -Math.PI / 2, Math.PI / 2, 1000, BABYLON.Vector3.Zero(), scene
    );
    camera.mode = BABYLON.Camera.ORTHOGRAPHIC_CAMERA;
    camera.orthoLeft = -300;
    camera.orthoRight = 300;
    camera.orthoTop = 240;
    camera.orthoBottom = -240;
    camera.attachControl(canvasRef.current, true);

    const light = new BABYLON.HemisphericLight('light', new BABYLON.Vector3(0, 1, 0), scene);
    light.intensity = 1.0;

    // Create glass and cutouts
    const config = sharedConfig.glass_config;
    const cutouts = sharedConfig.cutouts || [];
    
    const maxSize = 350;
    const scale = Math.min(maxSize / config.width_mm, maxSize / config.height_mm, 0.4);
    
    // Glass
    const glass = BABYLON.MeshBuilder.CreateBox('glass', {
      width: config.width_mm * scale,
      height: config.height_mm * scale,
      depth: 5
    }, scene);
    const glassMat = new BABYLON.StandardMaterial('glassMat', scene);
    glassMat.diffuseColor = BABYLON.Color3.FromHexString('#E8E8E8');
    glassMat.alpha = 0.75;
    glass.material = glassMat;
    glass.isPickable = false;

    // Cutouts
    cutouts.forEach((cutout, idx) => {
      const posX = (cutout.x - config.width_mm / 2) * scale;
      const posY = (cutout.y - config.height_mm / 2) * scale;
      
      let mesh;
      if (['SH', 'HX', 'HR'].includes(cutout.type)) {
        mesh = BABYLON.MeshBuilder.CreateCylinder(`cutout_${idx}`, {
          diameter: (cutout.diameter || 50) * scale,
          height: 10,
          tessellation: cutout.type === 'HX' ? 6 : 32
        }, scene);
        mesh.rotation.x = Math.PI / 2;
      } else {
        mesh = BABYLON.MeshBuilder.CreateBox(`cutout_${idx}`, {
          width: (cutout.width || 100) * scale,
          height: (cutout.height || 80) * scale,
          depth: 10
        }, scene);
      }

      mesh.position.x = posX;
      mesh.position.y = posY;
      mesh.position.z = 8;

      const mat = new BABYLON.StandardMaterial(`cutoutMat_${idx}`, scene);
      mat.diffuseColor = BABYLON.Color3.FromHexString(CUTOUT_COLORS[cutout.type]?.normal || '#3B82F6');
      mesh.material = mat;
    });

    // GUI labels
    const gui = AdvancedDynamicTexture.CreateFullscreenUI('UI', true, scene);
    
    const widthLabel = new TextBlock('widthLabel');
    widthLabel.text = `← ${config.width_mm} mm →`;
    widthLabel.color = '#333';
    widthLabel.fontSize = 14;
    widthLabel.fontWeight = 'bold';
    widthLabel.top = '200px';
    gui.addControl(widthLabel);

    const heightLabel = new TextBlock('heightLabel');
    heightLabel.text = `↑ ${config.height_mm} mm ↓`;
    heightLabel.color = '#333';
    heightLabel.fontSize = 14;
    heightLabel.fontWeight = 'bold';
    heightLabel.rotation = -Math.PI / 2;
    heightLabel.left = '-260px';
    gui.addControl(heightLabel);

    engine.runRenderLoop(() => scene.render());
    
    const handleResize = () => engine.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      gui.dispose();
      engine.dispose();
    };
  }, [sharedConfig]);

  const copyLink = () => {
    navigator.clipboard.writeText(window.location.href);
    setCopied(true);
    toast.success('Link copied to clipboard!');
    setTimeout(() => setCopied(false), 2000);
  };

  const shareWhatsApp = () => {
    const text = `Check out this glass configuration: ${sharedConfig?.title || 'Custom Glass Design'}`;
    const url = encodeURIComponent(window.location.href);
    window.open(`https://wa.me/?text=${encodeURIComponent(text)}%20${url}`, '_blank');
  };

  const shareEmail = () => {
    const subject = encodeURIComponent(`Glass Configuration - ${sharedConfig?.title || 'Custom Design'}`);
    const body = encodeURIComponent(`Hi,\n\nI wanted to share this glass configuration with you:\n\n${window.location.href}\n\nTake a look and let me know what you think!`);
    window.open(`mailto:?subject=${subject}&body=${body}`, '_blank');
  };

  const editAndOrder = () => {
    // Navigate to configurator with pre-loaded config
    const config = sharedConfig.glass_config;
    const cutouts = sharedConfig.cutouts || [];
    
    // Store in sessionStorage for the configurator to pick up
    sessionStorage.setItem('shared_config', JSON.stringify({
      glass_config: config,
      cutouts: cutouts,
      share_id: shareId
    }));
    
    navigate('/customize?from=share');
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="w-12 h-12 animate-spin text-blue-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading shared configuration...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-red-50 to-orange-100 flex items-center justify-center">
        <div className="bg-white rounded-2xl shadow-xl p-8 max-w-md text-center">
          <AlertCircle className="w-16 h-16 text-red-500 mx-auto mb-4" />
          <h1 className="text-2xl font-bold text-gray-800 mb-2">Oops!</h1>
          <p className="text-gray-600 mb-6">{error}</p>
          <Button onClick={() => navigate('/customize')} className="bg-blue-600 hover:bg-blue-700">
            Create New Configuration
          </Button>
        </div>
      </div>
    );
  }

  const config = sharedConfig?.glass_config || {};
  const cutouts = sharedConfig?.cutouts || [];
  const expiresAt = sharedConfig?.expires_at ? new Date(sharedConfig.expires_at) : null;

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50" data-testid="shared-config-page">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-6xl mx-auto px-4 py-4 flex justify-between items-center">
          <div>
            <h1 className="text-xl font-bold text-gray-800" data-testid="shared-config-title">
              {sharedConfig?.title || 'Shared Glass Configuration'}
            </h1>
            <div className="flex items-center gap-4 text-sm text-gray-500 mt-1">
              <span className="flex items-center gap-1" data-testid="view-count">
                <Eye className="w-4 h-4" />
                {sharedConfig?.view_count || 0} views
              </span>
              {expiresAt && (
                <span className="flex items-center gap-1" data-testid="expiry-date">
                  <Clock className="w-4 h-4" />
                  Expires {expiresAt.toLocaleDateString()}
                </span>
              )}
            </div>
          </div>
          <div className="flex gap-2">
            <Button
              onClick={copyLink}
              variant="outline"
              size="sm"
              className="flex items-center gap-2"
              data-testid="copy-link-button"
            >
              {copied ? <CheckCircle className="w-4 h-4 text-green-500" /> : <Copy className="w-4 h-4" />}
              {copied ? 'Copied!' : 'Copy Link'}
            </Button>
            <Button
              onClick={shareWhatsApp}
              variant="outline"
              size="sm"
              className="flex items-center gap-2 bg-green-50 hover:bg-green-100 border-green-200"
              data-testid="whatsapp-button"
            >
              <MessageCircle className="w-4 h-4 text-green-600" />
              WhatsApp
            </Button>
          </div>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-4 py-6">
        <div className="grid lg:grid-cols-3 gap-6">
          {/* 3D Preview */}
          <div className="lg:col-span-2">
            <div className="bg-white rounded-2xl shadow-lg overflow-hidden">
              <div className="p-4 border-b bg-gray-50">
                <h3 className="font-semibold text-gray-800">3D Preview</h3>
              </div>
              <canvas
                ref={canvasRef}
                className="w-full h-[400px]"
                style={{ touchAction: 'none' }}
                data-testid="shared-config-canvas"
              />
            </div>
          </div>

          {/* Config Details */}
          <div className="space-y-4">
            {/* Glass Specs */}
            <div className="bg-white rounded-2xl shadow-lg p-5" data-testid="glass-specifications">
              <h3 className="font-semibold text-gray-800 mb-4">Glass Specifications</h3>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between py-2 border-b">
                  <span className="text-gray-500">Dimensions</span>
                  <span className="font-medium" data-testid="dimensions">{config.width_mm} × {config.height_mm} mm</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-gray-500">Thickness</span>
                  <span className="font-medium" data-testid="thickness">{config.thickness_mm}mm</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-gray-500">Glass Type</span>
                  <span className="font-medium capitalize" data-testid="glass-type">{config.glass_type}</span>
                </div>
                <div className="flex justify-between py-2 border-b">
                  <span className="text-gray-500">Color</span>
                  <span className="font-medium" data-testid="color">{config.color_name}</span>
                </div>
                <div className="flex justify-between py-2">
                  <span className="text-gray-500">Cutouts</span>
                  <span className="font-medium" data-testid="cutouts-count">{cutouts.length}</span>
                </div>
              </div>
            </div>

            {/* Cutouts List */}
            {cutouts.length > 0 && (
              <div className="bg-white rounded-2xl shadow-lg p-5" data-testid="cutouts-list">
                <h3 className="font-semibold text-gray-800 mb-4">Cutouts</h3>
                <div className="space-y-2 max-h-48 overflow-y-auto">
                  {cutouts.map((cutout, idx) => {
                    const shape = CUTOUT_SHAPES.find(s => s.id === cutout.type);
                    return (
                      <div 
                        key={idx}
                        className="flex justify-between items-center p-2 bg-gray-50 rounded-lg text-sm"
                        data-testid={`cutout-item-${idx}`}
                      >
                        <div className="flex items-center gap-2">
                          <div 
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: CUTOUT_COLORS[cutout.type]?.normal }}
                          />
                          <span>{shape?.name || 'Unknown'} #{idx + 1}</span>
                        </div>
                        <span className="text-gray-500">
                          {cutout.diameter ? `Ø${cutout.diameter}mm` : `${cutout.width}×${cutout.height}mm`}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Action Buttons */}
            <div className="space-y-3">
              <Button
                onClick={editAndOrder}
                className="w-full h-12 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-xl flex items-center justify-center gap-2"
                data-testid="edit-order-button"
              >
                <Edit3 className="w-5 h-5" />
                Edit & Order
              </Button>
              
              <Button
                onClick={shareEmail}
                variant="outline"
                className="w-full h-12 rounded-xl flex items-center justify-center gap-2"
                data-testid="share-email-button"
              >
                <Share2 className="w-5 h-5" />
                Share via Email
              </Button>
            </div>

            {/* Share Stats */}
            <div className="bg-gradient-to-r from-blue-500 to-indigo-600 rounded-2xl p-5 text-white" data-testid="share-stats">
              <p className="text-sm opacity-80 mb-1">Configuration shared</p>
              <p className="text-3xl font-bold" data-testid="total-views">{sharedConfig?.view_count || 0}</p>
              <p className="text-sm opacity-80">times viewed</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SharedConfig;
