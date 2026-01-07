import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import * as BABYLON from '@babylonjs/core';
import '@babylonjs/loaders';
import { AdvancedDynamicTexture, TextBlock, Control } from '@babylonjs/gui';
import earcut from 'earcut';
import { QRCodeSVG } from 'qrcode.react';
import { 
  Package, Ruler, Circle, Square, RectangleHorizontal,
  Plus, Minus, Trash2, ZoomIn, ZoomOut, RotateCcw,
  FileText, Loader2, Triangle, ChevronDown,
  Hexagon, Heart, MousePointer, GripVertical,
  Copy, Grid3X3, LayoutTemplate, Magnet, Download, Eye, Crosshair,
  Wrench, Truck, MapPin, Calculator, Share2, MessageCircle, Mail, X, QrCode, Link2
} from 'lucide-react';
import { Button } from '../components/ui/button';
import { toast } from 'sonner';

const API_URL = process.env.REACT_APP_BACKEND_URL;

// Cut-out shape types
const CUTOUT_SHAPES = [
  { id: 'SH', name: 'Hole', icon: Circle, label: 'H', defaultSize: { diameter: 50 } },
  { id: 'R', name: 'Rectangle', icon: RectangleHorizontal, label: 'R', defaultSize: { width: 100, height: 80 } },
  { id: 'T', name: 'Triangle', icon: Triangle, label: 'T', defaultSize: { width: 100, height: 80 } },
  { id: 'HX', name: 'Hexagon', icon: Hexagon, label: 'HX', defaultSize: { diameter: 60 } },
  { id: 'HR', name: 'Heart', icon: Heart, label: 'HR', defaultSize: { diameter: 60 } },
];

// Job Work Types
const JOB_WORK_TYPES = [
  { id: 'toughening', name: 'Glass Toughening', description: 'Heat treatment for safety glass' },
  { id: 'lamination', name: 'Lamination', description: 'PVB/SGP lamination for safety' },
  { id: 'beveling', name: 'Beveling', description: 'Edge beveling and polishing' },
  { id: 'drilling', name: 'Drilling & Holes', description: 'Precision hole drilling' },
  { id: 'edging', name: 'Edge Processing', description: 'Flat/pencil/bevel edges' },
];

// Common hole sizes for quick selection (in mm)
const PRESET_HOLE_SIZES = [5, 8, 10, 12, 14, 16, 20, 25, 30, 40, 50];

// Snap grid sizes (in mm)
const SNAP_GRID_SIZES = [5, 10, 20, 50];

// Cutout templates
const CUTOUT_TEMPLATES = [
  { id: 'handle_hole', name: 'Door Handle Hole', type: 'SH', diameter: 35, description: 'Standard door handle cutout' },
  { id: 'hinge_cutout', name: 'Hinge Cutout', type: 'R', width: 80, height: 25, description: 'Standard hinge placement' },
  { id: 'lock_cutout', name: 'Lock Cutout', type: 'R', width: 25, height: 150, description: 'Mortise lock cutout' },
  { id: 'cable_hole', name: 'Cable Pass-through', type: 'SH', diameter: 50, description: 'Cable management hole' },
  { id: 'vent_hole', name: 'Ventilation Hole', type: 'SH', diameter: 25, description: 'Small ventilation cutout' },
];

// Production mode colors for different cutout types
const CUTOUT_COLORS = {
  SH: { normal: '#3B82F6', highlight: '#60A5FA', name: 'Blue' },
  R: { normal: '#22C55E', highlight: '#4ADE80', name: 'Green' },
  T: { normal: '#F59E0B', highlight: '#FBBF24', name: 'Orange' },
  HX: { normal: '#8B5CF6', highlight: '#A78BFA', name: 'Purple' },
  HR: { normal: '#EC4899', highlight: '#F472B6', name: 'Pink' },
};

// View modes
const VIEW_MODES = {
  NORMAL: 'normal',
  HIGH_CONTRAST: 'high_contrast',
};

// Default job work item
const createDefaultJobItem = (id) => ({
  id,
  name: `Glass ${id}`,
  job_work_type: 'toughening',
  thickness_mm: 8,
  width_mm: 900,
  height_mm: 600,
  cutouts: [],
  quantity: 1,
  item_price: 0
});

const JobWork3DConfigurator = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  
  // Canvas and engine refs - NEVER recreate
  const canvasRef = useRef(null);
  const engineRef = useRef(null);
  const sceneRef = useRef(null);
  const cameraRef = useRef(null);
  const guiTextureRef = useRef(null);
  
  // FIXED glass mesh - created once, never recreated
  const glassMeshRef = useRef(null);
  const glassMaterialRef = useRef(null);
  const glassCreatedRef = useRef(false);
  
  // Dimension labels for glass (static - created once)
  const widthLabelRef = useRef(null);
  const heightLabelRef = useRef(null);
  
  // Shape meshes and their attached elements
  const cutoutMeshesRef = useRef(new Map());
  const cutoutLabelsRef = useRef(new Map());
  const handleMeshesRef = useRef([]);
  
  // Interaction refs
  const isDraggingRef = useRef(false);
  const dragTypeRef = useRef(null);
  const dragDataRef = useRef(null);
  const lastPointerRef = useRef({ x: 0, y: 0 });
  
  // State
  const [jobItems, setJobItems] = useState([createDefaultJobItem(1)]);
  const [activeItemIndex, setActiveItemIndex] = useState(0);
  const [labourRates, setLabourRates] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedCutoutId, setSelectedCutoutId] = useState(null);
  const [selectedCutoutType, setSelectedCutoutType] = useState('SH');
  const [isPlacementMode, setIsPlacementMode] = useState(false);
  
  // Snap-to-grid state
  const [snapEnabled, setSnapEnabled] = useState(false);
  const [snapGridSize, setSnapGridSize] = useState(10);
  
  // Templates panel
  const [showTemplates, setShowTemplates] = useState(false);
  const [orderTotal, setOrderTotal] = useState(0);
  const [calculatingPrice, setCalculatingPrice] = useState(false);
  
  // Production mode state
  const [viewMode, setViewMode] = useState(VIEW_MODES.NORMAL);
  const [showGrid, setShowGrid] = useState(false);
  const [showCenterMarks, setShowCenterMarks] = useState(true);
  const [showDimensionLines, setShowDimensionLines] = useState(true);
  const [showCutoutNumbers, setShowCutoutNumbers] = useState(true);
  const [exportingPDF, setExportingPDF] = useState(false);
  
  // Share functionality state
  const [showShareModal, setShowShareModal] = useState(false);
  const [shareUrl, setShareUrl] = useState('');
  const [creatingShareLink, setCreatingShareLink] = useState(false);
  
  // Transport options
  const [needsPickup, setNeedsPickup] = useState(false);
  const [needsDelivery, setNeedsDelivery] = useState(false);
  const [pickupDistance, setPickupDistance] = useState(0);
  const [deliveryDistance, setDeliveryDistance] = useState(0);
  
  // Dropdown open states
  const [openDropdown, setOpenDropdown] = useState(null);
  
  // Current config
  const config = jobItems[activeItemIndex] || createDefaultJobItem(1);
  
  // Grid overlay ref
  const gridLinesRef = useRef([]);
  
  // Refs for latest state in event handlers
  const activeItemIndexRef = useRef(activeItemIndex);
  const selectedCutoutIdRef = useRef(selectedCutoutId);
  const isPlacementModeRef = useRef(isPlacementMode);
  const selectedCutoutTypeRef = useRef(selectedCutoutType);
  const configRef = useRef(config);
  const snapEnabledRef = useRef(snapEnabled);
  const snapGridSizeRef = useRef(snapGridSize);
  
  useEffect(() => { activeItemIndexRef.current = activeItemIndex; }, [activeItemIndex]);
  useEffect(() => { selectedCutoutIdRef.current = selectedCutoutId; }, [selectedCutoutId]);
  useEffect(() => { isPlacementModeRef.current = isPlacementMode; }, [isPlacementMode]);
  useEffect(() => { selectedCutoutTypeRef.current = selectedCutoutType; }, [selectedCutoutType]);
  useEffect(() => { configRef.current = config; }, [config]);
  useEffect(() => { snapEnabledRef.current = snapEnabled; }, [snapEnabled]);
  useEffect(() => { snapGridSizeRef.current = snapGridSize; }, [snapGridSize]);

  // Get selected cutout
  const selectedCutout = config.cutouts.find(c => c.id === selectedCutoutId);

  // Update current item helper
  const updateCurrentItem = useCallback((updates) => {
    setJobItems(prev => prev.map((item, idx) => 
      idx === activeItemIndex ? { ...item, ...updates } : item
    ));
  }, [activeItemIndex]);

  // Scale calculation - FIXED visual size for production printing
  const getScale = useCallback(() => {
    // Fixed visual size - glass always appears same size visually
    const fixedWidth = 400;
    const fixedHeight = 300;
    
    const aspectRatio = config.width_mm / config.height_mm;
    
    if (aspectRatio > fixedWidth / fixedHeight) {
      return fixedWidth / config.width_mm;
    } else {
      return fixedHeight / config.height_mm;
    }
  }, [config.width_mm, config.height_mm]);

  // Fetch labour rates
  useEffect(() => {
    fetchLabourRates();
  }, []);

  const fetchLabourRates = async () => {
    try {
      const token = localStorage.getItem('token');
      const headers = token ? { 'Authorization': `Bearer ${token}` } : {};
      const response = await fetch(`${API_URL}/api/erp/glass-config/job-work/labour-rates`, { headers });
      const data = await response.json();
      setLabourRates(data);
    } catch (error) {
      console.error('Failed to fetch labour rates:', error);
      // Fallback rates
      setLabourRates({
        rates: { "4": 8, "5": 10, "6": 12, "8": 15, "10": 18, "12": 22, "15": 28, "19": 35 },
        hole_rates: {
          circle: { base: 20, per_mm: 0.5 },
          square: { base: 30, per_mm: 0.6 },
          rectangle: { base: 35, per_mm: 0.6 }
        },
        transport_pickup_base: 300,
        transport_drop_base: 300,
        transport_per_km: 12
      });
    } finally {
      setLoading(false);
    }
  };

  // Initialize Babylon.js scene - ONCE only
  useEffect(() => {
    if (!canvasRef.current || loading) return;
    if (engineRef.current) return;

    const engine = new BABYLON.Engine(canvasRef.current, true, { 
      preserveDrawingBuffer: true, 
      stencil: true,
      antialias: true
    });
    engineRef.current = engine;

    const scene = new BABYLON.Scene(engine);
    scene.clearColor = new BABYLON.Color4(0.96, 0.97, 0.98, 1);
    sceneRef.current = scene;

    // Camera - orthographic for 2D-like view
    const camera = new BABYLON.ArcRotateCamera(
      'camera', -Math.PI / 2, Math.PI / 2, 1000, BABYLON.Vector3.Zero(), scene
    );
    camera.mode = BABYLON.Camera.ORTHOGRAPHIC_CAMERA;
    camera.orthoLeft = -350;
    camera.orthoRight = 350;
    camera.orthoTop = 280;
    camera.orthoBottom = -280;
    camera.attachControl(canvasRef.current, true);
    camera.wheelPrecision = 10;
    camera.panningSensibility = 0;
    cameraRef.current = camera;

    // Lights
    const light = new BABYLON.HemisphericLight('light', new BABYLON.Vector3(0, 1, 0), scene);
    light.intensity = 1.0;

    // GUI for labels
    const guiTexture = AdvancedDynamicTexture.CreateFullscreenUI('UI', true, scene);
    guiTextureRef.current = guiTexture;

    // Create FIXED glass mesh
    createGlassMesh(scene);
    createGlassDimensionLabels(guiTexture);

    // Pointer events
    scene.onPointerObservable.add((info) => {
      requestAnimationFrame(() => handlePointer(info));
    });

    engine.runRenderLoop(() => scene.render());
    
    const handleResize = () => engine.resize();
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      guiTexture.dispose();
      engine.dispose();
    };
  }, [loading]);

  // Create FIXED glass mesh
  const createGlassMesh = (scene) => {
    if (glassCreatedRef.current) return;
    
    const glass = BABYLON.MeshBuilder.CreateBox('glass', { width: 1, height: 1, depth: 1 }, scene);
    const glassMat = new BABYLON.StandardMaterial('glassMat', scene);
    glassMat.diffuseColor = BABYLON.Color3.FromHexString('#D4E5F7');
    glassMat.alpha = 0.75;
    glassMat.specularColor = new BABYLON.Color3(0.3, 0.3, 0.3);
    glass.material = glassMat;
    glass.position = BABYLON.Vector3.Zero();
    glass.isPickable = false;
    
    glassMeshRef.current = glass;
    glassMaterialRef.current = glassMat;
    glassCreatedRef.current = true;
    
    updateGlassMeshSize();
  };

  // Update glass mesh size WITHOUT recreating - FIXED visual size
  const updateGlassMeshSize = useCallback(() => {
    if (!glassMeshRef.current) return;
    
    // Fixed visual dimensions
    const fixedVisualWidth = 400;
    const fixedVisualHeight = 300;
    
    const aspectRatio = config.width_mm / config.height_mm;
    let visualWidth, visualHeight;
    
    if (aspectRatio > fixedVisualWidth / fixedVisualHeight) {
      visualWidth = fixedVisualWidth;
      visualHeight = fixedVisualWidth / aspectRatio;
    } else {
      visualHeight = fixedVisualHeight;
      visualWidth = fixedVisualHeight * aspectRatio;
    }
    
    glassMeshRef.current.scaling.x = visualWidth;
    glassMeshRef.current.scaling.y = visualHeight;
    glassMeshRef.current.scaling.z = 5;
    
    updateGlassDimensionLabels();
  }, [config.width_mm, config.height_mm]);

  // Create static glass dimension labels - positioned at glass edges
  const createGlassDimensionLabels = (gui) => {
    const widthLabel = new TextBlock('widthLabel');
    widthLabel.text = `← ${config.width_mm} mm →`;
    widthLabel.color = '#1a1a1a';
    widthLabel.fontSize = 16;
    widthLabel.fontWeight = 'bold';
    widthLabel.outlineWidth = 3;
    widthLabel.outlineColor = 'white';
    widthLabel.top = '180px';
    widthLabel.verticalAlignment = Control.VERTICAL_ALIGNMENT_CENTER;
    gui.addControl(widthLabel);
    widthLabelRef.current = widthLabel;

    const heightLabel = new TextBlock('heightLabel');
    heightLabel.text = `↑ ${config.height_mm} mm ↓`;
    heightLabel.color = '#1a1a1a';
    heightLabel.fontSize = 16;
    heightLabel.fontWeight = 'bold';
    heightLabel.outlineWidth = 3;
    heightLabel.outlineColor = 'white';
    heightLabel.rotation = -Math.PI / 2;
    heightLabel.left = '240px';
    heightLabel.horizontalAlignment = Control.HORIZONTAL_ALIGNMENT_CENTER;
    gui.addControl(heightLabel);
    heightLabelRef.current = heightLabel;
  };

  // Update glass dimension labels text only
  const updateGlassDimensionLabels = useCallback(() => {
    if (widthLabelRef.current) widthLabelRef.current.text = `← ${config.width_mm} mm →`;
    if (heightLabelRef.current) heightLabelRef.current.text = `↑ ${config.height_mm} mm ↓`;
  }, [config.width_mm, config.height_mm]);

  // Update only when glass size changes
  useEffect(() => {
    if (sceneRef.current && glassMeshRef.current) {
      updateGlassMeshSize();
    }
  }, [config.width_mm, config.height_mm, updateGlassMeshSize]);

  // Update cutout visuals without touching glass
  useEffect(() => {
    if (sceneRef.current && guiTextureRef.current) {
      updateCutoutVisuals();
    }
  }, [config.cutouts, selectedCutoutId]);

  // Update cutout meshes and labels
  const updateCutoutVisuals = useCallback(() => {
    const scene = sceneRef.current;
    const gui = guiTextureRef.current;
    if (!scene || !gui) return;

    const scale = getScale();
    const currentCutoutIds = new Set(config.cutouts.map(c => c.id));
    
    // Remove old cutouts
    cutoutMeshesRef.current.forEach((mesh, id) => {
      if (!currentCutoutIds.has(id)) {
        mesh.dispose();
        cutoutMeshesRef.current.delete(id);
      }
    });
    
    cutoutLabelsRef.current.forEach((labels, id) => {
      if (!currentCutoutIds.has(id)) {
        labels.forEach(l => l.dispose());
        cutoutLabelsRef.current.delete(id);
      }
    });

    handleMeshesRef.current.forEach(h => h.dispose());
    handleMeshesRef.current = [];

    config.cutouts.forEach((cutout, index) => {
      createOrUpdateCutoutMesh(cutout, scale, scene);
      createOrUpdateCutoutLabels(cutout, scale, gui, index);
      
      if (cutout.id === selectedCutoutIdRef.current) {
        createHandles(cutout, scale, scene);
      }
    });
    
    // Handle grid overlay
    if (showGrid) {
      createGridOverlay(scale, scene);
    } else {
      gridLinesRef.current.forEach(line => line.dispose());
      gridLinesRef.current = [];
    }
  }, [config.cutouts, selectedCutoutId, getScale, showGrid, viewMode, showCenterMarks, showDimensionLines, showCutoutNumbers]);

  // Create or update cutout mesh
  const createOrUpdateCutoutMesh = (cutout, scale, scene) => {
    const existingMesh = cutoutMeshesRef.current.get(cutout.id);
    if (existingMesh) {
      existingMesh.dispose();
      cutoutMeshesRef.current.delete(cutout.id);
    }
    
    const glassWidth = configRef.current.width_mm;
    const glassHeight = configRef.current.height_mm;
    const posX = (cutout.x - glassWidth / 2) * scale;
    const posY = (cutout.y - glassHeight / 2) * scale;
    const rotation = (cutout.rotation || 0) * Math.PI / 180;

    let mesh;
    if (['SH', 'HX', 'HR'].includes(cutout.type)) {
      const tessellation = cutout.type === 'HX' ? 6 : 32;
      mesh = BABYLON.MeshBuilder.CreateCylinder(`cutout_${cutout.id}`, {
        diameter: (cutout.diameter || 50) * scale,
        height: 10,
        tessellation
      }, scene);
      mesh.rotation.x = Math.PI / 2;
    } else if (cutout.type === 'T') {
      const w = (cutout.width || 100) * scale;
      const h = (cutout.height || 80) * scale;
      const triangleShape = [
        new BABYLON.Vector3(0, h / 2, 0),
        new BABYLON.Vector3(-w / 2, -h / 2, 0),
        new BABYLON.Vector3(w / 2, -h / 2, 0),
      ];
      mesh = BABYLON.MeshBuilder.ExtrudePolygon(`cutout_${cutout.id}`, { 
        shape: triangleShape, depth: 10, sideOrientation: BABYLON.Mesh.DOUBLESIDE
      }, scene, earcut);
      mesh.rotation.x = -Math.PI / 2;
    } else {
      mesh = BABYLON.MeshBuilder.CreateBox(`cutout_${cutout.id}`, {
        width: (cutout.width || 100) * scale,
        height: (cutout.height || 80) * scale,
        depth: 10
      }, scene);
    }

    mesh.position.x = posX;
    mesh.position.y = posY;
    mesh.position.z = 8;
    mesh.rotation.z = rotation;

    const cutoutColor = CUTOUT_COLORS[cutout.type] || CUTOUT_COLORS.SH;
    const isSelected = cutout.id === selectedCutoutIdRef.current;
    
    const mat = new BABYLON.StandardMaterial(`cutoutMat_${cutout.id}`, scene);
    
    if (viewMode === VIEW_MODES.HIGH_CONTRAST) {
      mat.diffuseColor = isSelected ? new BABYLON.Color3(0.3, 0.3, 0.3) : new BABYLON.Color3(0.1, 0.1, 0.1);
      mat.emissiveColor = new BABYLON.Color3(0.05, 0.05, 0.05);
    } else {
      const hexColor = isSelected ? cutoutColor.highlight : cutoutColor.normal;
      mat.diffuseColor = BABYLON.Color3.FromHexString(hexColor);
      mat.emissiveColor = new BABYLON.Color3(0.1, 0.1, 0.1);
    }
    
    mesh.material = mat;
    mesh.isPickable = true;
    mesh.metadata = { cutoutId: cutout.id };

    cutoutMeshesRef.current.set(cutout.id, mesh);
    
    if (showCenterMarks) {
      createCenterMark(cutout, scale, glassWidth, glassHeight, scene);
    }
    
    createCutoutBorder(cutout, scale, glassWidth, glassHeight, scene);
  };

  // Create center mark
  const createCenterMark = (cutout, scale, glassWidth, glassHeight, scene) => {
    const posX = (cutout.x - glassWidth / 2) * scale;
    const posY = (cutout.y - glassHeight / 2) * scale;
    const markSize = 8;
    
    const hPoints = [new BABYLON.Vector3(posX - markSize, posY, 12), new BABYLON.Vector3(posX + markSize, posY, 12)];
    const hLine = BABYLON.MeshBuilder.CreateLines(`centerH_${cutout.id}`, { points: hPoints }, scene);
    hLine.color = viewMode === VIEW_MODES.HIGH_CONTRAST ? new BABYLON.Color3(1, 0, 0) : new BABYLON.Color3(0.8, 0, 0);
    handleMeshesRef.current.push(hLine);
    
    const vPoints = [new BABYLON.Vector3(posX, posY - markSize, 12), new BABYLON.Vector3(posX, posY + markSize, 12)];
    const vLine = BABYLON.MeshBuilder.CreateLines(`centerV_${cutout.id}`, { points: vPoints }, scene);
    vLine.color = viewMode === VIEW_MODES.HIGH_CONTRAST ? new BABYLON.Color3(1, 0, 0) : new BABYLON.Color3(0.8, 0, 0);
    handleMeshesRef.current.push(vLine);
  };

  // Create cutout border
  const createCutoutBorder = (cutout, scale, glassWidth, glassHeight, scene) => {
    const posX = (cutout.x - glassWidth / 2) * scale;
    const posY = (cutout.y - glassHeight / 2) * scale;
    const bounds = getCutoutBounds(cutout);
    
    let borderPoints = [];
    
    if (['SH', 'HX', 'HR'].includes(cutout.type)) {
      const radius = bounds.halfWidth * scale;
      const segments = cutout.type === 'HX' ? 6 : 32;
      for (let i = 0; i <= segments; i++) {
        const angle = (i / segments) * Math.PI * 2;
        borderPoints.push(new BABYLON.Vector3(posX + Math.cos(angle) * radius, posY + Math.sin(angle) * radius, 12));
      }
    } else {
      const halfW = bounds.halfWidth * scale;
      const halfH = bounds.halfHeight * scale;
      borderPoints = [
        new BABYLON.Vector3(posX - halfW, posY - halfH, 12),
        new BABYLON.Vector3(posX + halfW, posY - halfH, 12),
        new BABYLON.Vector3(posX + halfW, posY + halfH, 12),
        new BABYLON.Vector3(posX - halfW, posY + halfH, 12),
        new BABYLON.Vector3(posX - halfW, posY - halfH, 12),
      ];
    }
    
    const border = BABYLON.MeshBuilder.CreateLines(`border_${cutout.id}`, { points: borderPoints }, scene);
    border.color = viewMode === VIEW_MODES.HIGH_CONTRAST 
      ? new BABYLON.Color3(0, 0, 0) 
      : BABYLON.Color3.FromHexString(CUTOUT_COLORS[cutout.type]?.normal || '#3B82F6');
    handleMeshesRef.current.push(border);
  };

  // Create or update cutout labels
  const createOrUpdateCutoutLabels = (cutout, scale, gui, cutoutIndex) => {
    const existingLabels = cutoutLabelsRef.current.get(cutout.id);
    if (existingLabels) existingLabels.forEach(l => l.dispose());

    const labels = [];
    const shapeConfig = CUTOUT_SHAPES.find(s => s.id === cutout.type);
    
    const glassWidth = configRef.current.width_mm;
    const glassHeight = configRef.current.height_mm;
    const posX = (cutout.x - glassWidth / 2) * scale;
    const posY = (cutout.y - glassHeight / 2) * scale;
    
    const textColor = viewMode === VIEW_MODES.HIGH_CONTRAST ? '#000000' : '#1a1a1a';
    const dimColor = viewMode === VIEW_MODES.HIGH_CONTRAST ? '#000000' : '#dc2626';

    if (showCutoutNumbers) {
      const numberLabel = new TextBlock(`number_${cutout.id}`);
      numberLabel.text = `${shapeConfig?.label || 'C'}${cutoutIndex + 1}`;
      numberLabel.color = '#ffffff';
      numberLabel.fontSize = 11;
      numberLabel.fontWeight = 'bold';
      numberLabel.outlineWidth = 2;
      numberLabel.outlineColor = CUTOUT_COLORS[cutout.type]?.normal || '#3B82F6';
      numberLabel.left = `${posX}px`;
      numberLabel.top = `${-posY - 5}px`;
      gui.addControl(numberLabel);
      labels.push(numberLabel);
    }

    const innerLabel = new TextBlock(`inner_${cutout.id}`);
    if (['SH', 'HX', 'HR'].includes(cutout.type)) {
      innerLabel.text = `Ø${Math.round(cutout.diameter || 50)}`;
    } else {
      innerLabel.text = `${Math.round(cutout.width || 100)}×${Math.round(cutout.height || 80)}`;
    }
    innerLabel.color = textColor;
    innerLabel.fontSize = 9;
    innerLabel.fontWeight = 'bold';
    innerLabel.left = `${posX}px`;
    innerLabel.top = `${-posY + 8}px`;
    gui.addControl(innerLabel);
    labels.push(innerLabel);

    const bounds = getCutoutBounds(cutout);
    const leftDist = Math.round(cutout.x - bounds.halfWidth);
    const rightDist = Math.round(glassWidth - cutout.x - bounds.halfWidth);
    const topDist = Math.round(glassHeight - cutout.y - bounds.halfHeight);
    const bottomDist = Math.round(cutout.y - bounds.halfHeight);

    if (showDimensionLines) {
      const labelOffset = 30;
      
      const leftLabel = new TextBlock(`left_${cutout.id}`);
      leftLabel.text = `←${leftDist}`;
      leftLabel.color = dimColor;
      leftLabel.fontSize = 8;
      leftLabel.left = `${posX - bounds.halfWidth * scale - labelOffset}px`;
      leftLabel.top = `${-posY}px`;
      gui.addControl(leftLabel);
      labels.push(leftLabel);

      const rightLabel = new TextBlock(`right_${cutout.id}`);
      rightLabel.text = `${rightDist}→`;
      rightLabel.color = dimColor;
      rightLabel.fontSize = 8;
      rightLabel.left = `${posX + bounds.halfWidth * scale + labelOffset}px`;
      rightLabel.top = `${-posY}px`;
      gui.addControl(rightLabel);
      labels.push(rightLabel);

      const topLabel = new TextBlock(`top_${cutout.id}`);
      topLabel.text = `↑${topDist}`;
      topLabel.color = dimColor;
      topLabel.fontSize = 8;
      topLabel.left = `${posX}px`;
      topLabel.top = `${-posY - bounds.halfHeight * scale - 18}px`;
      gui.addControl(topLabel);
      labels.push(topLabel);

      const bottomLabel = new TextBlock(`bottom_${cutout.id}`);
      bottomLabel.text = `↓${bottomDist}`;
      bottomLabel.color = dimColor;
      bottomLabel.fontSize = 8;
      bottomLabel.left = `${posX}px`;
      bottomLabel.top = `${-posY + bounds.halfHeight * scale + 18}px`;
      gui.addControl(bottomLabel);
      labels.push(bottomLabel);
    }

    cutoutLabelsRef.current.set(cutout.id, labels);
  };

  // Create grid overlay
  const createGridOverlay = (scale, scene) => {
    gridLinesRef.current.forEach(line => line.dispose());
    gridLinesRef.current = [];
    
    const glassWidth = configRef.current.width_mm;
    const glassHeight = configRef.current.height_mm;
    const gridStep = snapGridSize;
    
    const halfW = (glassWidth * scale) / 2;
    const halfH = (glassHeight * scale) / 2;
    
    const gridColor = viewMode === VIEW_MODES.HIGH_CONTRAST 
      ? new BABYLON.Color3(0.5, 0.5, 0.5) 
      : new BABYLON.Color3(0.7, 0.8, 0.9);
    
    for (let x = 0; x <= glassWidth; x += gridStep) {
      const posX = (x - glassWidth / 2) * scale;
      const points = [new BABYLON.Vector3(posX, -halfH, 6), new BABYLON.Vector3(posX, halfH, 6)];
      const line = BABYLON.MeshBuilder.CreateLines(`gridV_${x}`, { points }, scene);
      line.color = gridColor;
      line.alpha = 0.5;
      gridLinesRef.current.push(line);
    }
    
    for (let y = 0; y <= glassHeight; y += gridStep) {
      const posY = (y - glassHeight / 2) * scale;
      const points = [new BABYLON.Vector3(-halfW, posY, 6), new BABYLON.Vector3(halfW, posY, 6)];
      const line = BABYLON.MeshBuilder.CreateLines(`gridH_${y}`, { points }, scene);
      line.color = gridColor;
      line.alpha = 0.5;
      gridLinesRef.current.push(line);
    }
  };

  // Export to PDF
  const exportToPDF = async () => {
    setExportingPDF(true);
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`${API_URL}/api/erp/glass-config/export-pdf`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({
          glass_config: {
            width_mm: config.width_mm,
            height_mm: config.height_mm,
            thickness_mm: config.thickness_mm,
            glass_type: `Job Work - ${JOB_WORK_TYPES.find(j => j.id === config.job_work_type)?.name || config.job_work_type}`,
            color_name: 'Clear',
            application: 'Job Work',
          },
          cutouts: config.cutouts.map((c, idx) => ({
            number: `${CUTOUT_SHAPES.find(s => s.id === c.type)?.label || 'C'}${idx + 1}`,
            type: CUTOUT_SHAPES.find(s => s.id === c.type)?.name || 'Unknown',
            diameter: c.diameter,
            width: c.width,
            height: c.height,
            x: c.x,
            y: c.y,
            rotation: c.rotation || 0,
            left_edge: Math.round(c.x - (c.diameter ? c.diameter/2 : c.width/2)),
            right_edge: Math.round(config.width_mm - c.x - (c.diameter ? c.diameter/2 : c.width/2)),
            top_edge: Math.round(config.height_mm - c.y - (c.diameter ? c.diameter/2 : c.height/2)),
            bottom_edge: Math.round(c.y - (c.diameter ? c.diameter/2 : c.height/2)),
          })),
          quantity: config.quantity,
        })
      });
      
      if (response.ok) {
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `job_work_specification_${Date.now()}.pdf`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        toast.success('PDF exported successfully!');
      } else {
        toast.error('Failed to export PDF');
      }
    } catch (error) {
      console.error('PDF export failed:', error);
      toast.error('Error exporting PDF');
    } finally {
      setExportingPDF(false);
    }
  };

  // Create shareable link
  const createShareLink = async () => {
    setCreatingShareLink(true);
    try {
      const response = await fetch(`${API_URL}/api/erp/glass-config/share`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          glass_config: {
            width_mm: config.width_mm,
            height_mm: config.height_mm,
            thickness_mm: config.thickness_mm,
            glass_type: `Job Work - ${JOB_WORK_TYPES.find(j => j.id === config.job_work_type)?.name || config.job_work_type}`,
            color_name: 'Clear',
            application: 'Job Work',
          },
          cutouts: config.cutouts.map(c => ({
            type: c.type,
            diameter: c.diameter,
            width: c.width,
            height: c.height,
            x: c.x,
            y: c.y,
            rotation: c.rotation || 0,
          })),
          title: `Job Work ${config.width_mm}×${config.height_mm}mm`
        })
      });
      
      if (response.ok) {
        const data = await response.json();
        const fullUrl = `${window.location.origin}/share/${data.share_id}`;
        setShareUrl(fullUrl);
        setShowShareModal(true);
        toast.success('Share link created!');
      } else {
        toast.error('Failed to create share link');
      }
    } catch (error) {
      console.error('Share link creation failed:', error);
      toast.error('Error creating share link');
    } finally {
      setCreatingShareLink(false);
    }
  };

  // Share helpers
  const copyShareLink = () => {
    navigator.clipboard.writeText(shareUrl);
    toast.success('Link copied!');
  };

  const shareViaWhatsApp = () => {
    const text = `Check out this job work configuration (${config.width_mm}×${config.height_mm}mm):`;
    window.open(`https://wa.me/?text=${encodeURIComponent(text)}%20${encodeURIComponent(shareUrl)}`, '_blank');
  };

  const shareViaEmail = () => {
    const subject = encodeURIComponent(`Job Work Configuration - ${config.width_mm}×${config.height_mm}mm`);
    const body = encodeURIComponent(`Hi,\n\nI've created a job work configuration:\n\n${shareUrl}\n\nTake a look!`);
    window.open(`mailto:?subject=${subject}&body=${body}`, '_blank');
  };

  // Create resize and rotation handles
  const createHandles = (cutout, scale, scene) => {
    const bounds = getCutoutBounds(cutout);
    const halfW = bounds.halfWidth * scale;
    const halfH = bounds.halfHeight * scale;
    
    const glassWidth = configRef.current.width_mm;
    const glassHeight = configRef.current.height_mm;
    const cx = (cutout.x - glassWidth / 2) * scale;
    const cy = (cutout.y - glassHeight / 2) * scale;

    const handlePositions = [
      { pos: 'tl', x: cx - halfW, y: cy + halfH },
      { pos: 'tr', x: cx + halfW, y: cy + halfH },
      { pos: 'bl', x: cx - halfW, y: cy - halfH },
      { pos: 'br', x: cx + halfW, y: cy - halfH },
      { pos: 'tm', x: cx, y: cy + halfH },
      { pos: 'bm', x: cx, y: cy - halfH },
      { pos: 'lm', x: cx - halfW, y: cy },
      { pos: 'rm', x: cx + halfW, y: cy },
    ];

    handlePositions.forEach(hp => {
      const handle = BABYLON.MeshBuilder.CreateSphere(`handle_${hp.pos}`, { diameter: 8 }, scene);
      handle.position.x = hp.x;
      handle.position.y = hp.y;
      handle.position.z = 12;

      const mat = new BABYLON.StandardMaterial(`handleMat_${hp.pos}`, scene);
      mat.diffuseColor = new BABYLON.Color3(0.2, 0.5, 1);
      mat.emissiveColor = new BABYLON.Color3(0.1, 0.25, 0.5);
      handle.material = mat;
      handle.isPickable = true;
      handle.metadata = { handleType: 'resize', position: hp.pos, cutoutId: cutout.id };

      handleMeshesRef.current.push(handle);
    });

    const rotHandle = BABYLON.MeshBuilder.CreateSphere('rotHandle', { diameter: 10 }, scene);
    rotHandle.position.x = cx;
    rotHandle.position.y = cy + halfH + 20;
    rotHandle.position.z = 12;

    const rotMat = new BABYLON.StandardMaterial('rotMat', scene);
    rotMat.diffuseColor = new BABYLON.Color3(0.2, 0.7, 0.2);
    rotMat.emissiveColor = new BABYLON.Color3(0.1, 0.35, 0.1);
    rotHandle.material = rotMat;
    rotHandle.isPickable = true;
    rotHandle.metadata = { handleType: 'rotate', cutoutId: cutout.id };

    handleMeshesRef.current.push(rotHandle);

    const points = [new BABYLON.Vector3(cx, cy + halfH, 12), new BABYLON.Vector3(cx, cy + halfH + 20, 12)];
    const line = BABYLON.MeshBuilder.CreateLines('rotLine', { points }, scene);
    line.color = new BABYLON.Color3(0.2, 0.7, 0.2);
    handleMeshesRef.current.push(line);
  };

  // Get cutout bounds
  const getCutoutBounds = (cutout) => {
    if (['SH', 'HX', 'HR'].includes(cutout.type)) {
      const r = (cutout.diameter || 50) / 2;
      return { halfWidth: r, halfHeight: r };
    }
    return { halfWidth: (cutout.width || 100) / 2, halfHeight: (cutout.height || 80) / 2 };
  };

  // Pointer event handlers
  const handlePointer = (info) => {
    const scene = sceneRef.current;
    if (!scene) return;

    switch (info.type) {
      case BABYLON.PointerEventTypes.POINTERDOWN: handlePointerDown(scene); break;
      case BABYLON.PointerEventTypes.POINTERMOVE: handlePointerMove(scene); break;
      case BABYLON.PointerEventTypes.POINTERUP: handlePointerUp(); break;
    }
  };

  const handlePointerDown = (scene) => {
    const pick = scene.pick(scene.pointerX, scene.pointerY);
    lastPointerRef.current = { x: scene.pointerX, y: scene.pointerY };
    
    if (isPlacementModeRef.current) {
      const ray = scene.createPickingRay(scene.pointerX, scene.pointerY, BABYLON.Matrix.Identity(), cameraRef.current);
      if (ray.direction.z !== 0) {
        const t = -ray.origin.z / ray.direction.z;
        const intersectX = ray.origin.x + t * ray.direction.x;
        const intersectY = ray.origin.y + t * ray.direction.y;
        
        const scale = getScale();
        const glassWidth = configRef.current.width_mm;
        const glassHeight = configRef.current.height_mm;
        
        const clickX = intersectX / scale + glassWidth / 2;
        const clickY = intersectY / scale + glassHeight / 2;
        
        if (clickX >= 0 && clickX <= glassWidth && clickY >= 0 && clickY <= glassHeight) {
          placeCutoutAt(clickX, clickY);
          setIsPlacementMode(false);
          isPlacementModeRef.current = false;
        }
      }
      return;
    }

    if (pick.hit && pick.pickedMesh?.metadata?.handleType) {
      isDraggingRef.current = true;
      dragTypeRef.current = pick.pickedMesh.metadata.handleType;
      dragDataRef.current = { cutoutId: pick.pickedMesh.metadata.cutoutId, handlePos: pick.pickedMesh.metadata.position };
      if (cameraRef.current) cameraRef.current.detachControl();
      return;
    }

    if (pick.hit && pick.pickedMesh?.metadata?.cutoutId) {
      isDraggingRef.current = true;
      dragTypeRef.current = 'move';
      dragDataRef.current = { cutoutId: pick.pickedMesh.metadata.cutoutId };
      setSelectedCutoutId(pick.pickedMesh.metadata.cutoutId);
      selectedCutoutIdRef.current = pick.pickedMesh.metadata.cutoutId;
      if (cameraRef.current) cameraRef.current.detachControl();
      return;
    }

    setSelectedCutoutId(null);
    selectedCutoutIdRef.current = null;
  };

  const handlePointerMove = (scene) => {
    if (!isDraggingRef.current || !dragDataRef.current) return;

    const deltaX = scene.pointerX - lastPointerRef.current.x;
    const deltaY = scene.pointerY - lastPointerRef.current.y;
    lastPointerRef.current = { x: scene.pointerX, y: scene.pointerY };

    if (dragTypeRef.current === 'move') moveCutoutSmooth(dragDataRef.current.cutoutId, deltaX, deltaY);
    else if (dragTypeRef.current === 'resize') resizeCutoutSmooth(dragDataRef.current.cutoutId, deltaX, deltaY, dragDataRef.current.handlePos);
    else if (dragTypeRef.current === 'rotate') rotateCutoutSmooth(dragDataRef.current.cutoutId, deltaX);
  };

  const handlePointerUp = () => {
    if (isDraggingRef.current) {
      isDraggingRef.current = false;
      dragTypeRef.current = null;
      dragDataRef.current = null;
      if (cameraRef.current && canvasRef.current) cameraRef.current.attachControl(canvasRef.current, true);
    }
  };

  // Smooth cutout movement
  const moveCutoutSmooth = (cutoutId, deltaX, deltaY) => {
    const cam = cameraRef.current;
    if (!cam) return;
    
    const orthoWidth = cam.orthoRight - cam.orthoLeft;
    const canvasWidth = canvasRef.current?.width || 800;
    const pixelToWorld = orthoWidth / canvasWidth;
    const scale = getScale();
    
    const mmDeltaX = (deltaX * pixelToWorld) / scale;
    const mmDeltaY = (-deltaY * pixelToWorld) / scale;
    
    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      
      return {
        ...item,
        cutouts: item.cutouts.map(c => {
          if (c.id !== cutoutId) return c;
          
          const bounds = getCutoutBounds(c);
          const minX = bounds.halfWidth + 5;
          const maxX = item.width_mm - bounds.halfWidth - 5;
          const minY = bounds.halfHeight + 5;
          const maxY = item.height_mm - bounds.halfHeight - 5;
          
          let newX = c.x + mmDeltaX;
          let newY = c.y + mmDeltaY;
          
          if (snapEnabledRef.current) {
            newX = Math.round(newX / snapGridSizeRef.current) * snapGridSizeRef.current;
            newY = Math.round(newY / snapGridSizeRef.current) * snapGridSizeRef.current;
          }
          
          return { ...c, x: Math.max(minX, Math.min(maxX, newX)), y: Math.max(minY, Math.min(maxY, newY)) };
        })
      };
    }));
  };

  // Smooth cutout resize
  const resizeCutoutSmooth = (cutoutId, deltaX, deltaY, handlePos) => {
    const cam = cameraRef.current;
    if (!cam) return;
    
    const orthoWidth = cam.orthoRight - cam.orthoLeft;
    const canvasWidth = canvasRef.current?.width || 800;
    const pixelToWorld = orthoWidth / canvasWidth;
    const scale = getScale();
    
    const mmDeltaX = (deltaX * pixelToWorld) / scale;
    const mmDeltaY = (-deltaY * pixelToWorld) / scale;
    
    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      
      return {
        ...item,
        cutouts: item.cutouts.map(c => {
          if (c.id !== cutoutId) return c;
          
          const isCircular = ['SH', 'HX', 'HR'].includes(c.type);
          
          if (isCircular) {
            const delta = (Math.abs(mmDeltaX) > Math.abs(mmDeltaY) ? mmDeltaX : mmDeltaY) * 
                         (handlePos?.includes('r') || handlePos?.includes('t') ? 1 : -1);
            const newDiameter = Math.max(20, Math.min(300, (c.diameter || 50) + delta * 2));
            return { ...c, diameter: newDiameter };
          } else {
            let newWidth = c.width || 100;
            let newHeight = c.height || 80;
            
            if (handlePos?.includes('r')) newWidth += mmDeltaX * 2;
            if (handlePos?.includes('l')) newWidth -= mmDeltaX * 2;
            if (handlePos?.includes('t')) newHeight += mmDeltaY * 2;
            if (handlePos?.includes('b')) newHeight -= mmDeltaY * 2;
            
            return { ...c, width: Math.max(30, Math.min(400, newWidth)), height: Math.max(30, Math.min(400, newHeight)) };
          }
        })
      };
    }));
  };

  // Smooth cutout rotation
  const rotateCutoutSmooth = (cutoutId, deltaX) => {
    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      return {
        ...item,
        cutouts: item.cutouts.map(c => {
          if (c.id !== cutoutId) return c;
          return { ...c, rotation: ((c.rotation || 0) + deltaX * 0.5) % 360 };
        })
      };
    }));
  };

  // Update cutout dimensions
  const updateCutoutDimension = (cutoutId, field, value) => {
    const numValue = parseFloat(value) || 0;
    const clampedValue = Math.max(20, Math.min(400, numValue));
    
    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      return { ...item, cutouts: item.cutouts.map(c => c.id !== cutoutId ? c : { ...c, [field]: clampedValue }) };
    }));
  };

  // Update cutout position
  const updateCutoutPosition = (cutoutId, field, value) => {
    const numValue = parseFloat(value) || 0;
    
    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      
      return {
        ...item,
        cutouts: item.cutouts.map(c => {
          if (c.id !== cutoutId) return c;
          
          const bounds = getCutoutBounds(c);
          let clampedValue = numValue;
          
          if (field === 'x') clampedValue = Math.max(bounds.halfWidth + 5, Math.min(item.width_mm - bounds.halfWidth - 5, numValue));
          else if (field === 'y') clampedValue = Math.max(bounds.halfHeight + 5, Math.min(item.height_mm - bounds.halfHeight - 5, numValue));
          
          return { ...c, [field]: clampedValue };
        })
      };
    }));
  };

  // Place new cutout
  const placeCutoutAt = (x, y) => {
    const shapeConfig = CUTOUT_SHAPES.find(s => s.id === selectedCutoutTypeRef.current);
    if (!shapeConfig) return;

    const finalX = snapEnabled ? Math.round(x / snapGridSize) * snapGridSize : x;
    const finalY = snapEnabled ? Math.round(y / snapGridSize) * snapGridSize : y;

    const newCutout = {
      id: `cutout_${Date.now()}`,
      type: selectedCutoutTypeRef.current,
      x: Math.max(50, Math.min(configRef.current.width_mm - 50, finalX)),
      y: Math.max(50, Math.min(configRef.current.height_mm - 50, finalY)),
      rotation: 0,
      ...shapeConfig.defaultSize
    };

    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      return { ...item, cutouts: [...item.cutouts, newCutout] };
    }));

    setSelectedCutoutId(newCutout.id);
    selectedCutoutIdRef.current = newCutout.id;
    toast.success(`${shapeConfig.name} placed!`);
  };

  // Duplicate cutout
  const duplicateCutout = (cutoutId) => {
    const cutout = config.cutouts.find(c => c.id === cutoutId);
    if (!cutout) return;

    const offset = 30;
    const newCutout = {
      ...cutout,
      id: `cutout_${Date.now()}`,
      x: Math.min(config.width_mm - 50, cutout.x + offset),
      y: Math.min(config.height_mm - 50, cutout.y + offset),
    };

    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      return { ...item, cutouts: [...item.cutouts, newCutout] };
    }));

    setSelectedCutoutId(newCutout.id);
    selectedCutoutIdRef.current = newCutout.id;
    toast.success('Cutout duplicated!');
  };

  // Place from template
  const placeFromTemplate = (template) => {
    const newCutout = {
      id: `cutout_${Date.now()}`,
      type: template.type,
      x: config.width_mm / 2,
      y: config.height_mm / 2,
      rotation: 0,
      diameter: template.diameter,
      width: template.width,
      height: template.height,
    };

    setJobItems(prev => prev.map((item, idx) => {
      if (idx !== activeItemIndexRef.current) return item;
      return { ...item, cutouts: [...item.cutouts, newCutout] };
    }));

    setSelectedCutoutId(newCutout.id);
    selectedCutoutIdRef.current = newCutout.id;
    setShowTemplates(false);
    toast.success(`${template.name} added!`);
  };

  // Apply preset size
  const applyPresetSize = (size) => {
    if (!selectedCutoutId) return;
    const cutout = config.cutouts.find(c => c.id === selectedCutoutId);
    if (!cutout) return;

    if (['SH', 'HX', 'HR'].includes(cutout.type)) {
      updateCutoutDimension(selectedCutoutId, 'diameter', size);
    } else {
      updateCutoutDimension(selectedCutoutId, 'width', size);
    }
    toast.success(`Size set to ${size}mm`);
  };

  // Remove cutout
  const removeCutout = (cutoutId) => {
    updateCurrentItem({ cutouts: config.cutouts.filter(c => c.id !== cutoutId) });
    setSelectedCutoutId(null);
    toast.success('Cutout removed');
  };

  // Zoom controls
  const handleZoom = (direction) => {
    if (!cameraRef.current) return;
    const cam = cameraRef.current;
    const factor = direction === 'in' ? 0.85 : 1.18;
    cam.orthoLeft *= factor;
    cam.orthoRight *= factor;
    cam.orthoTop *= factor;
    cam.orthoBottom *= factor;
  };

  const resetView = () => {
    if (!cameraRef.current) return;
    const cam = cameraRef.current;
    cam.orthoLeft = -350;
    cam.orthoRight = 350;
    cam.orthoTop = 280;
    cam.orthoBottom = -280;
  };

  // Calculate prices
  const calculateAllPrices = useCallback(async () => {
    if (!labourRates) return;
    setCalculatingPrice(true);
    
    try {
      let total = 0;
      const updatedItems = jobItems.map(item => {
        const thickness = item.thickness_mm.toString();
        const rate = labourRates.rates[thickness] || 15;
        
        const areaSqMm = item.width_mm * item.height_mm;
        const areaSqFt = areaSqMm / 92903.04;
        
        let labourCharge = rate * areaSqFt * item.quantity;
        
        let holesCharge = 0;
        item.cutouts.forEach(cutout => {
          const shape = ['SH', 'HX', 'HR'].includes(cutout.type) ? 'circle' : 'rectangle';
          const holeRate = labourRates.hole_rates[shape] || labourRates.hole_rates.circle;
          const size = cutout.diameter || Math.max(cutout.width || 0, cutout.height || 0);
          holesCharge += holeRate.base + (holeRate.per_mm * size);
        });
        
        let transportCharge = 0;
        if (needsPickup && pickupDistance > 0) {
          transportCharge += labourRates.transport_pickup_base + (labourRates.transport_per_km * pickupDistance);
        }
        if (needsDelivery && deliveryDistance > 0) {
          transportCharge += labourRates.transport_drop_base + (labourRates.transport_per_km * deliveryDistance);
        }
        
        const subtotal = labourCharge + holesCharge + transportCharge;
        const gstAmount = subtotal * 0.18;
        const grandTotal = subtotal + gstAmount;
        
        total += grandTotal;
        return { ...item, item_price: grandTotal };
      });
      
      setJobItems(updatedItems);
      setOrderTotal(total);
    } catch (error) {
      console.error('Price calculation failed:', error);
    } finally {
      setCalculatingPrice(false);
    }
  }, [jobItems, labourRates, needsPickup, needsDelivery, pickupDistance, deliveryDistance]);

  // Calculate price on config change
  useEffect(() => {
    const timer = setTimeout(() => {
      if (labourRates) calculateAllPrices();
    }, 500);
    return () => clearTimeout(timer);
  }, [config.job_work_type, config.thickness_mm, config.width_mm, config.height_mm, config.quantity, config.cutouts.length, labourRates, needsPickup, needsDelivery, pickupDistance, deliveryDistance]);

  // Add job item
  const addJobItem = () => {
    const newId = jobItems.length + 1;
    setJobItems(prev => [...prev, createDefaultJobItem(newId)]);
    setActiveItemIndex(jobItems.length);
    setSelectedCutoutId(null);
    toast.success(`Glass ${newId} added`);
  };

  // Enter placement mode
  const enterPlacementMode = (shapeType) => {
    setSelectedCutoutType(shapeType);
    selectedCutoutTypeRef.current = shapeType;
    setIsPlacementMode(true);
    isPlacementModeRef.current = true;
    setSelectedCutoutId(null);
    setOpenDropdown(null);
    toast.info(`Click on glass to place ${CUTOUT_SHAPES.find(s => s.id === shapeType)?.name}`);
  };

  // Dropdown component
  const Dropdown = ({ label, value, options, onChange, renderOption }) => {
    const isOpen = openDropdown === label;
    return (
      <div className="relative">
        <button
          onClick={() => setOpenDropdown(isOpen ? null : label)}
          className="w-full flex items-center justify-between px-3 py-2 bg-white border rounded-lg text-sm hover:bg-gray-50"
        >
          <span className="truncate">{value}</span>
          <ChevronDown className={`w-4 h-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
        </button>
        {isOpen && (
          <div className="absolute z-50 w-full mt-1 bg-white border rounded-lg shadow-lg max-h-48 overflow-y-auto">
            {options.map((opt, idx) => (
              <button
                key={idx}
                onClick={() => { onChange(opt); setOpenDropdown(null); }}
                className="w-full px-3 py-2 text-left text-sm hover:bg-orange-50 flex items-center gap-2"
              >
                {renderOption ? renderOption(opt) : opt.label || opt.name || opt}
              </button>
            ))}
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <Loader2 className="w-8 h-8 animate-spin text-orange-600" />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50" onClick={() => setOpenDropdown(null)}>
      {/* Header */}
      <div className="bg-gradient-to-r from-orange-600 to-amber-600 text-white">
        <div className="max-w-7xl mx-auto px-4 py-3">
          <div className="flex items-center gap-2">
            <Wrench className="w-5 h-5" />
            <h1 className="text-xl font-bold">Job Work Configurator</h1>
          </div>
          <p className="text-xs text-orange-100 mt-0.5">Toughening, Lamination & Processing Services</p>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-4">
        <div className="grid lg:grid-cols-3 gap-4">
          {/* Left Panel - Configuration */}
          <div className="lg:col-span-1 space-y-3">
            {/* Job Work Config */}
            <div className="bg-white rounded-xl shadow-sm p-3 space-y-3" onClick={e => e.stopPropagation()}>
              <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2">
                <Wrench className="w-4 h-4 text-orange-600" />
                Job Work Configuration
              </h3>
              
              {/* Job Type */}
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Job Work Type</label>
                <Dropdown
                  label="job_type"
                  value={JOB_WORK_TYPES.find(j => j.id === config.job_work_type)?.name || 'Select'}
                  options={JOB_WORK_TYPES}
                  onChange={(opt) => updateCurrentItem({ job_work_type: opt.id })}
                  renderOption={(opt) => (
                    <div>
                      <div className="font-medium">{opt.name}</div>
                      <div className="text-[10px] text-gray-500">{opt.description}</div>
                    </div>
                  )}
                />
              </div>

              {/* Thickness */}
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Thickness (mm)</label>
                <Dropdown
                  label="thickness"
                  value={`${config.thickness_mm}mm`}
                  options={Object.keys(labourRates?.rates || {}).map(t => ({ thickness: parseInt(t), rate: labourRates.rates[t] }))}
                  onChange={(opt) => updateCurrentItem({ thickness_mm: opt.thickness })}
                  renderOption={(opt) => (
                    <div className="flex justify-between w-full">
                      <span>{opt.thickness}mm</span>
                      <span className="text-gray-500">₹{opt.rate}/sq.ft</span>
                    </div>
                  )}
                />
              </div>

              {/* Size */}
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="text-xs text-gray-500 mb-1 block">Width (mm)</label>
                  <input
                    type="number"
                    value={config.width_mm}
                    onChange={(e) => updateCurrentItem({ width_mm: parseFloat(e.target.value) || 0 })}
                    className="w-full h-9 rounded-lg border px-2 text-sm"
                    data-testid="width-input"
                  />
                </div>
                <div>
                  <label className="text-xs text-gray-500 mb-1 block">Height (mm)</label>
                  <input
                    type="number"
                    value={config.height_mm}
                    onChange={(e) => updateCurrentItem({ height_mm: parseFloat(e.target.value) || 0 })}
                    className="w-full h-9 rounded-lg border px-2 text-sm"
                    data-testid="height-input"
                  />
                </div>
              </div>
            </div>

            {/* Cutouts */}
            <div className="bg-white rounded-xl shadow-sm p-3" onClick={e => e.stopPropagation()}>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">
                Add Cutouts
                {isPlacementMode && <span className="ml-2 text-xs text-orange-600 animate-pulse">(Click on glass)</span>}
              </h3>
              
              <div className="grid grid-cols-5 gap-1 mb-2">
                {CUTOUT_SHAPES.map(shape => {
                  const Icon = shape.icon;
                  return (
                    <button
                      key={shape.id}
                      onClick={() => enterPlacementMode(shape.id)}
                      className={`p-2 rounded-lg text-xs flex flex-col items-center gap-0.5 transition-all ${
                        isPlacementMode && selectedCutoutType === shape.id
                          ? 'bg-orange-600 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                      data-testid={`shape-${shape.id}`}
                      title={shape.name}
                    >
                      <Icon className="w-4 h-4" />
                      <span className="text-[10px]">{shape.name}</span>
                    </button>
                  );
                })}
              </div>

              {isPlacementMode && (
                <Button
                  onClick={() => { setIsPlacementMode(false); isPlacementModeRef.current = false; }}
                  variant="outline"
                  size="sm"
                  className="w-full h-8 text-xs"
                >
                  Cancel
                </Button>
              )}

              {/* Selected cutout info */}
              {selectedCutout && !isPlacementMode && (
                <div className="mt-2 p-2 bg-orange-50 rounded-lg border border-orange-200 text-xs">
                  <div className="flex justify-between items-center mb-2">
                    <span className="font-semibold text-orange-800">
                      {CUTOUT_SHAPES.find(s => s.id === selectedCutout.type)?.name} Selected
                    </span>
                    <div className="flex gap-1">
                      <button onClick={() => duplicateCutout(selectedCutout.id)} className="text-orange-600 hover:text-orange-800 p-1 rounded hover:bg-orange-100" title="Duplicate">
                        <Copy className="w-3 h-3" />
                      </button>
                      <button onClick={() => removeCutout(selectedCutout.id)} className="text-red-500 hover:text-red-700 p-1 rounded hover:bg-red-100" title="Remove">
                        <Trash2 className="w-3 h-3" />
                      </button>
                    </div>
                  </div>
                  
                  {/* Preset Size Buttons */}
                  {['SH', 'HX', 'HR'].includes(selectedCutout.type) && (
                    <div className="mb-2">
                      <p className="text-[10px] font-medium text-gray-600 uppercase mb-1">Quick Sizes (mm)</p>
                      <div className="flex flex-wrap gap-1">
                        {PRESET_HOLE_SIZES.map(size => (
                          <button
                            key={size}
                            onClick={() => applyPresetSize(size)}
                            className={`px-2 py-1 rounded text-[10px] transition-all ${
                              Math.round(selectedCutout.diameter) === size ? 'bg-orange-600 text-white' : 'bg-white border hover:bg-gray-100'
                            }`}
                          >
                            Ø{size}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  
                  {/* Dimension Input Fields */}
                  <div className="space-y-2 mb-2">
                    <p className="text-[10px] font-medium text-gray-600 uppercase">Dimensions (mm)</p>
                    
                    {['SH', 'HX', 'HR'].includes(selectedCutout.type) ? (
                      <div className="flex items-center gap-2">
                        <label className="text-gray-500 w-12">Ø</label>
                        <input
                          type="number"
                          value={Math.round(selectedCutout.diameter || 50)}
                          onChange={(e) => updateCutoutDimension(selectedCutout.id, 'diameter', e.target.value)}
                          className="flex-1 h-7 px-2 rounded border text-xs text-center"
                          min="20" max="400"
                          data-testid="cutout-diameter-input"
                        />
                        <span className="text-gray-400 text-[10px]">mm</span>
                      </div>
                    ) : (
                      <div className="grid grid-cols-2 gap-2">
                        <div className="flex items-center gap-1">
                          <label className="text-gray-500 text-[10px]">W</label>
                          <input
                            type="number"
                            value={Math.round(selectedCutout.width || 100)}
                            onChange={(e) => updateCutoutDimension(selectedCutout.id, 'width', e.target.value)}
                            className="flex-1 h-7 px-2 rounded border text-xs text-center"
                            min="30" max="400"
                            data-testid="cutout-width-input"
                          />
                        </div>
                        <div className="flex items-center gap-1">
                          <label className="text-gray-500 text-[10px]">H</label>
                          <input
                            type="number"
                            value={Math.round(selectedCutout.height || 80)}
                            onChange={(e) => updateCutoutDimension(selectedCutout.id, 'height', e.target.value)}
                            className="flex-1 h-7 px-2 rounded border text-xs text-center"
                            min="30" max="400"
                            data-testid="cutout-height-input"
                          />
                        </div>
                      </div>
                    )}
                    
                    {/* Position Input Fields */}
                    <p className="text-[10px] font-medium text-gray-600 uppercase mt-2">Position (mm from bottom-left)</p>
                    <div className="grid grid-cols-2 gap-2">
                      <div className="flex items-center gap-1">
                        <label className="text-gray-500 text-[10px]">X</label>
                        <input
                          type="number"
                          value={Math.round(selectedCutout.x || 0)}
                          onChange={(e) => updateCutoutPosition(selectedCutout.id, 'x', e.target.value)}
                          className="flex-1 h-7 px-2 rounded border text-xs text-center"
                          data-testid="cutout-x-input"
                        />
                      </div>
                      <div className="flex items-center gap-1">
                        <label className="text-gray-500 text-[10px]">Y</label>
                        <input
                          type="number"
                          value={Math.round(selectedCutout.y || 0)}
                          onChange={(e) => updateCutoutPosition(selectedCutout.id, 'y', e.target.value)}
                          className="flex-1 h-7 px-2 rounded border text-xs text-center"
                          data-testid="cutout-y-input"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* Snap to Grid & Templates */}
              <div className="mt-2 flex gap-2">
                <button
                  onClick={() => setSnapEnabled(!snapEnabled)}
                  className={`flex-1 flex items-center justify-center gap-1 px-2 py-1.5 rounded text-[10px] transition-all ${
                    snapEnabled ? 'bg-green-100 text-green-700 border border-green-300' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                  title="Snap to Grid"
                >
                  <Magnet className="w-3 h-3" />
                  Snap {snapEnabled ? 'ON' : 'OFF'}
                </button>
                <button
                  onClick={() => setShowTemplates(!showTemplates)}
                  className={`flex-1 flex items-center justify-center gap-1 px-2 py-1.5 rounded text-[10px] transition-all ${
                    showTemplates ? 'bg-purple-100 text-purple-700 border border-purple-300' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                  title="Templates"
                >
                  <LayoutTemplate className="w-3 h-3" />
                  Templates
                </button>
              </div>

              {/* Snap Grid Size Selector */}
              {snapEnabled && (
                <div className="mt-2 p-2 bg-green-50 rounded-lg border border-green-200">
                  <p className="text-[10px] font-medium text-green-700 mb-1">Grid Size (mm)</p>
                  <div className="flex gap-1">
                    {SNAP_GRID_SIZES.map(size => (
                      <button
                        key={size}
                        onClick={() => setSnapGridSize(size)}
                        className={`flex-1 px-2 py-1 rounded text-[10px] transition-all ${
                          snapGridSize === size ? 'bg-green-600 text-white' : 'bg-white border hover:bg-green-100'
                        }`}
                      >
                        {size}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Templates Panel */}
              {showTemplates && (
                <div className="mt-2 p-2 bg-purple-50 rounded-lg border border-purple-200">
                  <p className="text-[10px] font-medium text-purple-700 mb-2">Quick Templates</p>
                  <div className="space-y-1 max-h-32 overflow-y-auto">
                    {CUTOUT_TEMPLATES.map(template => (
                      <button
                        key={template.id}
                        onClick={() => placeFromTemplate(template)}
                        className="w-full text-left p-2 bg-white rounded border hover:bg-purple-100 transition-all"
                      >
                        <div className="flex justify-between items-center">
                          <span className="text-[10px] font-medium text-gray-800">{template.name}</span>
                          <span className="text-[9px] text-gray-500">
                            {template.diameter ? `Ø${template.diameter}` : `${template.width}×${template.height}`}
                          </span>
                        </div>
                        <p className="text-[9px] text-gray-500">{template.description}</p>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Cutout list */}
              {config.cutouts.length > 0 && (
                <div className="mt-2 space-y-1">
                  {config.cutouts.map((c, idx) => (
                    <div 
                      key={c.id}
                      onClick={() => { setSelectedCutoutId(c.id); selectedCutoutIdRef.current = c.id; }}
                      className={`p-1.5 rounded text-xs cursor-pointer flex justify-between ${
                        selectedCutoutId === c.id ? 'bg-orange-100 border border-orange-300' : 'bg-gray-50 hover:bg-gray-100'
                      }`}
                    >
                      <span>{CUTOUT_SHAPES.find(s => s.id === c.type)?.name} #{idx + 1}</span>
                      <span className="text-gray-500">
                        {['SH', 'HX', 'HR'].includes(c.type) ? `Ø${Math.round(c.diameter)}` : `${Math.round(c.width)}×${Math.round(c.height)}`}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Transport Options */}
            <div className="bg-white rounded-xl shadow-sm p-3" onClick={e => e.stopPropagation()}>
              <h3 className="text-sm font-semibold text-gray-700 mb-2 flex items-center gap-2">
                <Truck className="w-4 h-4 text-orange-600" />
                Transport Options
              </h3>
              
              <div className="space-y-2">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={needsPickup}
                    onChange={(e) => setNeedsPickup(e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                  />
                  <span className="text-sm">Need Pickup</span>
                </label>
                
                {needsPickup && (
                  <div className="pl-6">
                    <label className="text-xs text-gray-500 mb-1 block">Distance (km)</label>
                    <input
                      type="number"
                      value={pickupDistance}
                      onChange={(e) => setPickupDistance(parseFloat(e.target.value) || 0)}
                      className="w-full h-8 rounded-lg border px-2 text-sm"
                      placeholder="Enter distance"
                    />
                  </div>
                )}
                
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={needsDelivery}
                    onChange={(e) => setNeedsDelivery(e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                  />
                  <span className="text-sm">Need Delivery</span>
                </label>
                
                {needsDelivery && (
                  <div className="pl-6">
                    <label className="text-xs text-gray-500 mb-1 block">Distance (km)</label>
                    <input
                      type="number"
                      value={deliveryDistance}
                      onChange={(e) => setDeliveryDistance(parseFloat(e.target.value) || 0)}
                      className="w-full h-8 rounded-lg border px-2 text-sm"
                      placeholder="Enter distance"
                    />
                  </div>
                )}
              </div>
            </div>

            {/* Quantity */}
            <div className="bg-white rounded-xl shadow-sm p-3">
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Quantity</h3>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => updateCurrentItem({ quantity: Math.max(1, config.quantity - 1) })}
                  className="w-8 h-8 rounded-lg bg-gray-100 flex items-center justify-center hover:bg-gray-200"
                >
                  <Minus className="w-3 h-3" />
                </button>
                <input
                  type="number"
                  value={config.quantity}
                  onChange={(e) => updateCurrentItem({ quantity: Math.max(1, parseInt(e.target.value) || 1) })}
                  className="w-14 h-8 rounded-lg border text-center text-sm"
                />
                <button
                  onClick={() => updateCurrentItem({ quantity: config.quantity + 1 })}
                  className="w-8 h-8 rounded-lg bg-gray-100 flex items-center justify-center hover:bg-gray-200"
                >
                  <Plus className="w-3 h-3" />
                </button>
              </div>
            </div>

            {/* Add Another Glass */}
            <button
              onClick={addJobItem}
              className="w-full py-2 border-2 border-dashed border-orange-300 rounded-xl text-orange-600 text-sm font-medium hover:border-orange-400 hover:bg-orange-50 flex items-center justify-center gap-2"
              data-testid="add-job-item-btn"
            >
              <Plus className="w-4 h-4" /> ADD ANOTHER GLASS
            </button>

            {/* Actions */}
            <div className="space-y-2">
              <Button className="w-full h-10 bg-orange-600 hover:bg-orange-700 text-white font-medium rounded-xl text-sm">
                <Calculator className="w-4 h-4 mr-2" /> GET QUOTATION
              </Button>
            </div>
          </div>

          {/* Right Panel - 3D Preview */}
          <div className="lg:col-span-2">
            <div className="bg-white rounded-xl shadow-sm overflow-hidden">
              <div className="p-3 border-b bg-gray-50">
                <div className="flex justify-between items-center mb-2">
                  <div>
                    <h3 className="text-sm font-semibold text-gray-800">Live 3D Preview</h3>
                    <p className="text-[10px] text-gray-500">
                      {isPlacementMode ? '🎯 Click on glass to place shape' : 'Production-ready visualization'}
                    </p>
                  </div>
                  <div className="flex gap-1">
                    <button onClick={() => handleZoom('in')} className="p-1.5 bg-gray-200 rounded hover:bg-gray-300" data-testid="zoom-in-btn" title="Zoom In">
                      <ZoomIn className="w-4 h-4" />
                    </button>
                    <button onClick={() => handleZoom('out')} className="p-1.5 bg-gray-200 rounded hover:bg-gray-300" data-testid="zoom-out-btn" title="Zoom Out">
                      <ZoomOut className="w-4 h-4" />
                    </button>
                    <button onClick={resetView} className="p-1.5 bg-gray-200 rounded hover:bg-gray-300" title="Reset View">
                      <RotateCcw className="w-4 h-4" />
                    </button>
                  </div>
                </div>
                
                {/* Production Mode Controls */}
                <div className="flex flex-wrap gap-1 pt-2 border-t">
                  <button
                    onClick={() => setViewMode(viewMode === VIEW_MODES.HIGH_CONTRAST ? VIEW_MODES.NORMAL : VIEW_MODES.HIGH_CONTRAST)}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-all ${
                      viewMode === VIEW_MODES.HIGH_CONTRAST ? 'bg-gray-800 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                    title="High Contrast Mode"
                  >
                    <Eye className="w-3 h-3" />
                    {viewMode === VIEW_MODES.HIGH_CONTRAST ? 'B/W' : 'Color'}
                  </button>
                  
                  <button
                    onClick={() => setShowGrid(!showGrid)}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-all ${
                      showGrid ? 'bg-orange-600 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                    title="Grid Overlay"
                  >
                    <Grid3X3 className="w-3 h-3" />
                    Grid
                  </button>
                  
                  <button
                    onClick={() => setShowCenterMarks(!showCenterMarks)}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-all ${
                      showCenterMarks ? 'bg-red-600 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                    title="Center Marks"
                  >
                    <Crosshair className="w-3 h-3" />
                    Centers
                  </button>
                  
                  <button
                    onClick={() => setShowDimensionLines(!showDimensionLines)}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-all ${
                      showDimensionLines ? 'bg-green-600 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                    title="Dimension Lines"
                  >
                    <Ruler className="w-3 h-3" />
                    Dims
                  </button>
                  
                  <button
                    onClick={() => setShowCutoutNumbers(!showCutoutNumbers)}
                    className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] transition-all ${
                      showCutoutNumbers ? 'bg-purple-600 text-white' : 'bg-gray-100 hover:bg-gray-200'
                    }`}
                    title="Cutout Numbers"
                  >
                    <FileText className="w-3 h-3" />
                    Labels
                  </button>
                  
                  <button
                    onClick={exportToPDF}
                    disabled={exportingPDF || config.cutouts.length === 0}
                    className="flex items-center gap-1 px-2 py-1 rounded text-[10px] bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
                    title="Export PDF Specification"
                  >
                    {exportingPDF ? <Loader2 className="w-3 h-3 animate-spin" /> : <Download className="w-3 h-3" />}
                    PDF
                  </button>
                  
                  {/* Share Button */}
                  <button
                    onClick={createShareLink}
                    disabled={creatingShareLink}
                    className="flex items-center gap-1 px-2 py-1 rounded text-[10px] bg-indigo-500 text-white hover:bg-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
                    title="Share Configuration"
                    data-testid="share-button"
                  >
                    {creatingShareLink ? <Loader2 className="w-3 h-3 animate-spin" /> : <Share2 className="w-3 h-3" />}
                    Share
                  </button>
                </div>
              </div>
              
              <div className="relative">
                <canvas
                  ref={canvasRef}
                  className={`w-full h-[450px] ${viewMode === VIEW_MODES.HIGH_CONTRAST ? 'bg-white' : ''}`}
                  data-testid="babylon-canvas"
                  style={{ touchAction: 'none', cursor: isPlacementMode ? 'crosshair' : 'default' }}
                />

                {isPlacementMode && (
                  <div className="absolute top-2 left-2 bg-orange-600 text-white px-3 py-1.5 rounded-lg text-xs flex items-center gap-2 animate-pulse">
                    <MousePointer className="w-3 h-3" />
                    Click to place {CUTOUT_SHAPES.find(s => s.id === selectedCutoutType)?.name}
                  </div>
                )}

                {selectedCutoutId && !isPlacementMode && (
                  <div className="absolute top-2 left-2 bg-orange-500 text-white px-2 py-1 rounded-lg text-xs flex items-center gap-1">
                    <GripVertical className="w-3 h-3" /> Editing: {CUTOUT_SHAPES.find(s => s.id === selectedCutout?.type)?.name}
                  </div>
                )}

                {jobItems.length > 1 && (
                  <div className="absolute top-2 right-2 bg-white rounded-lg shadow-lg p-2 max-w-[150px]">
                    <p className="text-[10px] font-semibold text-gray-600 mb-1">Items ({jobItems.length})</p>
                    {jobItems.map((item, idx) => (
                      <div 
                        key={item.id}
                        onClick={() => setActiveItemIndex(idx)}
                        className={`p-1 rounded cursor-pointer text-[10px] ${
                          idx === activeItemIndex ? 'bg-orange-50 border border-orange-200' : 'hover:bg-gray-50'
                        }`}
                      >
                        {item.name} - ₹{(item.item_price || 0).toLocaleString()}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Price Summary */}
            <div className="mt-3 bg-white rounded-xl shadow-sm p-3">
              <div className="flex justify-between items-center">
                <div>
                  <p className="text-xs text-gray-500">Job Work Total ({jobItems.length} item{jobItems.length > 1 ? 's' : ''})</p>
                  <p className="text-2xl font-bold text-orange-700">
                    {calculatingPrice ? <Loader2 className="w-5 h-5 animate-spin" /> : `₹${orderTotal.toLocaleString()}`}
                  </p>
                  <p className="text-[10px] text-gray-400">Including GST @ 18%</p>
                </div>
                <div className="text-right text-xs text-gray-600 space-y-0.5">
                  <p>Area: {((config.width_mm * config.height_mm) / 92903.04).toFixed(2)} sq.ft</p>
                  <p>Cutouts: {config.cutouts.length}</p>
                  <p>Qty: {config.quantity}</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Share Modal with QR Code */}
      {showShareModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4" onClick={() => setShowShareModal(false)}>
          <div className="bg-white rounded-2xl shadow-2xl max-w-lg w-full p-6" onClick={e => e.stopPropagation()}>
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-bold text-gray-800 flex items-center gap-2">
                <Share2 className="w-5 h-5 text-orange-600" />
                Share Job Work Configuration
              </h3>
              <button onClick={() => setShowShareModal(false)} className="p-1 hover:bg-gray-100 rounded-full">
                <X className="w-5 h-5 text-gray-500" />
              </button>
            </div>
            
            <div className="grid md:grid-cols-2 gap-4">
              {/* QR Code Section */}
              <div className="bg-gradient-to-br from-orange-50 to-amber-50 rounded-xl p-4 text-center">
                <div className="bg-white p-3 rounded-lg inline-block shadow-sm mb-3">
                  <QRCodeSVG 
                    value={shareUrl} 
                    size={140}
                    level="H"
                    includeMargin={true}
                    bgColor="#ffffff"
                    fgColor="#ea580c"
                  />
                </div>
                <p className="text-xs font-medium text-orange-700 flex items-center justify-center gap-1">
                  <QrCode className="w-3 h-3" />
                  Scan to view configuration
                </p>
                <button
                  onClick={() => {
                    const svg = document.querySelector('.fixed svg');
                    if (svg) {
                      const svgData = new XMLSerializer().serializeToString(svg);
                      const canvas = document.createElement('canvas');
                      const ctx = canvas.getContext('2d');
                      const img = new Image();
                      img.onload = () => {
                        canvas.width = img.width;
                        canvas.height = img.height;
                        ctx.fillStyle = 'white';
                        ctx.fillRect(0, 0, canvas.width, canvas.height);
                        ctx.drawImage(img, 0, 0);
                        const link = document.createElement('a');
                        link.download = `job_work_qr_${config.width_mm}x${config.height_mm}.png`;
                        link.href = canvas.toDataURL('image/png');
                        link.click();
                      };
                      img.src = 'data:image/svg+xml;base64,' + btoa(svgData);
                    }
                    toast.success('QR Code downloaded!');
                  }}
                  className="mt-2 px-3 py-1.5 bg-orange-600 hover:bg-orange-700 text-white text-xs font-medium rounded-lg flex items-center gap-1 mx-auto"
                >
                  <Download className="w-3 h-3" />
                  Download QR
                </button>
              </div>
              
              {/* Share Options */}
              <div className="space-y-3">
                <p className="text-sm text-gray-600">
                  Share via link, WhatsApp, or Email!
                </p>
                
                <div className="bg-gray-50 rounded-lg p-2">
                  <p className="text-[10px] text-gray-500 mb-1">Shareable Link (7 days)</p>
                  <div className="flex items-center gap-1">
                    <input
                      type="text"
                      value={shareUrl}
                      readOnly
                      className="flex-1 text-xs bg-white border rounded px-2 py-1 truncate"
                    />
                    <button
                      onClick={copyShareLink}
                      className="px-2 py-1 bg-gray-200 hover:bg-gray-300 rounded text-xs font-medium"
                    >
                      Copy
                    </button>
                  </div>
                </div>
                
                <button
                  onClick={shareViaWhatsApp}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-green-500 hover:bg-green-600 text-white rounded-lg font-medium transition-colors text-sm"
                >
                  <MessageCircle className="w-4 h-4" />
                  WhatsApp
                </button>
                
                <button
                  onClick={shareViaEmail}
                  className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-blue-500 hover:bg-blue-600 text-white rounded-lg font-medium transition-colors text-sm"
                >
                  <Mail className="w-4 h-4" />
                  Email
                </button>
              </div>
            </div>
            
            <p className="text-xs text-gray-400 mt-4 text-center border-t pt-3">
              {config.width_mm}×{config.height_mm}mm • {config.thickness_mm}mm • {config.cutouts.length} cutouts
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default JobWork3DConfigurator;
