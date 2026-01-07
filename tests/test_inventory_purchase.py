"""
Inventory and Purchase Module Tests for Glass Factory ERP System
Tests Inventory Management and Purchase Order workflows
"""
import pytest
import requests
import os
import uuid

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', 'https://glassmesh.preview.emergentagent.com').rstrip('/')


class TestInventoryMaterials:
    """Inventory Material Management Tests"""
    
    def test_get_materials(self):
        """Test fetching all materials"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/materials")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/inventory/materials - Found {len(data)} materials")
        return data
    
    def test_create_material(self):
        """Test creating a new material"""
        material_data = {
            "name": f"TEST_Material_{uuid.uuid4().hex[:6]}",
            "category": "glass",
            "unit": "sqft",
            "current_stock": 100,
            "minimum_stock": 50,
            "unit_price": 75.50,
            "location": "Warehouse A"
        }
        response = requests.post(f"{BASE_URL}/api/erp/inventory/materials", json=material_data)
        assert response.status_code == 200
        data = response.json()
        assert "material_id" in data
        assert data["message"] == "Material created"
        print(f"✓ POST /api/erp/inventory/materials - Created material {data['material_id']}")
        
        # Verify material was created by fetching it
        get_response = requests.get(f"{BASE_URL}/api/erp/inventory/materials/{data['material_id']}")
        assert get_response.status_code == 200
        created_material = get_response.json()
        assert created_material["name"] == material_data["name"]
        assert created_material["category"] == material_data["category"]
        assert created_material["current_stock"] == material_data["current_stock"]
        print(f"✓ Verified material creation via GET")
        return data["material_id"]
    
    def test_get_material_by_id(self):
        """Test fetching a single material by ID"""
        # First get all materials
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        if not materials:
            pytest.skip("No materials found")
        
        material_id = materials[0]["id"]
        response = requests.get(f"{BASE_URL}/api/erp/inventory/materials/{material_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == material_id
        print(f"✓ GET /api/erp/inventory/materials/{material_id} - Found material: {data['name']}")
    
    def test_get_material_not_found(self):
        """Test fetching non-existent material returns 404"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/materials/non-existent-id")
        assert response.status_code == 404
        print(f"✓ GET /api/erp/inventory/materials/non-existent-id - Returns 404 as expected")
    
    def test_update_material(self):
        """Test updating material details"""
        # Create a material first
        material_data = {
            "name": f"TEST_UpdateMaterial_{uuid.uuid4().hex[:6]}",
            "category": "chemical",
            "unit": "ltr",
            "current_stock": 50,
            "minimum_stock": 20,
            "unit_price": 120,
            "location": "Chemical Store"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/inventory/materials", json=material_data)
        material_id = create_response.json()["material_id"]
        
        # Update the material
        update_data = {
            "minimum_stock": 30,
            "unit_price": 150,
            "location": "New Location"
        }
        update_response = requests.patch(f"{BASE_URL}/api/erp/inventory/materials/{material_id}", json=update_data)
        assert update_response.status_code == 200
        assert update_response.json()["message"] == "Material updated"
        
        # Verify update
        get_response = requests.get(f"{BASE_URL}/api/erp/inventory/materials/{material_id}")
        updated_material = get_response.json()
        assert updated_material["minimum_stock"] == 30
        assert updated_material["unit_price"] == 150
        assert updated_material["location"] == "New Location"
        print(f"✓ PATCH /api/erp/inventory/materials/{material_id} - Material updated successfully")
    
    def test_get_materials_by_category(self):
        """Test filtering materials by category"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/materials", params={"category": "glass"})
        assert response.status_code == 200
        data = response.json()
        for material in data:
            assert material["category"] == "glass"
        print(f"✓ GET /api/erp/inventory/materials?category=glass - Found {len(data)} glass materials")
    
    def test_get_low_stock_items(self):
        """Test fetching low stock items"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/low-stock")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Verify all returned items are actually low stock
        for item in data:
            assert item["current_stock"] <= item["minimum_stock"]
        print(f"✓ GET /api/erp/inventory/low-stock - Found {len(data)} low stock items")


class TestInventoryTransactions:
    """Inventory Transaction Tests (Stock In/Out)"""
    
    def test_stock_in_transaction(self):
        """Test adding stock (Stock In)"""
        # Get a material
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        if not materials:
            pytest.skip("No materials found")
        
        material = materials[0]
        original_stock = material["current_stock"]
        
        txn_data = {
            "material_id": material["id"],
            "type": "IN",
            "quantity": 50,
            "reference": "TEST_REF_001",
            "notes": "Test stock in transaction"
        }
        response = requests.post(f"{BASE_URL}/api/erp/inventory/transactions", json=txn_data)
        assert response.status_code == 200
        data = response.json()
        assert "transaction_id" in data
        assert data["new_stock"] == original_stock + 50
        print(f"✓ POST /api/erp/inventory/transactions (IN) - Stock: {original_stock} → {data['new_stock']}")
        
        # Verify stock was updated
        updated_material = requests.get(f"{BASE_URL}/api/erp/inventory/materials/{material['id']}").json()
        assert updated_material["current_stock"] == original_stock + 50
        print(f"✓ Verified stock update in material record")
        return data["transaction_id"]
    
    def test_stock_out_transaction(self):
        """Test removing stock (Stock Out)"""
        # Get a material with sufficient stock
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        material = next((m for m in materials if m["current_stock"] >= 10), None)
        if not material:
            pytest.skip("No material with sufficient stock found")
        
        original_stock = material["current_stock"]
        
        txn_data = {
            "material_id": material["id"],
            "type": "OUT",
            "quantity": 10,
            "reference": "JC_TEST_001",
            "notes": "Test stock out for production"
        }
        response = requests.post(f"{BASE_URL}/api/erp/inventory/transactions", json=txn_data)
        assert response.status_code == 200
        data = response.json()
        assert data["new_stock"] == original_stock - 10
        print(f"✓ POST /api/erp/inventory/transactions (OUT) - Stock: {original_stock} → {data['new_stock']}")
    
    def test_stock_out_insufficient_stock(self):
        """Test stock out fails when insufficient stock"""
        # Create a material with low stock
        material_data = {
            "name": f"TEST_LowStock_{uuid.uuid4().hex[:6]}",
            "category": "spare",
            "unit": "pcs",
            "current_stock": 5,
            "minimum_stock": 10,
            "unit_price": 100,
            "location": "Spare Parts"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/inventory/materials", json=material_data)
        material_id = create_response.json()["material_id"]
        
        # Try to remove more than available
        txn_data = {
            "material_id": material_id,
            "type": "OUT",
            "quantity": 100,
            "notes": "Should fail - insufficient stock"
        }
        response = requests.post(f"{BASE_URL}/api/erp/inventory/transactions", json=txn_data)
        assert response.status_code == 400
        assert "Insufficient stock" in response.json()["detail"]
        print(f"✓ Stock out correctly rejected for insufficient stock")
    
    def test_get_transactions(self):
        """Test fetching all transactions"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/transactions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/inventory/transactions - Found {len(data)} transactions")
    
    def test_get_transactions_by_material(self):
        """Test filtering transactions by material"""
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        if not materials:
            pytest.skip("No materials found")
        
        material_id = materials[0]["id"]
        response = requests.get(f"{BASE_URL}/api/erp/inventory/transactions", params={"material_id": material_id})
        assert response.status_code == 200
        data = response.json()
        for txn in data:
            assert txn["material_id"] == material_id
        print(f"✓ GET /api/erp/inventory/transactions?material_id={material_id} - Found {len(data)} transactions")
    
    def test_get_transactions_by_type(self):
        """Test filtering transactions by type"""
        response = requests.get(f"{BASE_URL}/api/erp/inventory/transactions", params={"txn_type": "IN"})
        assert response.status_code == 200
        data = response.json()
        for txn in data:
            assert txn["type"] == "IN"
        print(f"✓ GET /api/erp/inventory/transactions?txn_type=IN - Found {len(data)} IN transactions")


class TestSupplierManagement:
    """Supplier Management Tests"""
    
    def test_get_suppliers(self):
        """Test fetching all suppliers"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/purchase/suppliers - Found {len(data)} suppliers")
        return data
    
    def test_create_supplier(self):
        """Test creating a new supplier"""
        supplier_data = {
            "name": f"TEST_Supplier_{uuid.uuid4().hex[:6]}",
            "contact_person": "John Doe",
            "email": f"test_{uuid.uuid4().hex[:6]}@supplier.com",
            "phone": "9876543210",
            "address": "123 Test Street, Test City",
            "gst_number": "27AABCU9603R1ZX",
            "payment_terms": "Net 30"
        }
        response = requests.post(f"{BASE_URL}/api/erp/purchase/suppliers", json=supplier_data)
        assert response.status_code == 200
        data = response.json()
        assert "supplier_id" in data
        assert data["message"] == "Supplier created"
        print(f"✓ POST /api/erp/purchase/suppliers - Created supplier {data['supplier_id']}")
        
        # Verify supplier was created
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        created_supplier = next((s for s in suppliers if s["id"] == data["supplier_id"]), None)
        assert created_supplier is not None
        assert created_supplier["name"] == supplier_data["name"]
        assert created_supplier["phone"] == supplier_data["phone"]
        print(f"✓ Verified supplier creation")
        return data["supplier_id"]


class TestPurchaseOrders:
    """Purchase Order Management Tests"""
    
    def test_get_purchase_orders(self):
        """Test fetching all purchase orders"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/orders")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        print(f"✓ GET /api/erp/purchase/orders - Found {len(data)} POs")
        return data
    
    def test_create_purchase_order(self):
        """Test creating a new purchase order"""
        # Get supplier and materials
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers:
            pytest.skip("No suppliers found")
        if not materials:
            pytest.skip("No materials found")
        
        supplier = suppliers[0]
        material = materials[0]
        
        po_data = {
            "supplier_id": supplier["id"],
            "supplier_name": supplier["name"],
            "items": [
                {
                    "material_id": material["id"],
                    "material_name": material["name"],
                    "quantity": 100,
                    "unit_price": material["unit_price"]
                }
            ],
            "expected_delivery": "2026-01-15",
            "notes": "Test purchase order"
        }
        response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        assert response.status_code == 200
        data = response.json()
        assert "po_number" in data
        assert "po_id" in data
        assert data["message"] == "Purchase order created"
        print(f"✓ POST /api/erp/purchase/orders - Created PO {data['po_number']}")
        
        # Verify PO was created
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{data['po_id']}").json()
        assert po["po_number"] == data["po_number"]
        assert po["status"] == "pending"
        assert len(po["items"]) == 1
        
        # Verify totals calculation (subtotal + 18% GST)
        expected_subtotal = 100 * material["unit_price"]
        expected_gst = expected_subtotal * 0.18
        expected_total = expected_subtotal + expected_gst
        assert po["subtotal"] == round(expected_subtotal, 2)
        assert po["gst"] == round(expected_gst, 2)
        assert po["total"] == round(expected_total, 2)
        print(f"✓ Verified PO totals: Subtotal={po['subtotal']}, GST={po['gst']}, Total={po['total']}")
        return data["po_id"]
    
    def test_create_po_with_multiple_items(self):
        """Test creating PO with multiple items"""
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers or len(materials) < 2:
            pytest.skip("Need at least 1 supplier and 2 materials")
        
        supplier = suppliers[0]
        
        po_data = {
            "supplier_id": supplier["id"],
            "supplier_name": supplier["name"],
            "items": [
                {
                    "material_id": materials[0]["id"],
                    "material_name": materials[0]["name"],
                    "quantity": 50,
                    "unit_price": materials[0]["unit_price"]
                },
                {
                    "material_id": materials[1]["id"],
                    "material_name": materials[1]["name"],
                    "quantity": 30,
                    "unit_price": materials[1]["unit_price"]
                }
            ],
            "expected_delivery": "2026-01-20",
            "notes": "Multi-item PO test"
        }
        response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        assert response.status_code == 200
        data = response.json()
        
        # Verify PO has multiple items
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{data['po_id']}").json()
        assert len(po["items"]) == 2
        print(f"✓ Created PO with {len(po['items'])} items, Total: ₹{po['total']}")
        return data["po_id"]
    
    def test_get_po_by_id(self):
        """Test fetching a single PO by ID"""
        # Create a PO first
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers or not materials:
            pytest.skip("Need suppliers and materials")
        
        po_data = {
            "supplier_id": suppliers[0]["id"],
            "supplier_name": suppliers[0]["name"],
            "items": [{"material_id": materials[0]["id"], "material_name": materials[0]["name"], "quantity": 10, "unit_price": 100}],
            "notes": "Test PO for GET"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        po_id = create_response.json()["po_id"]
        
        response = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == po_id
        print(f"✓ GET /api/erp/purchase/orders/{po_id} - Found PO: {data['po_number']}")
    
    def test_get_po_not_found(self):
        """Test fetching non-existent PO returns 404"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/orders/non-existent-id")
        assert response.status_code == 404
        print(f"✓ GET /api/erp/purchase/orders/non-existent-id - Returns 404 as expected")
    
    def test_get_pos_by_status(self):
        """Test filtering POs by status"""
        response = requests.get(f"{BASE_URL}/api/erp/purchase/orders", params={"status": "pending"})
        assert response.status_code == 200
        data = response.json()
        for po in data:
            assert po["status"] == "pending"
        print(f"✓ GET /api/erp/purchase/orders?status=pending - Found {len(data)} pending POs")


class TestPOStatusWorkflow:
    """Purchase Order Status Workflow Tests (Pending -> Approved -> Ordered -> Received)"""
    
    def test_po_status_workflow_complete(self):
        """Test complete PO workflow: Pending -> Approved -> Ordered -> Received with inventory update"""
        # Setup: Get supplier and material
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers or not materials:
            pytest.skip("Need suppliers and materials")
        
        supplier = suppliers[0]
        material = materials[0]
        original_stock = material["current_stock"]
        order_quantity = 200
        
        # Step 1: Create PO (status: pending)
        po_data = {
            "supplier_id": supplier["id"],
            "supplier_name": supplier["name"],
            "items": [
                {
                    "material_id": material["id"],
                    "material_name": material["name"],
                    "quantity": order_quantity,
                    "unit_price": material["unit_price"]
                }
            ],
            "expected_delivery": "2026-01-25",
            "notes": "Workflow test PO"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        assert create_response.status_code == 200
        po_id = create_response.json()["po_id"]
        po_number = create_response.json()["po_number"]
        print(f"✓ Step 1: Created PO {po_number} (status: pending)")
        
        # Verify initial status
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}").json()
        assert po["status"] == "pending"
        
        # Step 2: Approve PO
        approve_response = requests.patch(
            f"{BASE_URL}/api/erp/purchase/orders/{po_id}/status",
            json={"status": "approved"}
        )
        assert approve_response.status_code == 200
        assert "approved" in approve_response.json()["message"]
        
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}").json()
        assert po["status"] == "approved"
        print(f"✓ Step 2: PO approved (status: approved)")
        
        # Step 3: Mark as Ordered
        order_response = requests.patch(
            f"{BASE_URL}/api/erp/purchase/orders/{po_id}/status",
            json={"status": "ordered"}
        )
        assert order_response.status_code == 200
        
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}").json()
        assert po["status"] == "ordered"
        print(f"✓ Step 3: PO ordered (status: ordered)")
        
        # Step 4: Mark as Received - This should update inventory
        receive_response = requests.patch(
            f"{BASE_URL}/api/erp/purchase/orders/{po_id}/status",
            json={"status": "received"}
        )
        assert receive_response.status_code == 200
        
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}").json()
        assert po["status"] == "received"
        assert "received_at" in po
        print(f"✓ Step 4: PO received (status: received)")
        
        # Verify inventory was updated
        updated_material = requests.get(f"{BASE_URL}/api/erp/inventory/materials/{material['id']}").json()
        expected_stock = original_stock + order_quantity
        assert updated_material["current_stock"] == expected_stock
        print(f"✓ Inventory updated: {original_stock} → {updated_material['current_stock']} (+{order_quantity})")
        
        # Verify transaction was created
        transactions = requests.get(
            f"{BASE_URL}/api/erp/inventory/transactions",
            params={"material_id": material["id"]}
        ).json()
        po_transaction = next((t for t in transactions if t.get("reference") == po_number), None)
        assert po_transaction is not None
        assert po_transaction["type"] == "IN"
        assert po_transaction["quantity"] == order_quantity
        print(f"✓ Inventory transaction created with reference: {po_number}")
    
    def test_po_cancel(self):
        """Test cancelling a PO"""
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers or not materials:
            pytest.skip("Need suppliers and materials")
        
        po_data = {
            "supplier_id": suppliers[0]["id"],
            "supplier_name": suppliers[0]["name"],
            "items": [{"material_id": materials[0]["id"], "material_name": materials[0]["name"], "quantity": 10, "unit_price": 100}],
            "notes": "PO to cancel"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        po_id = create_response.json()["po_id"]
        
        # Cancel the PO
        cancel_response = requests.patch(
            f"{BASE_URL}/api/erp/purchase/orders/{po_id}/status",
            json={"status": "cancelled"}
        )
        assert cancel_response.status_code == 200
        
        po = requests.get(f"{BASE_URL}/api/erp/purchase/orders/{po_id}").json()
        assert po["status"] == "cancelled"
        print(f"✓ PO cancelled successfully")
    
    def test_invalid_status_update(self):
        """Test that invalid status returns error"""
        suppliers = requests.get(f"{BASE_URL}/api/erp/purchase/suppliers").json()
        materials = requests.get(f"{BASE_URL}/api/erp/inventory/materials").json()
        
        if not suppliers or not materials:
            pytest.skip("Need suppliers and materials")
        
        po_data = {
            "supplier_id": suppliers[0]["id"],
            "supplier_name": suppliers[0]["name"],
            "items": [{"material_id": materials[0]["id"], "material_name": materials[0]["name"], "quantity": 10, "unit_price": 100}],
            "notes": "Test invalid status"
        }
        create_response = requests.post(f"{BASE_URL}/api/erp/purchase/orders", json=po_data)
        po_id = create_response.json()["po_id"]
        
        # Try invalid status
        invalid_response = requests.patch(
            f"{BASE_URL}/api/erp/purchase/orders/{po_id}/status",
            json={"status": "invalid_status"}
        )
        assert invalid_response.status_code == 400
        assert "Invalid status" in invalid_response.json()["detail"]
        print(f"✓ Invalid status correctly rejected")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
