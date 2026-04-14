# Pharmacy Management System - Class Diagram

## Overview
This document provides a comprehensive class diagram for the Pharmacy Management System, showing all classes, attributes, and functions.

---

## Database Layer (Models)

### 1. User (Base Class)
**Attributes:**
- `id: Integer` (Primary Key)
- `unique_id: String` (Unique Identifier)
- `username: String`
- `email: String` (Unique)
- `password: String` (Hashed)
- `role: String` (system_admin, doctor, pharmacist, pharmacy_admin, client)
- `status: String` (pending, approved, cancelled)
- `created_at: DateTime`

**Methods:**
- `create_user()`
- `update_status()`
- `authenticate()`

---

### 2. Doctor extends User
**Additional Attributes:**
- `doctor_id: Integer`
- `doctor_code: String` (e.g., DOC-0001)
- `specialization: String`
- `license_number: String`
- `phone: String`
- `address: String`

**Methods:**
- `write_prescription()`
- `view_patients()`
- `manage_appointments()`

---

### 3. Client extends User
**Additional Attributes:**
- `client_id: Integer`
- `client_code: String` (e.g., CLT-0001)
- `date_of_birth: Date`
- `phone: String`
- `address: String`
- `allergies: Text`

**Methods:**
- `upload_prescription()`
- `book_appointment()`
- `view_bills()`
- `make_payment()`

---

### 4. Pharmacist extends User
**Additional Attributes:**
- `pharmacist_id: Integer`
- `pharmacist_code: String` (e.g., PHR-0001)
- `pharmacy_id: Integer` (Foreign Key)
- `license_number: String`
- `phone: String`

**Methods:**
- `dispense_medicine()`
- `view_pending_queue()`
- `manage_inventory()`
- `generate_bill()`

---

### 5. PharmacyAdmin extends User
**Additional Attributes:**
- `admin_id: Integer`
- `admin_code: String` (e.g., ADM-0001)
- `pharmacy_id: Integer` (Foreign Key)
- `phone: String`

**Methods:**
- `view_dashboard()`
- `manage_staff()`
- `view_sales()`

---

### 6. Pharmacy
**Attributes:**
- `pharmacy_id: Integer` (Primary Key)
- `pharmacy_code: String`
- `name: String`
- `location: String`
- `contact_number: String`
- `license_number: String`
- `status: String` (active, inactive)

**Methods:**
- `add_pharmacy()`
- `update_pharmacy()`
- `delete_pharmacy()`

---

### 7. PharmacyMedicine
**Attributes:**
- `medicine_id: Integer` (Primary Key)
- `name: String`
- `content: String` (Active ingredient)
- `brand_name: String`
- `is_active: Boolean`
- `created_at: DateTime`
- `updated_at: DateTime`

**Methods:**
- `add_medicine()`
- `update_stock()`
- `check_low_stock()`

---

### 8. Inventory
**Attributes:**
- `inventory_id: Integer` (Primary Key)
- `pharmacy_id: Integer` (Foreign Key)
- `medicine_id: Integer` (Foreign Key)
- `medicine_name: String`
- `quantity_available: Integer`
- `unit_price: Decimal`
- `min_stock_level: Integer`
- `is_active: Boolean`
- `expiry_date: Date`
- `created_at: DateTime`
- `updated_at: DateTime`

**Methods:**
- `add_stock()`
- `update_stock()`
- `remove_stock()`
- `check_availability()`

---

### 9. Prescription
**Attributes:**
- `prescription_id: Integer` (Primary Key)
- `doctor_id: Integer` (Foreign Key)
- `client_id: Integer` (Foreign Key)
- `document_path: String` (S3 path)
- `medicine_name: String`
- `quantity: Integer`
- `dosage_instructions: Text`
- `pharmacy_id: Integer` (Foreign Key)
- `routed_pharmacist_id: Integer` (Foreign Key)
- `status: String` (pending, routed, validated, dispensed)
- `created_date: DateTime`

**Methods:**
- `create_prescription()`
- `route_to_pharmacy()`
- `update_status()`
- `view_document()`

---

### 10. Appointment
**Attributes:**
- `appointment_id: Integer` (Primary Key)
- `client_id: Integer` (Foreign Key)
- `doctor_id: Integer` (Foreign Key)
- `appointment_date: Date`
- `appointment_time: Time`
- `reason: String`
- `symptoms: Text`
- `status: String` (pending, confirmed, cancelled, completed)
- `created_at: DateTime`

**Methods:**
- `book_appointment()`
- `confirm_appointment()`
- `cancel_appointment()`

---

### 11. PharmacyDispenseBill
**Attributes:**
- `bill_id: Integer` (Primary Key)
- `invoice_no: String`
- `prescription_id: Integer` (Foreign Key)
- `pharmacy_id: Integer` (Foreign Key)
- `pharmacist_id: Integer` (Foreign Key)
- `client_id: Integer` (Foreign Key)
- `requested_medicine: String`
- `dispensed_medicine: String`
- `dispensed_content: String`
- `quantity: Integer`
- `unit_price: Decimal`
- `subtotal_amount: Decimal`
- `gst_rate: Decimal`
- `gst_amount: Decimal`
- `total_amount: Decimal`
- `payment_status: String` (generated, paid)
- `payment_method: String` (upi, card, netbanking, cash)
- `paid_at: DateTime`
- `payment_notified: Boolean`
- `created_at: DateTime`

**Methods:**
- `generate_bill()`
- `process_payment()`
- `download_invoice()`

---

## Application Layer (Blueprints/Modules)

### 1. UserManagement
**Components:**
- `user_bp: Blueprint`

**Key Functions:**
- `generate_role_code()` - Generate unique codes for roles
- `generate_pharmacy_code()` - Generate pharmacy codes
- `get_client_id_from_session()` - Get client ID from session
- `get_doctor_id_from_session()` - Get doctor ID from session
- `get_pharmacist_context()` - Get pharmacist context
- `get_pharmacy_admin_context()` - Get pharmacy admin context
- `role_required()` - Decorator for role-based access
- `redirect_by_role()` - Redirect based on user role

**Route Handlers:**
- Authentication: `home()`, `register()`, `login()`, `logout()`
- System Admin: `system_admin_dashboard()`, `view_users()`, `approve_user()`, `reject_user()`, `delete_user()`, `all_users()`, `view_pharmacies()`, `add_pharmacy()`, `edit_pharmacy()`, `delete_pharmacy()`
- Doctor: `doctor_dashboard()`, `doctor_write_prescription()`, `doctor_medicine_suggest()`, `doctor_patients()`, `doctor_appointments()`, `confirm_appointment()`, `reject_appointment()`
- Pharmacist: `pharmacist_dashboard()`, `pharmacist_pending_queue()`, `pharmacist_dispensed()`, `pharmacist_billing()`, `add_pharmacist_medicine()`, `dispense_prescription()`
- Pharmacy Admin: `pharmacy_admin_dashboard()`
- Client: `client_dashboard()`, `client_upload_prescription()`, `client_book_appointment()`, `client_active_prescriptions()`, `client_view_prescription_document()`, `client_download_prescription()`, `client_send_prescription_to_pharmacy()`, `client_prescription_history()`, `client_orders()`, `client_billing_payments()`, `client_download_bill()`, `client_pay_bill()`, `client_profile()`

---

### 2. PrescriptionManagement
**Components:**
- `prescription_bp: Blueprint`

---

### 3. InventoryManagement
**Components:**
- `inventory_bp: Blueprint`

**Key Functions:**
- `ensure_medicine_inventory_tables()` - Create/ensure tables
- `seed_pharmacy_medicines()` - Seed default medicines
- `find_medicine_or_substitute()` - Find medicine or substitute

---

### 4. BillPayment
**Components:**
- `billing_bp: Blueprint`

**Key Functions:**
- `process_payment()` - Process payment for bills

---

### 6. CloudStorage
**Key Functions:**
- `_get_s3_client()` - Get AWS S3 client
- `upload_file()` - Upload file to S3
- `upload_prescription_document()` - Upload prescription to S3
- `_extract_object_key()` - Extract S3 object key
- `get_prescription_access_url()` - Get presigned URL

---

## Configuration & Infrastructure

### 1. Config
**Attributes:**
- `MYSQL_HOST: String`
- `MYSQL_USER: String`
- `MYSQL_PASSWORD: String`
- `MYSQL_DB: String`
- `MYSQL_PORT: Integer`
- `AWS_ACCESS_KEY_ID: String`
- `AWS_SECRET_ACCESS_KEY: String`
- `AWS_REGION: String`
- `S3_BUCKET: String`
- `S3_PRESCRIPTION_PREFIX: String`

---

### 2. Database
**Attributes:**
- `mysql: MySQL`

**Methods:**
- `get_connection()` - Get database connection

---

### 3. App (Flask Application)
**Components:**
- `app: Flask`

**Methods:**
- `register_blueprint()` - Register Flask blueprints
- `test_db()` - Test database connection

---

## Relationships Diagram

```
                    ┌─────────────────────────────────────────┐
                    │              User (Base)                 │
                    │  id, unique_id, username, email,         │
                    │  password, role, status, created_at     │
                    └──────────┬───────────────┬───────────────┘
                               │               │               │
          ┌────────────────────┘               │               │
          │                                     │               │
          ▼                                     ▼               ▼
    ┌─────────┐                           ┌─────────┐    ┌─────────────┐
    │ Doctor  │                           │ Client  │    │  Pharmacist │
    │-doctor_id                                │-client_id    │-pharmacist_id
    │-doctor_code                              │-client_code  │-pharmacy_id
    │-specialization                          │-dob          │-license_number
    │-license_number                          │-phone        └──────────────┘
    │-phone                                    │-address
    │-address                                  │-allergies
    └─────────┘                                └─────────────┘
                                                        │
                            ┌───────────────────────────┘
                            │
                            ▼
                   ┌────────────────┐
                   │ PharmacyAdmin  │
                   │-admin_id       │
                   │-admin_code     │
                   │-pharmacy_id    │
                   │-phone          │
                   └────────────────┘
```

```
┌─────────────┐       ┌──────────────────┐       ┌─────────────┐
│  Pharmacy   │       │ PharmacyMedicine │       │  Prescription│
│ -pharmacy_id│◄──────│ -medicine_id     │       │-prescription_id
│ -pharmacy_code      │ -pharmacy_id     │       │-doctor_id
│ -name        │       │ -name           │       │-client_id
│ -location    │       │ -content        │       │-pharmacy_id
│ -contact     │       │ -brand_name     │       │-status
│ -license     │       │ -stock_qty      │       │-medicine_name
└─────────────┘       │ -unit_price     │       └──────┬──────┘
                      │ -min_stock_level│              │
                      └─────────────────┘              │
                                                       │
              ┌────────────────────────────────────────┘
              │
              ▼
┌─────────────────────┐     ┌────────────────┐     ┌─────────────────────┐
│    Appointment      │     │PharmacyDispense│     │      Doctor         │
│ -appointment_id     │     │Bill            │     │     (See Above)     │
│ -client_id          │     │-bill_id        │     └─────────────────────┘
│ -doctor_id          │     │-prescription_id│
│ -appointment_date   │     │-pharmacy_id    │
│ -appointment_time   │     │-pharmacist_id  │
│ -status             │     │-total_amount   │
└─────────────────────┘     │-payment_status │     
                            └────────────────┘     
```

---

## How to View the PlantUML Diagram

### Option 1: Online Viewer
1. Copy the content from `class_diagram.puml`
2. Go to [PlantUML Online Server](https://www.plantuml.com/plantuml)
3. Paste the code and view the diagram

### Option 2: VSCode Extension
1. Install "PlantUML" extension in VSCode
2. Right-click on the `.puml` file
3. Select "Preview Current Diagram"

### Option 3: Generate Image
```bash
# Install PlantUML
# Then run:
java -jar plantuml.jar -tpng class_diagram.puml
```

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Total Classes | 16 |
| Database Models | 11 |
| Application Modules | 6 |
| Config Classes | 3 |
| Total Attributes | ~80+ |
| Total Methods | ~50+ |

---

## Relationships (Updated for Appointment, No Report Class)

1. `User` is the base class for `Doctor`, `Client`, `Pharmacist`, `PharmacyAdmin`.
1. `Pharmacist` (many) works at `Pharmacy` (1).
1. `PharmacyAdmin` (many) manages `Pharmacy` (1).
1. `Inventory` (many) belongs to `Pharmacy` (1).
1. `Inventory` (many) references `PharmacyMedicine` (1) as the catalog item.
1. `Prescription` (many) is written by `Doctor` (1).
1. `Prescription` (many) is for `Client` (1).
1. `Prescription` (many) may be routed to `Pharmacy` (0..1).
1. `Prescription` (many) may be assigned to `Pharmacist` (0..1).
1. `Appointment` (many) is booked by `Client` (1).
1. `Appointment` (many) is with `Doctor` (1).
1. `PharmacyDispenseBill` (many) is for `Prescription` (1).
1. `PharmacyDispenseBill` (many) is from `Pharmacy` (1).
1. `PharmacyDispenseBill` (many) is issued by `Pharmacist` (1).
1. `PharmacyDispenseBill` (many) may be for `Client` (0..1).

