from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from database.db import mysql
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from modules.cloud_storage import upload_prescription_document, get_prescription_access_url
from modules import routing_engine
from io import BytesIO
from flask import send_file
from flask import Response
import re

user_bp = Blueprint('user', __name__)


# ══════════════════════════════════════════
# ID GENERATORS
# ══════════════════════════════════════════

def generate_role_code(role, cur):
    config = {
        'doctor':         ('doctors',         'DOC', 'doctor_code'),
        'client':         ('clients',         'CLT', 'client_code'),
        'pharmacist':     ('pharmacists',     'PHR', 'pharmacist_code'),
        'pharmacy_admin': ('pharmacy_admins', 'ADM', 'admin_code'),
    }
    if role not in config:
        return None
    table, prefix, code_column = config[role]
    
    # Get the maximum existing code for this role
    cur.execute(f"SELECT MAX({code_column}) FROM {table} WHERE {code_column} LIKE %s", (f"{prefix}-%",))
    max_code = cur.fetchone()[0]
    
    if max_code:
        # Extract the numeric part and increment
        try:
            last_num = int(max_code.split('-')[-1])
            new_num = last_num + 1
        except (ValueError, IndexError):
            new_num = 1
    else:
        new_num = 1
    
    return f"{prefix}-{new_num:04d}"


def generate_pharmacy_code(cur):
    cur.execute("SELECT COUNT(*) FROM pharmacies")
    count = cur.fetchone()[0] + 1
    return f"PHARM-{count:04d}"


def ensure_default_pharmacies(cur):
    ensure_pharmacy_license_column(cur)
    default_pharmacies = [
        ("Neethi Pharmacy", "Kodungallur, Thrissur", "+91 94470 11223", "KER-PH-1001", "active"),
        ("Aster MedPoint", "Edappally, Kochi", "+91 98956 22334", "KER-PH-1002", "active"),
        ("Kottakkal Arya Vaidya Pharmacy", "Kottakkal, Malappuram", "+91 98460 33445", "KER-PH-1003", "active"),
        ("Malabar Medicals", "Nadakkavu, Kozhikode", "+91 97440 44556", "KER-PH-1004", "active"),
        ("Travancore Health Pharmacy", "Pattom, Thiruvananthapuram", "+91 96050 55667", "KER-PH-1005", "active"),
    ]

    for name, location, phone, license_number, status in default_pharmacies:
        cur.execute("SELECT pharmacyId FROM pharmacies WHERE LOWER(name)=LOWER(%s) LIMIT 1", (name,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE pharmacies SET licenseNumber=%s WHERE pharmacyId=%s", (license_number, row[0]))
            continue

        pharmacy_code = generate_pharmacy_code(cur)
        cur.execute("""
            INSERT INTO pharmacies (pharmacy_code, name, location, contactNumber, licenseNumber, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (pharmacy_code, name, location, phone, license_number, status))


def ensure_pharmacy_license_column(cur):
    cur.execute("SHOW COLUMNS FROM pharmacies LIKE 'licenseNumber'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE pharmacies ADD COLUMN licenseNumber VARCHAR(100) NULL")


def ensure_pharmacy_license_values(cur):
    ensure_pharmacy_license_column(cur)
    cur.execute("""
        UPDATE pharmacies
        SET licenseNumber = CONCAT('KER-LIC-', LPAD(pharmacyId, 4, '0'))
        WHERE licenseNumber IS NULL OR TRIM(licenseNumber) = ''
    """)


def get_client_id_from_session():
    role_id = session.get('role_id')
    if role_id:
        return role_id

    user_id = session.get('user_id')
    if not user_id:
        return None

    cur = mysql.connection.cursor()
    cur.execute("SELECT clientId FROM clients WHERE userId=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def get_doctor_id_from_session():
    role_id = session.get('role_id')
    if role_id:
        return role_id

    user_id = session.get('user_id')
    if not user_id:
        return None

    cur = mysql.connection.cursor()
    cur.execute("SELECT doctorId FROM doctors WHERE userId=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def ensure_appointments_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            appointmentId INT AUTO_INCREMENT PRIMARY KEY,
            clientId INT NOT NULL,
            doctorId INT NOT NULL,
            appointmentDate DATE NOT NULL,
            appointmentTime TIME NULL,
            reason VARCHAR(500),
            symptoms TEXT,
            status ENUM('pending','confirmed','cancelled','completed') NOT NULL DEFAULT 'pending',
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (clientId) REFERENCES clients(clientId),
            FOREIGN KEY (doctorId) REFERENCES doctors(doctorId)
        )
    """)
    # Allow doctor to set the time later during confirmation.
    cur.execute("SHOW COLUMNS FROM appointments LIKE 'appointmentTime'")
    row = cur.fetchone()
    if row and len(row) > 2 and row[2] == 'NO':
        cur.execute("ALTER TABLE appointments MODIFY COLUMN appointmentTime TIME NULL")
    # Cleanup legacy placeholder times from earlier client-side default behavior.
    cur.execute("""
        UPDATE appointments
        SET appointmentTime=NULL
        WHERE status='pending' AND appointmentTime='09:00:00'
    """)


# ══════════════════════════════════════════
# DECORATORS
# ══════════════════════════════════════════

def get_table_columns(cur, table_name):
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row[0] for row in cur.fetchall()}


def first_existing_column(columns, candidates):
    for name in candidates:
        if name in columns:
            return name
    return None


def first_non_empty_column_expr(table_alias, columns, candidates, fallback):
    existing = [name for name in candidates if name in columns]
    if not existing:
        return fallback
    parts = [f"NULLIF(TRIM({table_alias}.{name}), '')" for name in existing]
    return f"COALESCE({', '.join(parts)}, {fallback})"


def ensure_medicine_inventory_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS medicines (
            medicineId INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            content VARCHAR(255) NOT NULL,
            brandName VARCHAR(255) NULL,
            isActive TINYINT(1) NOT NULL DEFAULT 1,
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_pm_name (name),
            INDEX idx_pm_content (content),
            UNIQUE KEY uq_pm_name_content (name, content)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            inventoryId INT AUTO_INCREMENT PRIMARY KEY,
            pharmacyId INT NOT NULL,
            medicineId INT NOT NULL,
            quantityAvailable INT NOT NULL DEFAULT 0,
            unitPrice DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            minStockLevel INT NOT NULL DEFAULT 10,
            gstRates DECIMAL(5,2) NOT NULL DEFAULT 12.00,
            isActive TINYINT(1) NOT NULL DEFAULT 1,
            expiryDate DATE NULL,
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_inventory_pharmacy_medicine (pharmacyId, medicineId),
            INDEX idx_inv_pharmacy (pharmacyId),
            INDEX idx_inv_medicine (medicineId)
        )
    """)

    medicine_columns = get_table_columns(cur, "medicines")
    if "brandName" not in medicine_columns:
        cur.execute("ALTER TABLE medicines ADD COLUMN brandName VARCHAR(255) NULL")
    if "isActive" not in medicine_columns:
        cur.execute("ALTER TABLE medicines ADD COLUMN isActive TINYINT(1) NOT NULL DEFAULT 1")
    if "createdAt" not in medicine_columns:
        cur.execute("ALTER TABLE medicines ADD COLUMN createdAt DATETIME DEFAULT CURRENT_TIMESTAMP")
    if "updatedAt" not in medicine_columns:
        cur.execute("ALTER TABLE medicines ADD COLUMN updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

    inventory_columns = get_table_columns(cur, "inventory")
    if "quantityAvailable" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN quantityAvailable INT NOT NULL DEFAULT 0")
    if "unitPrice" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN unitPrice DECIMAL(10,2) NOT NULL DEFAULT 0.00")
    if "minStockLevel" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN minStockLevel INT NOT NULL DEFAULT 10")
    if "isActive" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN isActive TINYINT(1) NOT NULL DEFAULT 1")
    if "expiryDate" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN expiryDate DATE NULL")
    if "createdAt" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN createdAt DATETIME DEFAULT CURRENT_TIMESTAMP")
    if "updatedAt" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    if "gstRates" not in inventory_columns:
        cur.execute("ALTER TABLE inventory ADD COLUMN gstRates DECIMAL(5,2) NOT NULL DEFAULT 12.00")
    # Ensure one row per (pharmacyId, medicineId) to prevent duplicates.
    cur.execute("SHOW INDEX FROM inventory WHERE Key_name='uq_inventory_pharmacy_medicine'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE inventory ADD UNIQUE KEY uq_inventory_pharmacy_medicine (pharmacyId, medicineId)")


def get_or_create_medicine_id(cur, name, content, brand_name=None, pharmacy_id=None):
    medicine_columns = get_table_columns(cur, "medicines")
    has_pharmacy_id = "pharmacyId" in medicine_columns
    cur.execute("""
        SELECT medicineId
        FROM medicines
        WHERE LOWER(name)=LOWER(%s) AND LOWER(content)=LOWER(%s)
        LIMIT 1
    """, (name, content))
    row = cur.fetchone()
    if row:
        if brand_name or (has_pharmacy_id and pharmacy_id):
            set_parts = []
            params = []
            if brand_name:
                set_parts.append("brandName=COALESCE(brandName, %s)")
                params.append(brand_name)
            if has_pharmacy_id and pharmacy_id:
                set_parts.append("pharmacyId=COALESCE(pharmacyId, %s)")
                params.append(pharmacy_id)
            set_parts.append("isActive=1")
            cur.execute("""
                UPDATE medicines
                SET {updates}
                WHERE medicineId=%s
            """.format(updates=", ".join(set_parts)), (*params, row[0]))
        else:
            cur.execute("UPDATE medicines SET isActive=1 WHERE medicineId=%s", (row[0],))
        return row[0]

    if has_pharmacy_id and pharmacy_id:
        cur.execute("""
            INSERT INTO medicines (name, content, brandName, pharmacyId, isActive)
            VALUES (%s, %s, %s, %s, 1)
        """, (name, content, brand_name or None, pharmacy_id))
    else:
        cur.execute("""
            INSERT INTO medicines (name, content, brandName, isActive)
            VALUES (%s, %s, %s, 1)
        """, (name, content, brand_name or None))
    return cur.lastrowid


def ensure_dispense_bills_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pharmacy_dispense_bills (
            billId INT AUTO_INCREMENT PRIMARY KEY,
            invoiceNo VARCHAR(50) NULL,
            prescriptionId INT NOT NULL,
            pharmacyId INT NOT NULL,
            pharmacistId INT NOT NULL,
            clientId INT NULL,
            requestedMedicine VARCHAR(255) NOT NULL,
            dispensedMedicine VARCHAR(255) NOT NULL,
            dispensedContent VARCHAR(255) NOT NULL,
            quantity INT NOT NULL DEFAULT 1,
            unitPrice DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            subtotalAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            gstRate DECIMAL(5,2) NOT NULL DEFAULT 12.00,
            gstAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            totalAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            paymentStatus VARCHAR(50) NOT NULL DEFAULT 'generated',
            paymentMethod VARCHAR(40) NULL,
            paidAt DATETIME NULL,
            paymentNotified TINYINT(1) NOT NULL DEFAULT 0,
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_pdb_pharmacy_created (pharmacyId, createdAt),
            INDEX idx_pdb_prescription (prescriptionId)
        )
    """)
    ensure_dispense_bills_columns(cur)


def ensure_routing_engine_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routing_engine (
            routingId INT AUTO_INCREMENT PRIMARY KEY,
            prescriptionId INT NOT NULL,
            pharmacyId INT NOT NULL,
            allocationStatus VARCHAR(50) NOT NULL DEFAULT 'pending',
            createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_route_prescription (prescriptionId),
            INDEX idx_route_pharmacy (pharmacyId)
        )
    """)
    columns = get_table_columns(cur, "routing_engine")
    if "prescriptionId" not in columns:
        cur.execute("ALTER TABLE routing_engine ADD COLUMN prescriptionId INT NOT NULL")
    if "pharmacyId" not in columns:
        cur.execute("ALTER TABLE routing_engine ADD COLUMN pharmacyId INT NOT NULL")
    if "allocationStatus" not in columns:
        cur.execute("ALTER TABLE routing_engine ADD COLUMN allocationStatus VARCHAR(50) NOT NULL DEFAULT 'pending'")
    if "createdAt" not in columns:
        cur.execute("ALTER TABLE routing_engine ADD COLUMN createdAt DATETIME DEFAULT CURRENT_TIMESTAMP")


def ensure_documents_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            documentId INT AUTO_INCREMENT PRIMARY KEY,
            prescriptionId INT NOT NULL,
            filePath VARCHAR(500) NOT NULL,
            uploadedBy INT NOT NULL,
            uploadedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_pd_prescription (prescriptionId),
            INDEX idx_pd_uploaded_by (uploadedBy),
            FOREIGN KEY (prescriptionId) REFERENCES prescriptions(prescriptionId),
            FOREIGN KEY (uploadedBy) REFERENCES users(userId)
        )
    """)
    columns = get_table_columns(cur, "documents")
    if "documentId" not in columns:
        cur.execute("ALTER TABLE documents ADD COLUMN documentId INT AUTO_INCREMENT PRIMARY KEY")
    if "prescriptionId" not in columns:
        cur.execute("ALTER TABLE documents ADD COLUMN prescriptionId INT NOT NULL")
    if "filePath" not in columns:
        cur.execute("ALTER TABLE documents ADD COLUMN filePath VARCHAR(500) NOT NULL")
    if "uploadedBy" not in columns:
        cur.execute("ALTER TABLE documents ADD COLUMN uploadedBy INT NOT NULL")
    if "uploadedDate" not in columns:
        cur.execute("ALTER TABLE documents ADD COLUMN uploadedDate DATETIME DEFAULT CURRENT_TIMESTAMP")

def backfill_routing_engine(cur):
    ensure_prescription_support_columns(cur)
    ensure_routing_engine_table(cur)
    col = get_prescription_columns(cur)
    if not col["pharmacy"] or not col["status"]:
        return
    cur.execute(f"""
        INSERT INTO routing_engine (prescriptionId, pharmacyId, allocationStatus)
        SELECT p.prescriptionId, p.{col['pharmacy']}, p.{col['status']}
        FROM prescriptions p
        WHERE p.{col['pharmacy']} IS NOT NULL
          AND p.{col['status']} IN ('routed','validated','dispensed')
          AND NOT EXISTS (
              SELECT 1
              FROM routing_engine r
              WHERE r.prescriptionId = p.prescriptionId
          )
    """)


def ensure_dispense_bills_columns(cur):
    columns = get_table_columns(cur, "pharmacy_dispense_bills")
    if "invoiceNo" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN invoiceNo VARCHAR(50) NULL")
    if "subtotalAmount" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN subtotalAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00")
    if "gstRate" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN gstRate DECIMAL(5,2) NOT NULL DEFAULT 12.00")
    if "gstAmount" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN gstAmount DECIMAL(10,2) NOT NULL DEFAULT 0.00")
    if "paymentMethod" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN paymentMethod VARCHAR(40) NULL")
    if "paidAt" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN paidAt DATETIME NULL")
    if "paymentNotified" not in columns:
        cur.execute("ALTER TABLE pharmacy_dispense_bills ADD COLUMN paymentNotified TINYINT(1) NOT NULL DEFAULT 0")


def seed_medicines(cur, pharmacy_id):
    default_medicines = [
        ("Paracetamol 500", "Paracetamol", "Cipla", 120, 2.50, 20),
        ("Dolo 650", "Paracetamol", "Micro Labs", 100, 3.80, 20),
        ("Calpol 650", "Paracetamol", "GSK", 90, 3.60, 15),
        ("Crocin Advance", "Paracetamol", "GSK", 80, 3.20, 15),
        ("Azithral 500", "Azithromycin", "Alembic", 60, 14.00, 10),
        ("Azee 500", "Azithromycin", "Cipla", 50, 13.50, 10),
        ("Amoxycillin 500", "Amoxicillin", "Sun Pharma", 90, 6.50, 15),
        ("Augmentin 625", "Amoxicillin + Clavulanic Acid", "GSK", 70, 22.00, 12),
        ("Cefixime 200", "Cefixime", "Lupin", 75, 18.00, 12),
        ("Ciplox 500", "Ciprofloxacin", "Cipla", 70, 9.50, 12),
        ("Oflox 200", "Ofloxacin", "Cipla", 65, 11.00, 10),
        ("Metrogyl 400", "Metronidazole", "JB Chemicals", 85, 5.00, 15),
        ("Pantocid 40", "Pantoprazole", "Sun Pharma", 100, 7.20, 20),
        ("Pan 40", "Pantoprazole", "Alkem", 95, 6.80, 20),
        ("Rantac 150", "Ranitidine", "J B Chemicals", 60, 4.00, 10),
        ("Ecosprin 75", "Aspirin", "USV", 110, 2.20, 20),
        ("Clopitab 75", "Clopidogrel", "Lupin", 90, 8.50, 15),
        ("Atorva 10", "Atorvastatin", "Zydus", 100, 7.00, 20),
        ("Rosuvas 10", "Rosuvastatin", "Sun Pharma", 80, 9.20, 15),
        ("Telma 40", "Telmisartan", "Glenmark", 90, 8.80, 15),
        ("Amlodac 5", "Amlodipine", "Zydus", 100, 4.20, 20),
        ("Met XL 50", "Metoprolol", "Ajanta", 85, 6.00, 15),
        ("Lasix 40", "Furosemide", "Sanofi", 65, 5.80, 10),
        ("Glycomet 500", "Metformin", "USV", 130, 3.10, 25),
        ("Glycomet GP1", "Metformin + Glimepiride", "USV", 90, 7.50, 15),
        ("Januvia 100", "Sitagliptin", "MSD", 40, 42.00, 8),
        ("Thyronorm 50", "Levothyroxine", "Abbott", 100, 2.60, 20),
        ("Shelcal 500", "Calcium + Vitamin D3", "Torrent", 100, 6.30, 20),
        ("Limcee 500", "Vitamin C", "Abbott", 120, 2.80, 25),
        ("Becosules", "B-Complex", "Pfizer", 90, 3.90, 15),
        ("Neurobion Forte", "Methylcobalamin + B Vitamins", "Merck", 85, 8.00, 15),
        ("Zincovit", "Multivitamin + Zinc", "Apex", 80, 9.00, 12),
        ("Montair LC", "Montelukast + Levocetirizine", "Cipla", 90, 12.00, 15),
        ("Cetcip 10", "Cetirizine", "Cipla", 120, 1.80, 25),
        ("Allegra 120", "Fexofenadine", "Sanofi", 70, 14.50, 10),
        ("TusQ-DX", "Dextromethorphan + Chlorpheniramine + Phenylephrine", "Zuventus", 60, 68.00, 8),
        ("Ascoril LS", "Levosalbutamol + Ambroxol + Guaifenesin", "Glenmark", 55, 95.00, 8),
        ("Deriphyllin Retard", "Etofylline + Theophylline", "Zydus", 70, 7.80, 10),
        ("Combiflam", "Ibuprofen + Paracetamol", "Sanofi", 95, 4.80, 15),
        ("Brufen 400", "Ibuprofen", "Abbott", 90, 3.60, 15),
        ("Aceclo Plus", "Aceclofenac + Paracetamol", "Aristo", 85, 6.40, 12),
        ("Voveran SR", "Diclofenac", "Novartis", 70, 8.20, 10),
        ("Meftal Spas", "Mefenamic Acid + Dicyclomine", "Blue Cross", 75, 7.10, 12),
        ("Ondem 4", "Ondansetron", "Alkem", 80, 5.90, 12),
        ("Domstal", "Domperidone", "Torrent", 75, 4.90, 10),
        ("ORS Electral", "Oral Rehydration Salts", "FDC", 100, 21.00, 20),
        ("Loperamide 2", "Loperamide", "Cipla", 70, 2.70, 12),
        ("Duphalac", "Lactulose", "Abbott", 45, 180.00, 6),
        ("Volini Gel", "Diclofenac Diethylamine + Methyl Salicylate", "Sun Pharma", 50, 110.00, 8),
        ("Betadine Ointment", "Povidone Iodine", "Win-Medicare", 60, 68.00, 10),
    ]

    # Avoid repeated write-heavy upserts on every dashboard load.
    cur.execute("""
        SELECT COUNT(*)
        FROM inventory
        WHERE pharmacyId=%s
    """, (pharmacy_id,))
    existing_count = cur.fetchone()[0]
    # Only seed when inventory is empty to avoid re-adding deleted items.
    if existing_count > 0:
        return

    for name, content, brand, stock_qty, unit_price, min_stock in default_medicines:
        medicine_id = get_or_create_medicine_id(cur, name, content, brand, pharmacy_id=pharmacy_id)
        cur.execute("""
            INSERT INTO inventory
                (pharmacyId, medicineId, quantityAvailable, unitPrice, minStockLevel, isActive)
            VALUES (%s, %s, %s, %s, %s, 1)
            ON DUPLICATE KEY UPDATE
                quantityAvailable=quantityAvailable + VALUES(quantityAvailable),
                unitPrice=VALUES(unitPrice),
                minStockLevel=VALUES(minStockLevel),
                isActive=1
        """, (pharmacy_id, medicine_id, stock_qty, unit_price, min_stock))


def sync_inventory_from_medicines(cur, pharmacy_id=None):
    columns = get_table_columns(cur, "medicines")
    legacy_fields = {"pharmacyId", "stockQty", "unitPrice", "minStockLevel"}
    if not legacy_fields.issubset(columns):
        return

    if pharmacy_id:
        cur.execute("""
            INSERT INTO inventory
                (pharmacyId, medicineId, quantityAvailable, unitPrice, minStockLevel, isActive)
            SELECT
                pharmacyId,
                medicineId,
                stockQty,
                unitPrice,
                minStockLevel,
                isActive
            FROM medicines
            WHERE pharmacyId=%s
            ON DUPLICATE KEY UPDATE
                quantityAvailable=VALUES(quantityAvailable),
                unitPrice=VALUES(unitPrice),
                minStockLevel=VALUES(minStockLevel),
                isActive=VALUES(isActive)
        """, (pharmacy_id,))
        return

    cur.execute("""
        INSERT INTO inventory
            (pharmacyId, medicineId, quantityAvailable, unitPrice, minStockLevel, isActive)
        SELECT
            pharmacyId,
            medicineId,
            stockQty,
            unitPrice,
            minStockLevel,
            isActive
        FROM medicines
        ON DUPLICATE KEY UPDATE
            quantityAvailable=VALUES(quantityAvailable),
            unitPrice=VALUES(unitPrice),
            minStockLevel=VALUES(minStockLevel),
            isActive=VALUES(isActive)
    """)


def get_pharmacist_context(cur):
    role_id = session.get('role_id')
    if role_id:
        cur.execute("""
            SELECT pharmacistId, pharmacyId
            FROM pharmacists
            WHERE pharmacistId=%s
        """, (role_id,))
        row = cur.fetchone()
        if row:
            return row[0], row[1]

    user_id = session.get('user_id')
    if not user_id:
        return None, None

    cur.execute("""
        SELECT pharmacistId, pharmacyId
        FROM pharmacists
        WHERE userId=%s
    """, (user_id,))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def get_pharmacy_admin_context(cur):
    role_id = session.get('role_id')
    if role_id:
        cur.execute("""
            SELECT adminId, pharmacyId
            FROM pharmacy_admins
            WHERE adminId=%s
        """, (role_id,))
        row = cur.fetchone()
        if row:
            return row[0], row[1]

    user_id = session.get('user_id')
    if not user_id:
        return None, None

    cur.execute("""
        SELECT adminId, pharmacyId
        FROM pharmacy_admins
        WHERE userId=%s
    """, (user_id,))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def normalize_medicine_name(name):
    text = (name or "").strip().lower()
    if not text:
        return ""
    # Remove strength tokens like 500 mg / 5ml to match base medicine names.
    text = re.sub(r"\b\d+(?:\.\d+)?\s*(?:mg|g|mcg|ml)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -_,")
    return text


def find_medicine_or_substitute(cur, pharmacy_id, requested_medicine, quantity):
    requested = (requested_medicine or "").strip()
    if not requested:
        return None, None

    requested_lc = requested.lower()
    normalized_requested = normalize_medicine_name(requested)

    cur.execute("""
        SELECT m.medicineId, m.name, m.content, i.quantityAvailable, i.unitPrice
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND LOWER(m.name)=LOWER(%s)
          AND i.quantityAvailable >= %s
        LIMIT 1
    """, (pharmacy_id, requested, quantity))
    exact_match = cur.fetchone()
    if exact_match:
        return exact_match, False

    if normalized_requested and normalized_requested != requested_lc:
        cur.execute("""
            SELECT m.medicineId, m.name, m.content, i.quantityAvailable, i.unitPrice
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
              AND LOWER(m.name)=LOWER(%s)
              AND i.quantityAvailable >= %s
            LIMIT 1
        """, (pharmacy_id, normalized_requested, quantity))
        normalized_exact = cur.fetchone()
        if normalized_exact:
            return normalized_exact, False

    if normalized_requested:
        like_pattern = f"%{normalized_requested}%"
        cur.execute("""
            SELECT m.medicineId, m.name, m.content, i.quantityAvailable, i.unitPrice
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
              AND i.quantityAvailable >= %s
              AND (LOWER(m.name) LIKE %s OR LOWER(%s) LIKE CONCAT('%%', LOWER(m.name), '%%'))
            ORDER BY CASE WHEN LOWER(m.name)=LOWER(%s) THEN 0 ELSE 1 END, i.quantityAvailable DESC
            LIMIT 1
        """, (pharmacy_id, quantity, like_pattern, normalized_requested, normalized_requested))
        fuzzy_name_match = cur.fetchone()
        if fuzzy_name_match:
            return fuzzy_name_match, False

    by_name_content = None
    if normalized_requested:
        like_pattern = f"%{normalized_requested}%"
        cur.execute("""
            SELECT m.content
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
              AND (LOWER(m.name)=LOWER(%s)
                   OR LOWER(m.name)=LOWER(%s)
                   OR LOWER(m.name) LIKE %s
                   OR LOWER(%s) LIKE CONCAT('%%', LOWER(m.name), '%%'))
            ORDER BY i.quantityAvailable DESC
            LIMIT 1
        """, (pharmacy_id, requested, normalized_requested, like_pattern, normalized_requested))
        by_name_content = cur.fetchone()
    else:
        cur.execute("""
            SELECT m.content
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1 AND LOWER(m.name)=LOWER(%s)
            ORDER BY i.quantityAvailable DESC
            LIMIT 1
        """, (pharmacy_id, requested))
        by_name_content = cur.fetchone()

    content = (by_name_content[0] if by_name_content else "").strip()
    if not content:
        return None, None

    cur.execute("""
        SELECT m.medicineId, m.name, m.content, i.quantityAvailable, i.unitPrice
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND i.quantityAvailable >= %s
          AND LOWER(m.content)=LOWER(%s)
        ORDER BY CASE WHEN LOWER(m.name)=LOWER(%s) THEN 0 ELSE 1 END, i.quantityAvailable DESC
        LIMIT 1
    """, (pharmacy_id, quantity, content, normalized_requested or requested))
    substitute = cur.fetchone()
    if substitute:
        return substitute, True

    return None, None


def ensure_prescription_support_columns(cur):
    columns = get_table_columns(cur, "prescriptions")
    if "medicineName" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN medicineName VARCHAR(255) NULL")
    if "quantity" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN quantity INT NOT NULL DEFAULT 1")
    if "dosageInstructions" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN dosageInstructions TEXT NULL")
    if "pharmacyId" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN pharmacyId INT NULL")
    if "routedPharmacistId" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN routedPharmacistId INT NULL")
    if "uploadedDocumentPath" not in columns and "document_path" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN uploadedDocumentPath VARCHAR(500) NULL")
    if "extractedText" not in columns and "document_text" not in columns:
        cur.execute("ALTER TABLE prescriptions ADD COLUMN extractedText TEXT NULL")

def extract_medicine_from_text(text):
    """Extract medicine name from extractedText or dosageInstructions using regex."""
    if not text:
        return None
    # Common medicine patterns: Capitalized words, followed by strength/dosage
    patterns = [
        r'(?:Rx|Prescription)[^.\n]*?([A-Z][a-zA-Z\s]{2,30}(?:\s+\d+mg)?)(?=\.|,|$|\n)',
        r'([A-Z][a-zA-Z\s]{3,25}(?:\s+(?:[5-9]\d{2,3}|1[0-9]{3})[mM][gG]?)?)(?:\s*[-–]\s*1-0-1|\s*dose)',
        r'(Paracetamol|Dolo|Calpol|Crocin|Azithral|Azee|Augmentin|Cefixime|Ciplox|Pantocid|Pan|Ecosprin)(?:\s+\d+)?'
    ]
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
        if match:
            med = match.group(1).strip()
            if len(med) > 3 and not med.isdigit():
                return med
    return None


def get_prescription_columns(cur):
    columns = get_table_columns(cur, "prescriptions")
    return {
        "medicine": first_existing_column(columns, ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"]),
        "quantity": first_existing_column(columns, ["quantity", "qty", "prescribedQty", "medicineQty"]),
        "status": first_existing_column(columns, ["status"]),
        "pharmacy": first_existing_column(columns, ["pharmacyId", "pharmacy_id"]),
        "routed_pharmacist": first_existing_column(columns, ["routedPharmacistId"]),
        "uploaded_document": first_existing_column(columns, ["uploadedDocumentPath", "document_path"]),
        "extracted_text": first_existing_column(columns, ["extractedText", "document_text"]),
        "client": first_existing_column(columns, ["clientId", "client_id"]),
    }


def route_prescription_to_best_pharmacy(cur, prescription_id, client_id):
    # Delegate to shared routing engine (handles medicines/inventory tables).
    return routing_engine.route_prescription_to_best_pharmacy(cur, prescription_id, client_id)

def resolve_client_by_code_or_id(cur, client_lookup):
    value = (client_lookup or "").strip()
    if not value:
        return None

    if value.isdigit():
        cur.execute("""
            SELECT clientId, client_code, name
            FROM clients
            WHERE clientId=%s
            LIMIT 1
        """, (int(value),))
        row = cur.fetchone()
        if row:
            return row

    cur.execute("""
        SELECT clientId, client_code, name
        FROM clients
        WHERE UPPER(client_code)=UPPER(%s)
        LIMIT 1
    """, (value,))
    return cur.fetchone()


def insert_prescription_row(cur, columns, payload):
    doctor_col = first_existing_column(columns, ["doctorId", "doctor_id"])
    client_col = first_existing_column(columns, ["clientId", "client_id"])
    status_col = first_existing_column(columns, ["status"])
    pharmacy_col = first_existing_column(columns, ["pharmacyId", "pharmacy_id"])
    uploaded_col = first_existing_column(columns, ["uploadedDocumentPath", "document_path"])
    extracted_col = first_existing_column(columns, ["extractedText", "document_text"])
    created_col = first_existing_column(columns, ["createsDate", "createdAt", "created_date"])
    medicine_col = first_existing_column(columns, ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"])
    quantity_col = first_existing_column(columns, ["quantity", "qty", "prescribedQty", "medicineQty"])
    dosage_col = first_existing_column(columns, ["dosageInstructions", "instructions", "dosage"])

    insert_map = []
    if doctor_col:
        insert_map.append((doctor_col, payload["doctor_id"]))
    if client_col:
        insert_map.append((client_col, payload["client_id"]))
    if status_col:
        insert_map.append((status_col, payload["status"]))
    if pharmacy_col:
        insert_map.append((pharmacy_col, payload["pharmacy_id"]))
    if uploaded_col:
        insert_map.append((uploaded_col, payload["document_text"]))
    if extracted_col and payload.get("extracted_text") is not None:
        insert_map.append((extracted_col, payload["extracted_text"]))
    if medicine_col:
        insert_map.append((medicine_col, payload["medicine_name"]))
    if quantity_col:
        insert_map.append((quantity_col, payload["quantity"]))
    if dosage_col:
        insert_map.append((dosage_col, payload["dosage_text"]))
    if created_col and payload.get("created_at") is not None:
        insert_map.append((created_col, payload["created_at"]))

    if not insert_map:
        return None

    cols = ", ".join([c for c, _ in insert_map])
    placeholders = ", ".join(["%s"] * len(insert_map))
    values = tuple(v for _, v in insert_map)
    cur.execute(f"INSERT INTO prescriptions ({cols}) VALUES ({placeholders})", values)
    return cur.lastrowid


def build_simple_pdf_bytes(title, lines):
    try:
        from fpdf import FPDF

        def _safe(txt):
            return (txt or "").encode("latin-1", errors="replace").decode("latin-1")

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, _safe(title), ln=True, align="C")
        pdf.ln(2)
        pdf.set_font("Helvetica", "", 11)
        for line in lines:
            pdf.multi_cell(0, 6, _safe(str(line)))
        return pdf.output(dest="S").encode("latin-1", errors="replace")
    except Exception:
        # Minimal one-page PDF writer fallback for plain-text export.
        def _esc(txt):
            return (txt or "").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

        text_lines = [_esc(title), ""] + [_esc(line) for line in lines]
        ops = ["BT", "/F1 12 Tf", "14 TL", "72 780 Td"]
        for idx, line in enumerate(text_lines):
            ops.append(f"({line}) Tj")
            if idx < len(text_lines) - 1:
                ops.append("T*")
        ops.append("ET")
        stream = "\n".join(ops).encode("latin-1", errors="replace")

        obj1 = b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        obj2 = b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n"
        obj3 = b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n"
        obj4 = b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n"
        obj5 = f"5 0 obj << /Length {len(stream)} >> stream\n".encode("ascii") + stream + b"\nendstream endobj\n"

        pdf = b"%PDF-1.4\n"
        offsets = [0]
        for obj in (obj1, obj2, obj3, obj4, obj5):
            offsets.append(len(pdf))
            pdf += obj

        xref = len(pdf)
        pdf += f"xref\n0 {len(offsets)}\n".encode("ascii")
        pdf += b"0000000000 65535 f \n"
        for off in offsets[1:]:
            pdf += f"{off:010d} 00000 n \n".encode("ascii")
        pdf += f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode("ascii")
        return pdf


def extract_prescription_text(file_storage):
    filename = (file_storage.filename or "").lower()
    try:
        data = file_storage.read()
    finally:
        try:
            file_storage.stream.seek(0)
        except Exception:
            pass

    if filename.endswith(".txt"):
        try:
            return data.decode("utf-8", errors="ignore").strip()
        except Exception:
            return ""

    if filename.endswith(".pdf"):
        try:
            from PyPDF2 import PdfReader  # optional dependency
            reader = PdfReader(BytesIO(data))
            text = "\n".join((page.extract_text() or "").strip() for page in reader.pages)
            return text.strip()
        except Exception:
            return ""

    return ""


def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('user.home'))
            if session.get('role') not in roles:
                return render_template('403.html'), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


def redirect_by_role(role):
    routes = {
        'system_admin':   'user.system_admin_dashboard',
        'doctor':         'user.doctor_dashboard',
        'pharmacist':     'user.pharmacist_dashboard',
        'pharmacy_admin': 'user.pharmacy_admin_dashboard',
        'client':         'user.client_dashboard',
    }
    return redirect(url_for(routes.get(role, 'user.home')))


# ══════════════════════════════════════════
# AUTH ROUTES
# ══════════════════════════════════════════


@user_bp.route('/')
def home():
    return render_template('login.html')


@user_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        # ── Common fields (go into users table) ─────────────
        name     = request.form.get('name', '').strip()
        email    = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        role     = request.form.get('role', '')

        if role == 'system_admin':
            return render_template('registration.html', error="Invalid role selected.")

        if len(password) < 8:
            return render_template('registration.html',
                                   error="Password must be at least 8 characters.",
                                   form=request.form)

        cur = mysql.connection.cursor()

        # Check duplicate email in users table
        cur.execute("SELECT userId FROM users WHERE email=%s", (email,))
        if cur.fetchone():
            cur.close()
            return render_template('registration.html',
                                   error="This email is already registered. Please sign in instead.",
                                   form=request.form)

        # Check duplicate username
        cur.execute("SELECT userId FROM users WHERE name=%s", (name,))
        if cur.fetchone():
            cur.close()
            return render_template('registration.html',
                                   error="This username is already taken. Please choose a different name.",
                                   form=request.form)

        # Check duplicate email in role-specific tables
        if role == 'client':
            cur.execute("SELECT clientId FROM clients WHERE email=%s", (email,))
            if cur.fetchone():
                cur.close()
                return render_template('registration.html',
                                       error="This email is already registered as a client. Please sign in instead.",
                                       form=request.form)
        elif role == 'doctor':
            cur.execute("SELECT doctorId FROM doctors WHERE email=%s", (email,))
            if cur.fetchone():
                cur.close()
                return render_template('registration.html',
                                       error="This email is already registered as a doctor. Please sign in instead.",
                                       form=request.form)
        elif role == 'pharmacist':
            cur.execute("SELECT pharmacistId FROM pharmacists WHERE email=%s", (email,))
            if cur.fetchone():
                cur.close()
                return render_template('registration.html',
                                       error="This email is already registered as a pharmacist. Please sign in instead.",
                                       form=request.form)
        elif role == 'pharmacy_admin':
            cur.execute("SELECT adminId FROM pharmacy_admins WHERE email=%s", (email,))
            if cur.fetchone():
                cur.close()
                return render_template('registration.html',
                                       error="This email is already registered as a pharmacy admin. Please sign in instead.",
                                       form=request.form)

        # Hash password
        hashed_password = generate_password_hash(password)

        # Generate role code
        role_code = generate_role_code(role, cur)

        # ── INSERT into users ────────────────────────────────
        cur.execute("""
            INSERT INTO users (name, email, password, role, status)
            VALUES (%s, %s, %s, %s, 'pending')
        """, (name, email, hashed_password, role))
        mysql.connection.commit()
        user_id = cur.lastrowid

        # ── INSERT into role-specific table with all fields ──
        if role == 'doctor':
            specialization = request.form.get('specialization', '').strip()
            license_number = request.form.get('licenseNumber', '').strip()
            phone          = request.form.get('phone', '').strip()
            address        = request.form.get('address', '').strip()

            cur.execute("""
                INSERT INTO doctors
                    (doctor_code, userId, name, email, specialization, licenseNumber, phone, address)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (role_code, user_id, name, email, specialization, license_number, phone, address))

        elif role == 'client':
            dob       = request.form.get('dateOfBirth', None)
            phone     = request.form.get('phone', '').strip()
            address   = request.form.get('address', '').strip()
            allergies = request.form.get('allergies', '').strip()

            cur.execute("""
                INSERT INTO clients
                    (client_code, userId, name, email, dateOfBirth, phone, address, allergies)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (role_code, user_id, name, email, dob or None, phone, address, allergies))

        elif role == 'pharmacist':
            license_number = request.form.get('licenseNumber', '').strip()
            phone          = request.form.get('phone', '').strip()
            pharmacy_code  = request.form.get('pharmacyLicensePh', '').strip().upper()

            if not pharmacy_code:
                cur.close()
                return render_template('registration.html',
                                       error="Pharmacy license/code is required for pharmacist registration.",
                                       form=request.form)

            cur.execute("""
                SELECT pharmacyId
                FROM pharmacies
                WHERE UPPER(pharmacy_code)=%s OR UPPER(COALESCE(licenseNumber, ''))=%s
            """, (pharmacy_code, pharmacy_code))
            row = cur.fetchone()
            if not row:
                cur.close()
                return render_template('registration.html',
                                       error="Invalid pharmacy license number or code for pharmacist registration.",
                                       form=request.form)
            pharmacy_id = row[0]

            cur.execute("""
                INSERT INTO pharmacists
                    (pharmacist_code, userId, pharmacyId, name, email, licenseNumber, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (role_code, user_id, pharmacy_id, name, email, license_number, phone))

        elif role == 'pharmacy_admin':
            phone = request.form.get('phone', '').strip()
            pharmacy_code = request.form.get('pharmacyLicenseAdmin', '').strip().upper()

            if not pharmacy_code:
                cur.close()
                return render_template('registration.html',
                                       error="Pharmacy license/code is required for pharmacy admin registration.",
                                       form=request.form)

            cur.execute("""
                SELECT pharmacyId
                FROM pharmacies
                WHERE UPPER(pharmacy_code)=%s OR UPPER(COALESCE(licenseNumber, ''))=%s
            """, (pharmacy_code, pharmacy_code))
            row = cur.fetchone()
            if not row:
                cur.close()
                return render_template('registration.html',
                                       error="Invalid pharmacy license number or code for pharmacy admin registration.",
                                       form=request.form)
            pharmacy_id = row[0]

            cur.execute("""
                INSERT INTO pharmacy_admins
                    (admin_code, userId, pharmacyId, name, email, phone)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (role_code, user_id, pharmacy_id, name, email, phone))

        mysql.connection.commit()
        cur.close()

        return render_template('pending.html')

    return render_template('registration.html')


@user_bp.route('/login', methods=['POST'])
def login():
    email    = request.form['email'].strip().lower()
    password = request.form['password']
    print(email, password)
    cur = mysql.connection.cursor()

    cur.execute("""
        SELECT userId, name, role, status, password
        FROM users WHERE email=%s
    """, (email,))
    user = cur.fetchone()
    print(user)

    if not user:
        cur.close()
        return render_template('login.html', error="Invalid email or password.")

    user_id, name, role, status, stored_hash = user
    print(user_id)

    if not check_password_hash(stored_hash, password):
        cur.close()
        return render_template('login.html', error="Invalid email or password.")

    role_code = None
    role_id   = None

    if role == 'doctor':
        cur.execute("SELECT doctorId, doctor_code FROM doctors WHERE userId=%s", (user_id,))
        row = cur.fetchone()
        if row: role_id, role_code = row
    elif role == 'client':
        cur.execute("SELECT clientId, client_code FROM clients WHERE userId=%s", (user_id,))
        row = cur.fetchone()
        if row: role_id, role_code = row
    elif role == 'pharmacist':
        cur.execute("SELECT pharmacistId, pharmacist_code FROM pharmacists WHERE userId=%s", (user_id,))
        row = cur.fetchone()
        if row: role_id, role_code = row
    elif role == 'pharmacy_admin':
        cur.execute("SELECT adminId, admin_code FROM pharmacy_admins WHERE userId=%s", (user_id,))
        row = cur.fetchone()
        if row: role_id, role_code = row

    cur.close()

    if role == 'system_admin':
        session['user_id']   = user_id
        session['name']      = name
        session['role']      = role
        session['role_code'] = None
        session['role_id']   = None
        return redirect(url_for('user.system_admin_dashboard'))

    if status != 'approved':
        return render_template('pending.html')

    session['user_id']   = user_id
    session['name']      = name
    session['role']      = role
    session['role_code'] = role_code
    session['role_id']   = role_id

    return redirect_by_role(role)


@user_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('user.home'))


# ══════════════════════════════════════════
# SYSTEM ADMIN — DASHBOARD
# ══════════════════════════════════════════


@user_bp.route('/system_admin_dashboard')
@role_required('system_admin')
def system_admin_dashboard():
    cur = mysql.connection.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE role != 'system_admin'")
    total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE status='pending' AND role != 'system_admin'")
    pending_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE status='approved' AND role != 'system_admin'")
    approved_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM doctors")
    doctor_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pharmacists")
    pharmacist_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pharmacies")
    pharmacy_count = cur.fetchone()[0]
    cur.execute("""
        SELECT u.userId, u.name, u.email, u.role, u.status,
               COALESCE(d.doctor_code, c.client_code, ph.pharmacist_code, pa.admin_code) AS role_code
        FROM users u
        LEFT JOIN doctors         d  ON d.userId  = u.userId
        LEFT JOIN clients         c  ON c.userId  = u.userId
        LEFT JOIN pharmacists     ph ON ph.userId  = u.userId
        LEFT JOIN pharmacy_admins pa ON pa.userId  = u.userId
        WHERE u.status = 'pending' AND u.role != 'system_admin'
        ORDER BY u.userId DESC LIMIT 5
    """)
    pending_users = cur.fetchall()
    cur.close()
    return render_template('dashboards/system_admin_dashboard.html',
        total_users=total_users, pending_count=pending_count,
        approved_count=approved_count, doctor_count=doctor_count,
        pharmacist_count=pharmacist_count, pharmacy_count=pharmacy_count,
        pending_users=pending_users)


# ══════════════════════════════════════════
# SYSTEM ADMIN — APPROVE / REJECT
# ══════════════════════════════════════════

@user_bp.route('/admin/users')
@role_required('system_admin')
def view_users():
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT u.userId, u.name, u.email, u.role, u.status,
               COALESCE(d.doctor_code, c.client_code, ph.pharmacist_code, pa.admin_code) AS role_code
        FROM users u
        LEFT JOIN doctors         d  ON d.userId  = u.userId
        LEFT JOIN clients         c  ON c.userId  = u.userId
        LEFT JOIN pharmacists     ph ON ph.userId  = u.userId
        LEFT JOIN pharmacy_admins pa ON pa.userId  = u.userId
        WHERE u.status = 'pending' AND u.role != 'system_admin'
        ORDER BY u.userId DESC
    """)
    users = cur.fetchall()
    cur.close()
    return render_template('dashboards/approve_users.html', users=users)


@user_bp.route('/approve/<int:user_id>')
@role_required('system_admin')
def approve_user(user_id):
    cur = mysql.connection.cursor()
    cur.execute("UPDATE users SET status='approved' WHERE userId=%s", (user_id,))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.view_users'))


@user_bp.route('/reject/<int:user_id>')
@role_required('system_admin')
def reject_user(user_id):
    cur = mysql.connection.cursor()
    cur.execute("DELETE FROM users WHERE userId=%s AND role != 'system_admin'", (user_id,))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.view_users'))


@user_bp.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@role_required('system_admin')
def delete_user(user_id):
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT role FROM users WHERE userId=%s LIMIT 1", (user_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            return redirect(url_for('user.all_users', delete_error='User not found.'))

        role = row[0]
        if role == 'system_admin':
            cur.close()
            return redirect(url_for('user.all_users', delete_error='System admin users cannot be deleted.'))

        def table_has_column(table_name, column_name):
            try:
                cols = get_table_columns(cur, table_name)
                return column_name in cols
            except Exception:
                return False

        client_id = None
        doctor_id = None
        pharmacist_id = None

        if role == 'client':
            cur.execute("SELECT clientId FROM clients WHERE userId=%s LIMIT 1", (user_id,))
            r = cur.fetchone()
            client_id = r[0] if r else None
        elif role == 'doctor':
            cur.execute("SELECT doctorId FROM doctors WHERE userId=%s LIMIT 1", (user_id,))
            r = cur.fetchone()
            doctor_id = r[0] if r else None
        elif role == 'pharmacist':
            cur.execute("SELECT pharmacistId FROM pharmacists WHERE userId=%s LIMIT 1", (user_id,))
            r = cur.fetchone()
            pharmacist_id = r[0] if r else None

        # Remove dependent data first to satisfy FK constraints.
        if role == 'client' and client_id:
            if table_has_column("appointments", "clientId"):
                cur.execute("DELETE FROM appointments WHERE clientId=%s", (client_id,))
            if table_has_column("pharmacy_dispense_bills", "clientId"):
                cur.execute("DELETE FROM pharmacy_dispense_bills WHERE clientId=%s", (client_id,))
            if table_has_column("prescriptions", "clientId"):
                if table_has_column("pharmacy_dispense_bills", "prescriptionId"):
                    cur.execute("""
                        DELETE FROM pharmacy_dispense_bills
                        WHERE prescriptionId IN (
                            SELECT prescriptionId FROM prescriptions WHERE clientId=%s
                        )
                    """, (client_id,))
                cur.execute("DELETE FROM prescriptions WHERE clientId=%s", (client_id,))

        if role == 'doctor' and doctor_id:
            if table_has_column("appointments", "doctorId"):
                cur.execute("DELETE FROM appointments WHERE doctorId=%s", (doctor_id,))
            if table_has_column("prescriptions", "doctorId"):
                if table_has_column("pharmacy_dispense_bills", "prescriptionId"):
                    cur.execute("""
                        DELETE FROM pharmacy_dispense_bills
                        WHERE prescriptionId IN (
                            SELECT prescriptionId FROM prescriptions WHERE doctorId=%s
                        )
                    """, (doctor_id,))
                cur.execute("DELETE FROM prescriptions WHERE doctorId=%s", (doctor_id,))

        if role == 'pharmacist' and pharmacist_id:
            if table_has_column("pharmacy_dispense_bills", "pharmacistId"):
                cur.execute("DELETE FROM pharmacy_dispense_bills WHERE pharmacistId=%s", (pharmacist_id,))
            if table_has_column("prescriptions", "routedPharmacistId"):
                cur.execute("UPDATE prescriptions SET routedPharmacistId=NULL WHERE routedPharmacistId=%s", (pharmacist_id,))

        if role == 'doctor':
            cur.execute("DELETE FROM doctors WHERE userId=%s", (user_id,))
        elif role == 'client':
            cur.execute("DELETE FROM clients WHERE userId=%s", (user_id,))
        elif role == 'pharmacist':
            cur.execute("DELETE FROM pharmacists WHERE userId=%s", (user_id,))
        elif role == 'pharmacy_admin':
            cur.execute("DELETE FROM pharmacy_admins WHERE userId=%s", (user_id,))

        cur.execute("DELETE FROM users WHERE userId=%s", (user_id,))
        mysql.connection.commit()
        cur.close()
        return redirect(url_for('user.all_users', delete_success=1))
    except Exception as exc:
        mysql.connection.rollback()
        cur.close()
        err_code = exc.args[0] if getattr(exc, "args", None) else None
        if err_code in (1451, 1452):
            return redirect(url_for('user.all_users',
                                    delete_error='Cannot delete this user because related records exist.'))
        return redirect(url_for('user.all_users', delete_error=f'Delete failed: {str(exc)}'))


# ══════════════════════════════════════════
# SYSTEM ADMIN — ALL USERS
# ══════════════════════════════════════════

@user_bp.route('/admin/all_users')
@role_required('system_admin')
def all_users():
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT u.userId, u.name, u.email, u.role, u.status,
               COALESCE(d.doctor_code, c.client_code, ph.pharmacist_code, pa.admin_code) AS role_code
        FROM users u
        LEFT JOIN doctors         d  ON d.userId  = u.userId
        LEFT JOIN clients         c  ON c.userId  = u.userId
        LEFT JOIN pharmacists     ph ON ph.userId  = u.userId
        LEFT JOIN pharmacy_admins pa ON pa.userId  = u.userId
        WHERE u.role != 'system_admin'
        ORDER BY u.userId DESC
    """)
    users = cur.fetchall()
    cur.close()
    return render_template('dashboards/all_users.html',
                           users=users,
                           delete_success=request.args.get('delete_success'),
                           delete_error=request.args.get('delete_error'))


# ══════════════════════════════════════════
# SYSTEM ADMIN — PHARMACY MANAGEMENT
# ══════════════════════════════════════════

@user_bp.route('/admin/pharmacies')
@role_required('system_admin')
def view_pharmacies():
    cur = mysql.connection.cursor()
    ensure_default_pharmacies(cur)
    ensure_pharmacy_license_values(cur)
    mysql.connection.commit()
    cur.execute("""
        SELECT
            p.pharmacyId,
            p.pharmacy_code,
            p.name,
            p.location,
            p.contactNumber,
            p.licenseNumber,
            p.status,
            pa.admins,
            ph.pharmacists
        FROM pharmacies p
        LEFT JOIN (
            SELECT
                pharmacyId,
                GROUP_CONCAT(CONCAT(adminId, ' - ', name) ORDER BY adminId SEPARATOR ', ') AS admins
            FROM pharmacy_admins
            WHERE pharmacyId IS NOT NULL
            GROUP BY pharmacyId
        ) pa ON pa.pharmacyId = p.pharmacyId
        LEFT JOIN (
            SELECT
                pharmacyId,
                GROUP_CONCAT(CONCAT(pharmacistId, ' - ', name) ORDER BY pharmacistId SEPARATOR ', ') AS pharmacists
            FROM pharmacists
            WHERE pharmacyId IS NOT NULL
            GROUP BY pharmacyId
        ) ph ON ph.pharmacyId = p.pharmacyId
        ORDER BY p.pharmacyId DESC
    """)
    pharmacies = cur.fetchall()
    cur.close()
    return render_template('dashboards/pharmacies.html', pharmacies=pharmacies)


@user_bp.route('/admin/pharmacies/add', methods=['GET', 'POST'])
@role_required('system_admin')
def add_pharmacy():
    if request.method == 'POST':
        name     = request.form['name'].strip()
        location = request.form.get('location', '').strip()
        phone    = request.form.get('contactNumber', '').strip()
        license_number = request.form.get('licenseNumber', '').strip().upper()
        status   = request.form.get('status', 'active').strip().lower()
        if status not in ('active', 'inactive'):
            status = 'active'
        cur = mysql.connection.cursor()
        ensure_pharmacy_license_column(cur)
        pharmacy_code = generate_pharmacy_code(cur)
        if not license_number:
            cur.execute("SELECT COALESCE(MAX(pharmacyId), 0) + 1 FROM pharmacies")
            next_pharmacy_id = cur.fetchone()[0]
            license_number = f"KER-LIC-{next_pharmacy_id:04d}"
        cur.execute("""
            INSERT INTO pharmacies (pharmacy_code, name, location, contactNumber, licenseNumber, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (pharmacy_code, name, location, phone, license_number, status))
        mysql.connection.commit()
        cur.close()
        return redirect(url_for('user.view_pharmacies'))
    return render_template('dashboards/add_pharmacy.html')


@user_bp.route('/admin/pharmacies/edit/<int:pharmacy_id>', methods=['GET', 'POST'])
@role_required('system_admin')
def edit_pharmacy(pharmacy_id):
    cur = mysql.connection.cursor()
    ensure_pharmacy_license_column(cur)

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        location = request.form.get('location', '').strip()
        phone = request.form.get('contactNumber', '').strip()
        license_number = request.form.get('licenseNumber', '').strip().upper()
        status = request.form.get('status', 'active').strip().lower()
        if status not in ('active', 'inactive'):
            status = 'active'
        if not license_number:
            license_number = f"KER-LIC-{pharmacy_id:04d}"

        cur.execute("""
            UPDATE pharmacies
            SET name=%s, location=%s, contactNumber=%s, licenseNumber=%s, status=%s
            WHERE pharmacyId=%s
        """, (name, location, phone, license_number, status, pharmacy_id))
        mysql.connection.commit()
        cur.close()
        return redirect(url_for('user.view_pharmacies'))

    cur.execute("""
        SELECT pharmacyId, pharmacy_code, name, location, contactNumber, licenseNumber, status
        FROM pharmacies
        WHERE pharmacyId=%s
    """, (pharmacy_id,))
    pharmacy = cur.fetchone()
    cur.close()
    if not pharmacy:
        return redirect(url_for('user.view_pharmacies'))

    return render_template('dashboards/edit_pharmacy.html', pharmacy=pharmacy)


@user_bp.route('/admin/pharmacies/delete/<int:pharmacy_id>')
@role_required('system_admin')
def delete_pharmacy(pharmacy_id):
    cur = mysql.connection.cursor()
    cur.execute("DELETE FROM pharmacies WHERE pharmacyId=%s", (pharmacy_id,))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.view_pharmacies'))


# ══════════════════════════════════════════
# ROLE DASHBOARDS
# ══════════════════════════════════════════


@user_bp.route('/doctor_dashboard')
@role_required('doctor')
def doctor_dashboard():
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return render_template('dashboards/doctor_dashboard.html',
                               total_prescriptions=0,
                               pending_prescriptions=0,
                               dispensed_prescriptions=0,
                               total_patients=0,
                               recent_prescriptions=[],
                               pending_appointments=0)

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)

    cur.execute("SELECT COUNT(*) FROM prescriptions WHERE doctorId=%s", (doctor_id,))
    total_prescriptions = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM prescriptions
        WHERE doctorId=%s AND status IN ('pending', 'routed', 'validated')
    """, (doctor_id,))
    pending_prescriptions = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM prescriptions WHERE doctorId=%s AND status='dispensed'", (doctor_id,))
    dispensed_prescriptions = cur.fetchone()[0]

    # Count unique patients from both prescriptions and appointment bookings.
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT clientId
            FROM prescriptions
            WHERE doctorId=%s
            UNION
            SELECT DISTINCT clientId
            FROM appointments
            WHERE doctorId=%s
        ) x
    """, (doctor_id, doctor_id))
    total_patients = cur.fetchone()[0]

    cur.execute("""
        SELECT p.prescriptionId, c.name, COALESCE(p.uploadedDocumentPath, 'Uploaded Document'),
               DATE_FORMAT(p.createsDate, '%%Y-%%m-%%d'), p.status
        FROM prescriptions p
        JOIN clients c ON c.clientId = p.clientId
        WHERE p.doctorId=%s
        ORDER BY p.prescriptionId DESC
        LIMIT 8
    """, (doctor_id,))
    recent_prescriptions = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*)
        FROM appointments
        WHERE doctorId=%s AND status='pending'
    """, (doctor_id,))
    pending_appointments = cur.fetchone()[0]

    cur.close()
    return render_template('dashboards/doctor_dashboard.html',
                           total_prescriptions=total_prescriptions,
                           pending_prescriptions=pending_prescriptions,
                           dispensed_prescriptions=dispensed_prescriptions,
                           total_patients=total_patients,
                           recent_prescriptions=recent_prescriptions,
                           pending_appointments=pending_appointments)


@user_bp.route('/doctor/prescriptions')
@role_required('doctor')
def doctor_my_prescriptions():
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return render_template('dashboards/doctor_prescriptions.html',
                               total_patients=0,
                               pending_appointments=0,
                               prescriptions=[])

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT clientId FROM prescriptions WHERE doctorId=%s
            UNION
            SELECT DISTINCT clientId FROM appointments WHERE doctorId=%s
        ) x
    """, (doctor_id, doctor_id))
    total_patients = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM appointments
        WHERE doctorId=%s AND status='pending'
    """, (doctor_id,))
    pending_appointments = cur.fetchone()[0]

    prescription_columns = get_table_columns(cur, "prescriptions")
    date_col = first_existing_column(prescription_columns, ["createsDate", "createdAt", "created_date"]) or "createsDate"

    cur.execute(f"""
        SELECT p.prescriptionId,
               c.name,
               COALESCE(p.medicineName, p.uploadedDocumentPath, 'Prescription'),
               DATE_FORMAT(p.{date_col}, '%%Y-%%m-%%d'),
               COALESCE(NULLIF(TRIM(p.status), ''), 'pending')
        FROM prescriptions p
        JOIN clients c ON c.clientId = p.clientId
        WHERE p.doctorId=%s
        ORDER BY p.prescriptionId DESC
    """, (doctor_id,))
    prescriptions = cur.fetchall()

    cur.close()
    return render_template('dashboards/doctor_prescriptions.html',
                           total_patients=total_patients,
                           pending_appointments=pending_appointments,
                           prescriptions=prescriptions)


@user_bp.route('/doctor/prescriptions/write', methods=['GET', 'POST'])
@role_required('doctor')
def doctor_write_prescription():
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return redirect(url_for('user.doctor_dashboard'))

    cur = mysql.connection.cursor()
    ensure_prescription_support_columns(cur)

    if request.method == 'POST':
        client_lookup = request.form.get('client_lookup', '').strip()
        notes = request.form.get('notes', '').strip()

        client_row = resolve_client_by_code_or_id(cur, client_lookup)
        if not client_row:
            cur.close()
            return render_template('dashboards/doctor_write_prescription.html',
                                   error='Client not found. Enter valid client id or client code.')
        client_id, client_code, client_name = client_row

        medicine_names = request.form.getlist('medicine_name[]')
        strengths_mg = request.form.getlist('strength_mg[]')
        dosages = request.form.getlist('dosage[]')
        timings = request.form.getlist('timing[]')
        quantities = request.form.getlist('quantity[]')
        durations = request.form.getlist('duration_days[]')

        rows_to_insert = []
        for i, raw_name in enumerate(medicine_names):
            med_name = (raw_name or '').strip()
            if not med_name:
                continue
            strength = (strengths_mg[i] if i < len(strengths_mg) else '').strip()
            dosage = (dosages[i] if i < len(dosages) else '').strip()
            timing = (timings[i] if i < len(timings) else '').strip()
            duration = (durations[i] if i < len(durations) else '').strip()

            try:
                qty = max(1, int(quantities[i])) if i < len(quantities) and quantities[i] else 1
            except (TypeError, ValueError):
                qty = 1

            strength_text = f"{strength} mg" if strength else ""
            dosage_text = f"{dosage}; {timing}; {duration} days".strip("; ")
            doc_text = f"Rx for {client_name} ({client_code}): {med_name} {strength_text}. Dose: {dosage}. Timing: {timing}. Days: {duration}. {notes}".strip()

            rows_to_insert.append({
                "medicine_name": f"{med_name} {strength_text}".strip(),
                "quantity": qty,
                "dosage_text": dosage_text,
                "document_text": doc_text,
            })

        if not rows_to_insert:
            cur.close()
            return render_template('dashboards/doctor_write_prescription.html',
                                   error='Add at least one medicine.')

        columns = get_table_columns(cur, "prescriptions")
        for row in rows_to_insert:
            insert_prescription_row(cur, columns, {
                "doctor_id": doctor_id,
                "client_id": client_id,
                "pharmacy_id": None,
                "status": "pending",
                "document_text": row["document_text"],
                "medicine_name": row["medicine_name"],
                "quantity": row["quantity"],
                "dosage_text": row["dosage_text"],
                "created_at": None,
            })

        mysql.connection.commit()
        cur.close()
        return render_template('dashboards/doctor_write_prescription.html',
                               success=f'{len(rows_to_insert)} prescription item(s) sent to {client_name} ({client_code}).')

    cur.close()
    return render_template('dashboards/doctor_write_prescription.html')


@user_bp.route('/doctor/medicines/suggest')
@role_required('doctor')
def doctor_medicine_suggest():
    q = request.args.get('q', '').strip().lower()
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)

    if q:
        cur.execute("""
            SELECT DISTINCT name
            FROM medicines
            WHERE isActive=1 AND LOWER(name) LIKE %s
            ORDER BY name ASC
            LIMIT 20
        """, (f"%{q}%",))
    else:
        cur.execute("""
            SELECT DISTINCT name
            FROM medicines
            WHERE isActive=1
            ORDER BY name ASC
            LIMIT 20
        """)
    rows = cur.fetchall()
    cur.close()

    suggestions = [row[0] for row in rows]
    if not suggestions:
        suggestions = [
            "Paracetamol 500", "Dolo 650", "Crocin Advance", "Azithral 500",
            "Augmentin 625", "Cefixime 200", "Pantocid 40", "Ecosprin 75",
            "Atorva 10", "Glycomet 500", "Thyronorm 50", "Montair LC"
        ]
        if q:
            suggestions = [name for name in suggestions if q in name.lower()]

    return jsonify({"suggestions": suggestions[:20]})


@user_bp.route('/doctor/patients')
@role_required('doctor')
def doctor_patients():
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return render_template('dashboards/doctor_patients.html',
                               total_patients=0,
                               pending_appointments=0,
                               patient_list=[])

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)
    cur.execute(f"""
        SELECT c.clientId,
               c.name,
               c.email,
               COALESCE(NULLIF(TRIM(c.phone), ''), '-')
        FROM clients c
        WHERE c.clientId IN (
            SELECT clientId FROM prescriptions WHERE doctorId=%s
            UNION
            SELECT clientId FROM appointments WHERE doctorId=%s
        )
        ORDER BY c.name
    """, (doctor_id, doctor_id))
    patient_list = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*)
        FROM appointments
        WHERE doctorId=%s AND status='pending'
    """, (doctor_id,))
    pending_appointments = cur.fetchone()[0]

    total_patients = len(patient_list)
    cur.close()
    return render_template('dashboards/doctor_patients.html',
                           total_patients=total_patients,
                           pending_appointments=pending_appointments,
                           patient_list=patient_list)


@user_bp.route('/doctor/appointments')
@role_required('doctor')
def doctor_appointments():
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return render_template('dashboards/doctor_appointments.html',
                               total_patients=0,
                               pending_appointments=0,
                               recent_appointments=[],
                               appointment_updated=request.args.get('appointment_updated'))

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT clientId FROM prescriptions WHERE doctorId=%s
            UNION
            SELECT DISTINCT clientId FROM appointments WHERE doctorId=%s
        ) x
    """, (doctor_id, doctor_id))
    total_patients = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM appointments
        WHERE doctorId=%s AND status='pending'
    """, (doctor_id,))
    pending_appointments = cur.fetchone()[0]

    cur.execute("""
        SELECT a.appointmentId, c.name,
               DATE_FORMAT(a.appointmentDate, '%%Y-%%m-%%d'),
               CASE
                   WHEN a.status='pending' OR a.appointmentTime IS NULL THEN '-'
                   ELSE DATE_FORMAT(a.appointmentTime, '%%H:%%i')
               END,
               COALESCE(a.reason, '-'),
               a.status
        FROM appointments a
        JOIN clients c ON c.clientId = a.clientId
        WHERE a.doctorId=%s
        ORDER BY a.appointmentDate DESC, a.appointmentTime DESC
    """, (doctor_id,))
    recent_appointments = cur.fetchall()
    cur.close()
    return render_template('dashboards/doctor_appointments.html',
                           total_patients=total_patients,
                           pending_appointments=pending_appointments,
                           recent_appointments=recent_appointments,
                           appointment_updated=request.args.get('appointment_updated'))


@user_bp.route('/doctor/appointments/<int:appointment_id>/confirm', methods=['POST'])
@role_required('doctor')
def confirm_appointment(appointment_id):
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return redirect(url_for('user.doctor_dashboard'))

    appointment_time = (request.form.get('appointmentTime') or '').strip()
    if not appointment_time:
        return redirect(url_for('user.doctor_appointments', appointment_updated=0))

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)
    cur.execute("""
        UPDATE appointments
        SET appointmentTime=%s,
            status='confirmed'
        WHERE appointmentId=%s AND doctorId=%s AND status='pending'
    """, (appointment_time, appointment_id, doctor_id))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.doctor_appointments', appointment_updated=1))


@user_bp.route('/doctor/appointments/<int:appointment_id>/reject')
@role_required('doctor')
def reject_appointment(appointment_id):
    doctor_id = get_doctor_id_from_session()
    if not doctor_id:
        return redirect(url_for('user.doctor_appointments'))

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)
    cur.execute("""
        UPDATE appointments
        SET status='cancelled'
        WHERE appointmentId=%s AND doctorId=%s AND status='pending'
    """, (appointment_id, doctor_id))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.doctor_appointments', appointment_updated=1))



def _suggest_quantity_from_dosage(dosage_text, fallback_qty=1):
    text = (dosage_text or "").strip().lower()
    try:
        fallback = max(1, int(fallback_qty or 1))
    except (TypeError, ValueError):
        fallback = 1

    if not text:
        return fallback

    dose_match = re.search(r"(\d+)\s*-\s*(\d+)\s*-\s*(\d+)", text)
    day_match = re.search(r"(\d+)\s*day", text)
    if dose_match and day_match:
        per_day = sum(int(v) for v in dose_match.groups())
        days = int(day_match.group(1))
        if per_day > 0 and days > 0:
            return max(1, per_day * days)

    return fallback


def get_pharmacist_pending_prescriptions(cur, pharmacy_id, pharmacist_id, limit=100):
    prescription_columns = get_table_columns(cur, "prescriptions")
    medicine_col = first_existing_column(
        prescription_columns,
        ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"]
    )
    quantity_col = first_existing_column(
        prescription_columns,
        ["quantity", "qty", "prescribedQty", "medicineQty"]
    )
    dosage_col = first_existing_column(
        prescription_columns,
        ["dosageInstructions", "instructions", "dosage"]
    )
    date_col = first_existing_column(
        prescription_columns,
        ["createsDate", "createdAt", "created_date"]
    )
    has_pharmacy_id = "pharmacyId" in prescription_columns
    has_routed_pharmacist_id = "routedPharmacistId" in prescription_columns

    # Pending queue should include only routed items awaiting billing.
    where_parts = ["p.status = 'routed'"]
    params = []
    if has_pharmacy_id:
        where_parts.insert(0, "p.pharmacyId=%s")
        params.append(pharmacy_id)
    if has_routed_pharmacist_id:
        where_parts.append("(p.routedPharmacistId IS NULL OR p.routedPharmacistId=%s)")
        params.append(pharmacist_id)
    where_clause = " AND ".join(where_parts)

    cur.execute(f"SELECT COUNT(*) FROM prescriptions p WHERE {where_clause}", tuple(params))
    pending_queue = cur.fetchone()[0]

    requested_expr = f"COALESCE(NULLIF(TRIM(p.{medicine_col}), ''), 'Prescription Medicine')" if medicine_col else "'Prescription Medicine'"
    date_expr = f"DATE_FORMAT(p.{date_col}, '%%Y-%%m-%%d')" if date_col else "'-'"
    qty_expr = f"COALESCE(p.{quantity_col}, 1)" if quantity_col else "1"
    dosage_expr = f"COALESCE(p.{dosage_col}, '')" if dosage_col else "''"

    cur.execute(f"""
        SELECT p.prescriptionId,
               c.name,
               d.name,
               {requested_expr} AS requested_medicine,
               {date_expr} AS created_date,
               {qty_expr} AS quantity,
               {dosage_expr} AS dosage_text
        FROM prescriptions p
        JOIN clients c ON c.clientId = p.clientId
        JOIN doctors d ON d.doctorId = p.doctorId
        WHERE {where_clause}
        ORDER BY p.prescriptionId DESC
        LIMIT %s
    """, tuple(params + [limit]))
    rows = cur.fetchall()
    enriched_rows = []
    for row in rows:
        prescription_id, client_name, doctor_name, requested_medicine, created_date, quantity, dosage_text = row
        suggested_qty = _suggest_quantity_from_dosage(dosage_text, quantity)
        enriched_rows.append((
            prescription_id,
            client_name,
            doctor_name,
            requested_medicine,
            created_date,
            quantity,
            dosage_text,
            suggested_qty,
        ))
    return pending_queue, enriched_rows


def get_pharmacist_dispense_bills(cur, pharmacy_id, pharmacist_id, limit=200):
    ensure_dispense_bills_table(cur)
    cur.execute("""
        SELECT
            COALESCE(b.invoiceNo, CONCAT('INV-', b.billId)) AS invoice_no,
            b.prescriptionId,
            COALESCE(c.name, 'Client'),
            b.dispensedMedicine,
            b.quantity,
            COALESCE(b.gstRate, 0.00) AS gstRate,
            b.totalAmount,
            COALESCE(p.paymentStatus, 'pending') AS paymentStatus,
            DATE_FORMAT(b.createdAt, '%%Y-%%m-%%d %%H:%%i')
        FROM bills b
        LEFT JOIN clients c ON c.clientId = b.clientId
        LEFT JOIN payments p ON p.billId = b.billId
        WHERE b.pharmacyId=%s AND b.pharmacistId=%s
        ORDER BY b.billId DESC
        LIMIT %s
    """, (pharmacy_id, pharmacist_id, limit))
    return cur.fetchall()


def get_unseen_paid_notifications(cur, pharmacist_id):
    ensure_dispense_bills_table(cur)
    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacistId=%s
          AND paymentStatus='paid'
          AND paymentNotified=0
    """, (pharmacist_id,))
    return cur.fetchone()[0]


@user_bp.route('/pharmacist_dashboard')
@role_required('pharmacist')
def pharmacist_dashboard():
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    ensure_dispense_bills_table(cur)
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacist_dashboard.html',
                               pending_queue=0, dispensed_today=0, low_stock=0, total_medicines=0,
                               pending_prescriptions=[], low_stock_items=[], inventory_items=[],
                               medicine_saved=request.args.get('medicine_saved'),
                               medicine_error=request.args.get('medicine_error'),
                               dispense_success=request.args.get('dispense_success'),
                               dispense_error=request.args.get('dispense_error'),
                               bill_generated=request.args.get('bill_generated'),
                               bill_total=request.args.get('bill_total'),
                               bill_gst=request.args.get('bill_gst'),
                               invoice_no=request.args.get('invoice_no'),
                               substitute_used=request.args.get('substitute_used'),
                               dispensed_name=request.args.get('dispensed_name'))

    # Inventory is managed explicitly per pharmacy; avoid auto-sync/seed here
    # to prevent cross-pharmacy duplication on dashboard loads.

    pending_queue, pending_prescriptions = get_pharmacist_pending_prescriptions(cur, pharmacy_id, pharmacist_id, limit=10)

    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND DATE(createdAt)=CURDATE()
    """, (pharmacy_id,))
    dispensed_today = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT m.medicineId)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1
    """, (pharmacy_id,))
    total_medicines = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT m.medicineId,
                   SUM(i.quantityAvailable) AS totalQty,
                   MAX(i.minStockLevel) AS minRequired
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
            GROUP BY m.medicineId
        ) s
        WHERE s.totalQty <= s.minRequired
    """, (pharmacy_id,))
    low_stock = cur.fetchone()[0]

    cur.execute("""
        SELECT m.name,
               SUM(i.quantityAvailable) AS totalQty,
               MAX(i.minStockLevel) AS minRequired
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
        GROUP BY m.medicineId, m.name
        HAVING SUM(i.quantityAvailable) <= MAX(i.minStockLevel)
        ORDER BY totalQty ASC, m.name ASC
        LIMIT 10
    """, (pharmacy_id,))
    low_stock_items = cur.fetchall()

    cur.execute("""
        SELECT m.name, m.content, COALESCE(m.brandName, '-'), i.quantityAvailable, i.unitPrice, i.minStockLevel
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND i.quantityAvailable > 0
        ORDER BY m.name ASC
        LIMIT 100
    """, (pharmacy_id,))
    inventory_items = cur.fetchall()
    paid_notifications = get_unseen_paid_notifications(cur, pharmacist_id)
    cur.close()

    return render_template('dashboards/pharmacist_dashboard.html',
                           pending_queue=pending_queue,
                           dispensed_today=dispensed_today,
                           low_stock=low_stock,
                           total_medicines=total_medicines,
                           pending_prescriptions=pending_prescriptions,
                           low_stock_items=low_stock_items,
                           inventory_items=inventory_items,
                           medicine_saved=request.args.get('medicine_saved'),
                           medicine_error=request.args.get('medicine_error'),
                           dispense_success=request.args.get('dispense_success'),
                           dispense_error=request.args.get('dispense_error'),
                           bill_generated=request.args.get('bill_generated'),
                           bill_total=request.args.get('bill_total'),
                           bill_gst=request.args.get('bill_gst'),
                           invoice_no=request.args.get('invoice_no'),
                           substitute_used=request.args.get('substitute_used'),
                           dispensed_name=request.args.get('dispensed_name'),
                           paid_notifications=paid_notifications,
                           active_page='dashboard')


@user_bp.route('/pharmacist/pending_queue')
@role_required('pharmacist')
def pharmacist_pending_queue():
    cur = mysql.connection.cursor()
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacist_pending_queue.html',
                               pending_queue=0, pending_prescriptions=[], active_page='pending_queue')

    pending_queue, pending_prescriptions = get_pharmacist_pending_prescriptions(cur, pharmacy_id, pharmacist_id, limit=200)
    cur.close()
    return render_template('dashboards/pharmacist_pending_queue.html',
                           pending_queue=pending_queue,
                           pending_prescriptions=pending_prescriptions,
                           active_page='pending_queue')


@user_bp.route('/pharmacist/inventory')
@role_required('pharmacist')
def pharmacist_inventory():
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacist_inventory.html',
                               inventory_items=[],
                               total_medicines=0,
                               low_stock=0,
                               active_page='inventory')

    # Inventory is managed explicitly per pharmacy; avoid auto-sync/seed here
    # to prevent cross-pharmacy duplication on page loads.

    cur.execute("""
        SELECT COUNT(DISTINCT m.medicineId)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1
    """, (pharmacy_id,))
    total_medicines = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT m.medicineId,
                   SUM(i.quantityAvailable) AS totalQty,
                   MAX(i.minStockLevel) AS minRequired
            FROM inventory i
            JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
            GROUP BY m.medicineId
        ) s
        WHERE s.totalQty <= s.minRequired
    """, (pharmacy_id,))
    low_stock = cur.fetchone()[0]

    cur.execute("""
        SELECT m.name, m.content, COALESCE(m.brandName, '-'), i.quantityAvailable, i.unitPrice, i.minStockLevel
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND i.quantityAvailable > 0
        ORDER BY m.name ASC
        LIMIT 500
    """, (pharmacy_id,))
    inventory_items = cur.fetchall()

    cur.execute("""
        SELECT m.name,
               SUM(i.quantityAvailable) AS totalQty,
               MAX(i.minStockLevel) AS minRequired
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
        GROUP BY m.medicineId, m.name
        HAVING SUM(i.quantityAvailable) <= MAX(i.minStockLevel)
        ORDER BY totalQty ASC, m.name ASC
        LIMIT 50
    """, (pharmacy_id,))
    low_stock_items = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacist_inventory.html',
                           inventory_items=inventory_items,
                           total_medicines=total_medicines,
                           low_stock=low_stock,
                           low_stock_items=low_stock_items,
                           active_page='inventory')


@user_bp.route('/pharmacist/dispensed')
@role_required('pharmacist')
def pharmacist_dispensed():
    cur = mysql.connection.cursor()
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacist_dispensed.html',
                               dispensed_rows=[], active_page='dispensed')

    dispensed_rows = get_pharmacist_dispense_bills(cur, pharmacy_id, pharmacist_id, limit=300)
    cur.close()
    return render_template('dashboards/pharmacist_dispensed.html',
                           dispensed_rows=dispensed_rows,
                           active_page='dispensed')


@user_bp.route('/pharmacist/billing')
@role_required('pharmacist')
def pharmacist_billing():
    cur = mysql.connection.cursor()
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacist_billing.html',
                               bills=[], active_page='billing')

    paid_notifications = get_unseen_paid_notifications(cur, pharmacist_id)
    if paid_notifications:
        cur.execute("""
            UPDATE pharmacy_dispense_bills
            SET paymentNotified=1
            WHERE pharmacistId=%s AND paymentStatus='paid' AND paymentNotified=0
        """, (pharmacist_id,))
        mysql.connection.commit()

    bills = get_pharmacist_dispense_bills(cur, pharmacy_id, pharmacist_id, limit=300)
    cur.close()
    return render_template('dashboards/pharmacist_billing.html',
                           bills=bills,
                           paid_notifications=paid_notifications,
                           active_page='billing')


@user_bp.route('/pharmacist/medicines/add', methods=['POST'])
@role_required('pharmacist')
def add_pharmacist_medicine():
    name = request.form.get('medicine_name', '').strip()
    content = request.form.get('medicine_content', '').strip()
    brand_name = request.form.get('brand_name', '').strip()

    try:
        stock_qty = max(0, int(request.form.get('stock_qty', 0)))
        unit_price = max(0.0, float(request.form.get('unit_price', 0)))
        min_stock_level = max(0, int(request.form.get('min_stock_level', 10)))
    except (TypeError, ValueError):
        return redirect(url_for('user.pharmacist_dashboard', medicine_error='Invalid numeric values for stock or price.'))

    if not name or not content:
        return redirect(url_for('user.pharmacist_dashboard', medicine_error='Medicine name and content are required.'))

    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard', medicine_error='Pharmacist is not mapped to a pharmacy.'))

    medicine_id = get_or_create_medicine_id(cur, name, content, brand_name or None, pharmacy_id=pharmacy_id)
    cur.execute("""
        INSERT INTO inventory
            (pharmacyId, medicineId, quantityAvailable, unitPrice, minStockLevel, isActive)
        VALUES (%s, %s, %s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            quantityAvailable=quantityAvailable + VALUES(quantityAvailable),
            unitPrice=VALUES(unitPrice),
            minStockLevel=VALUES(minStockLevel),
            isActive=1
    """, (pharmacy_id, medicine_id, stock_qty, unit_price, min_stock_level))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.pharmacist_dashboard', medicine_saved=1))


@user_bp.route('/pharmacy_admin/medicines/add', methods=['POST'])
@role_required('pharmacy_admin')
def add_pharmacy_admin_medicine():
    name = request.form.get('medicine_name', '').strip()
    content = request.form.get('medicine_content', '').strip()
    brand_name = request.form.get('brand_name', '').strip()

    try:
        stock_qty = max(0, int(request.form.get('stock_qty', 0)))
        unit_price = max(0.0, float(request.form.get('unit_price', 0)))
        min_stock_level = max(0, int(request.form.get('min_stock_level', 10)))
    except (TypeError, ValueError):
        return redirect(url_for('user.pharmacy_admin_inventory', medicine_error='Invalid numeric values for stock or price.'))

    if not name or not content:
        return redirect(url_for('user.pharmacy_admin_inventory', medicine_error='Medicine name and content are required.'))

    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return redirect(url_for('user.pharmacy_admin_inventory', medicine_error='Pharmacy admin is not mapped to a pharmacy.'))

    medicine_id = get_or_create_medicine_id(cur, name, content, brand_name or None, pharmacy_id=pharmacy_id)
    cur.execute("""
        INSERT INTO inventory
            (pharmacyId, medicineId, quantityAvailable, unitPrice, minStockLevel, isActive)
        VALUES (%s, %s, %s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            quantityAvailable=quantityAvailable + VALUES(quantityAvailable),
            unitPrice=VALUES(unitPrice),
            minStockLevel=VALUES(minStockLevel),
            isActive=1
    """, (pharmacy_id, medicine_id, stock_qty, unit_price, min_stock_level))
    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.pharmacy_admin_inventory', medicine_saved=1))


@user_bp.route('/pharmacist/dispense/<int:prescription_id>', methods=['POST'])
@role_required('pharmacist')
def dispense_prescription(prescription_id):
    requested_medicine = request.form.get('requested_medicine', '').strip()
    try:
        quantity = max(1, int(request.form.get('quantity', 1)))
    except (TypeError, ValueError):
        quantity = 1

    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    ensure_dispense_bills_table(cur)

    pharmacist_id, pharmacy_id = get_pharmacist_context(cur)
    if not pharmacist_id or not pharmacy_id:
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard', dispense_error='Pharmacist is not mapped to a pharmacy.'))

    prescription_columns = get_table_columns(cur, "prescriptions")
    medicine_col = first_existing_column(
        prescription_columns,
        ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"]
    )
    has_client_id = "clientId" in prescription_columns
    has_pharmacy_id = "pharmacyId" in prescription_columns
    has_status = "status" in prescription_columns
    has_routed_pharmacist_id = "routedPharmacistId" in prescription_columns

    where_parts = ["prescriptionId=%s"]
    params = [prescription_id]
    if has_pharmacy_id:
        where_parts.append("pharmacyId=%s")
        params.append(pharmacy_id)
    if has_routed_pharmacist_id:
        where_parts.append("(routedPharmacistId IS NULL OR routedPharmacistId=%s)")
        params.append(pharmacist_id)
    where_clause = " AND ".join(where_parts)

    selected_requested = f", {medicine_col}" if medicine_col else ""
    selected_client = ", clientId" if has_client_id else ""
    selected_status = ", status" if has_status else ""
    cur.execute(f"SELECT prescriptionId{selected_requested}{selected_client}{selected_status} FROM prescriptions WHERE {where_clause} LIMIT 1", tuple(params))
    prescription = cur.fetchone()
    if not prescription:
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard', dispense_error='Prescription not found for this pharmacy.'))

    idx = 1
    if medicine_col and not requested_medicine:
        requested_medicine = (prescription[idx] or '').strip()
        idx += 1
    elif medicine_col:
        idx += 1

    client_id = prescription[idx] if has_client_id else None
    if has_client_id:
        idx += 1

    current_status = prescription[idx] if has_status else None
    if has_status and current_status not in ('routed', 'validated'):
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard',
                                dispense_error='Only routed/validated prescriptions can be dispensed.'))

    medicine_row, is_substitute = find_medicine_or_substitute(cur, pharmacy_id, requested_medicine, quantity)
    if not medicine_row:
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard',
                                dispense_error=f'No stock available for {requested_medicine} or any same-content alternative.'))

    medicine_id, dispensed_name, dispensed_content, stock_qty, unit_price = medicine_row
    try:
        gst_rate = max(0.0, float(request.form.get('gst_rate', 12.0)))
    except (TypeError, ValueError):
        gst_rate = 12.0

    subtotal_amount = float(unit_price) * quantity
    gst_amount = round(subtotal_amount * gst_rate / 100.0, 2)
    total_amount = round(subtotal_amount + gst_amount, 2)
    try:
        # Lock inventory row to avoid concurrent stock updates for same medicine.
        cur.execute("""
            SELECT quantityAvailable
            FROM inventory
            WHERE medicineId=%s AND pharmacyId=%s
            FOR UPDATE
        """, (medicine_id, pharmacy_id))
        locked_row = cur.fetchone()
        if not locked_row or int(locked_row[0]) < quantity:
            mysql.connection.rollback()
            cur.close()
            return redirect(url_for('user.pharmacist_dashboard', dispense_error='Insufficient stock while dispensing. Please retry.'))

        # Bill is generated before final dispense/stock deduction.
        cur.execute("""
            INSERT INTO pharmacy_dispense_bills
                (invoiceNo, prescriptionId, pharmacyId, pharmacistId, clientId, requestedMedicine, dispensedMedicine,
                 dispensedContent, quantity, unitPrice, subtotalAmount, gstRate, gstAmount, totalAmount, paymentStatus)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'generated')
        """, (None, prescription_id, pharmacy_id, pharmacist_id, client_id, requested_medicine or dispensed_name,
              dispensed_name, dispensed_content, quantity, unit_price, subtotal_amount, gst_rate, gst_amount, total_amount))

        cur.execute("""
            UPDATE inventory
            SET quantityAvailable = quantityAvailable - %s
            WHERE medicineId=%s AND pharmacyId=%s
        """, (quantity, medicine_id, pharmacy_id))

        if has_status:
            # Bill generated and sent to client; wait for payment before marking dispensed.
            cur.execute("UPDATE prescriptions SET status='validated' WHERE prescriptionId=%s", (prescription_id,))

        mysql.connection.commit()
        cur.close()
        return redirect(url_for('user.pharmacist_dashboard',
                                dispense_success=1,
                                bill_generated=1,
                                bill_total=f"{total_amount:.2f}",
                                bill_gst=f"{gst_amount:.2f}",
                                invoice_no='Pending payment',
                                substitute_used=1 if is_substitute else 0,
                                dispensed_name=dispensed_name))
    except Exception as exc:
        mysql.connection.rollback()
        cur.close()
        err_code = exc.args[0] if getattr(exc, "args", None) else None
        if err_code in (1205, 1213):
            return redirect(url_for('user.pharmacist_dashboard',
                                    dispense_error='Another billing/dispense operation is in progress. Please retry in a moment.'))
        return redirect(url_for('user.pharmacist_dashboard',
                                dispense_error=f'Dispense failed: {str(exc)}'))


@user_bp.route('/pharmacy_admin_dashboard')
@role_required('pharmacy_admin')
def pharmacy_admin_dashboard():
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    ensure_dispense_bills_table(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_dashboard.html',
                               total_medicines=0, staff_count=0, today_sales=0, low_stock=0,
                               staff_list=[], recent_orders=[], active_page='dashboard')

    # Inventory is managed explicitly per pharmacy; avoid auto-sync/seed here
    # to prevent cross-pharmacy duplication on dashboard loads.

    cur.execute("""
        SELECT COUNT(*)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1
    """, (pharmacy_id,))
    total_medicines = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacists
        WHERE pharmacyId=%s
    """, (pharmacy_id,))
    staff_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND DATE(createdAt)=CURDATE()
    """, (pharmacy_id,))
    today_sales = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND i.quantityAvailable <= i.minStockLevel
    """, (pharmacy_id,))
    low_stock = cur.fetchone()[0]

    cur.execute("""
        SELECT name, 'pharmacist'
        FROM pharmacists
        WHERE pharmacyId=%s
        ORDER BY name
        LIMIT 10
    """, (pharmacy_id,))
    staff_list = cur.fetchall()

    cur.execute("""
        SELECT dispensedMedicine, quantity, paymentStatus
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
        ORDER BY createdAt DESC
        LIMIT 10
    """, (pharmacy_id,))
    recent_orders = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacy_admin_dashboard.html',
                           total_medicines=total_medicines,
                           staff_count=staff_count,
                           today_sales=today_sales,
                           low_stock=low_stock,
                           staff_list=staff_list,
                           recent_orders=recent_orders,
                           active_page='dashboard')


@user_bp.route('/pharmacy_admin/staff')
@role_required('pharmacy_admin')
def pharmacy_admin_manage_staff():
    cur = mysql.connection.cursor()
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_manage_staff.html',
                               staff_list=[],
                               staff_count=0,
                               active_page='manage_staff')

    cur.execute("""
        SELECT pharmacistId,
               COALESCE(pharmacist_code, '-'),
               name,
               email,
               COALESCE(phone, '-'),
               COALESCE(licenseNumber, '-')
        FROM pharmacists
        WHERE pharmacyId=%s
        ORDER BY name ASC
    """, (pharmacy_id,))
    staff_list = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacy_admin_manage_staff.html',
                           staff_list=staff_list,
                           staff_count=len(staff_list),
                           active_page='manage_staff')


@user_bp.route('/pharmacy_admin/inventory')
@role_required('pharmacy_admin')
def pharmacy_admin_inventory():
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_inventory.html',
                               inventory_items=[],
                               total_medicines=0,
                               low_stock=0,
                               active_page='inventory')

    # Inventory is managed explicitly per pharmacy; avoid auto-sync/seed here
    # to prevent cross-pharmacy duplication on page loads.

    cur.execute("""
        SELECT COUNT(*)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1
    """, (pharmacy_id,))
    total_medicines = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
          AND i.quantityAvailable <= i.minStockLevel
    """, (pharmacy_id,))
    low_stock = cur.fetchone()[0]

    cur.execute("""
        SELECT m.name, m.content, COALESCE(m.brandName, '-'), i.quantityAvailable, i.unitPrice, i.minStockLevel
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s AND i.isActive=1 AND m.isActive=1
        ORDER BY m.name ASC
        LIMIT 500
    """, (pharmacy_id,))
    inventory_items = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacy_admin_inventory.html',
                           inventory_items=inventory_items,
                           total_medicines=total_medicines,
                           low_stock=low_stock,
                           active_page='inventory')


@user_bp.route('/pharmacy_admin/orders')
@role_required('pharmacy_admin')
def pharmacy_admin_orders():
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_orders.html',
                               reorder_items=[],
                               reorder_count=0,
                               critical_count=0,
                               estimated_cost=0,
                               reorder_success=None,
                               reorder_error=None,
                               active_page='orders')

    # Inventory is managed explicitly per pharmacy; avoid auto-sync/seed here
    # to prevent cross-pharmacy duplication on page loads.

    cur.execute("""
        SELECT
            i.medicineId,
            m.name,
            m.content,
            COALESCE(m.brandName, '-') AS brand_name,
            SUM(i.quantityAvailable) AS total_qty,
            MAX(i.minStockLevel) AS min_stock,
            MAX(i.unitPrice) AS unit_price,
            GREATEST(MAX(i.minStockLevel) - SUM(i.quantityAvailable), 0) AS shortage_qty,
            GREATEST((MAX(i.minStockLevel) * 2) - SUM(i.quantityAvailable), MAX(i.minStockLevel)) AS suggested_order_qty
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.isActive=1
          AND m.isActive=1
        GROUP BY m.name, m.content, COALESCE(m.brandName, '-')
        HAVING SUM(i.quantityAvailable) <= MAX(i.minStockLevel)
        ORDER BY total_qty ASC, m.name ASC
        LIMIT 500
    """, (pharmacy_id,))
    reorder_items = cur.fetchall()
    cur.close()

    reorder_count = len(reorder_items)
    critical_count = sum(1 for item in reorder_items if item[4] <= 0)
    estimated_cost = sum(float(item[8] or 0) * float(item[6] or 0) for item in reorder_items)

    return render_template('dashboards/pharmacy_admin_orders.html',
                           reorder_items=reorder_items,
                           reorder_count=reorder_count,
                           critical_count=critical_count,
                           estimated_cost=round(estimated_cost, 2),
                           reorder_success=request.args.get('reorder_success'),
                           reorder_error=request.args.get('reorder_error'),
                           active_page='orders')


@user_bp.route('/pharmacy_admin/orders/reorder/<int:medicine_id>', methods=['POST'])
@role_required('pharmacy_admin')
def pharmacy_admin_reorder_medicine(medicine_id):
    cur = mysql.connection.cursor()
    ensure_medicine_inventory_tables(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return redirect(url_for('user.pharmacy_admin_orders',
                                reorder_error='Pharmacy admin is not mapped to a pharmacy.'))

    cur.execute("""
        SELECT i.quantityAvailable, i.minStockLevel, m.name
        FROM inventory i
        JOIN medicines m ON m.medicineId = i.medicineId AND m.pharmacyId = i.pharmacyId
        WHERE i.pharmacyId=%s
          AND i.medicineId=%s
          AND i.isActive=1
          AND m.isActive=1
        LIMIT 1
    """, (pharmacy_id, medicine_id))
    row = cur.fetchone()

    if not row:
        cur.close()
        return redirect(url_for('user.pharmacy_admin_orders',
                                reorder_error='Medicine not found in reorder queue.'))

    current_qty, min_stock_level, medicine_name = row
    current_qty = int(current_qty or 0)
    min_stock_level = int(min_stock_level or 0)

    if current_qty > min_stock_level:
        cur.close()
        return redirect(url_for('user.pharmacy_admin_orders',
                                reorder_error='This medicine no longer needs reorder.'))

    reorder_qty = max((min_stock_level * 2) - current_qty, min_stock_level, 1)

    cur.execute("""
        UPDATE inventory
        SET quantityAvailable = quantityAvailable + %s
        WHERE pharmacyId=%s AND medicineId=%s
    """, (reorder_qty, pharmacy_id, medicine_id))
    mysql.connection.commit()
    cur.close()

    return redirect(url_for('user.pharmacy_admin_orders',
                            reorder_success=f'{medicine_name} reordered successfully. Added {reorder_qty} units.'))


@user_bp.route('/pharmacy_admin/sales_reports')
@role_required('pharmacy_admin')
def pharmacy_admin_sales_reports():
    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_sales_reports.html',
                               total_sales=0,
                               today_sales=0,
                               month_sales=0,
                               paid_total=0,
                               pending_total=0,
                               paid_count=0,
                               pending_count=0,
                               daily_sales=[],
                               top_medicines=[],
                               active_page='sales_reports')

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
    """, (pharmacy_id,))
    total_sales = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND DATE(createdAt)=CURDATE()
    """, (pharmacy_id,))
    today_sales = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
          AND YEAR(createdAt)=YEAR(CURDATE())
          AND MONTH(createdAt)=MONTH(CURDATE())
    """, (pharmacy_id,))
    month_sales = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0), COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND paymentStatus='paid'
    """, (pharmacy_id,))
    paid_total, paid_count = cur.fetchone()

    cur.execute("""
        SELECT COALESCE(SUM(totalAmount), 0), COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND paymentStatus!='paid'
    """, (pharmacy_id,))
    pending_total, pending_count = cur.fetchone()

    cur.execute("""
        SELECT DATE_FORMAT(createdAt, '%%Y-%%m-%%d') AS sales_date,
               COUNT(*) AS bills_count,
               COALESCE(SUM(totalAmount), 0) AS sales_total
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
          AND createdAt >= DATE_SUB(CURDATE(), INTERVAL 6 DAY)
        GROUP BY DATE(createdAt)
        ORDER BY DATE(createdAt) DESC
    """, (pharmacy_id,))
    daily_sales = cur.fetchall()

    cur.execute("""
        SELECT COALESCE(dispensedMedicine, '-') AS medicine_name,
               COALESCE(SUM(quantity), 0) AS total_qty,
               COALESCE(SUM(totalAmount), 0) AS total_revenue
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
        GROUP BY dispensedMedicine
        ORDER BY total_revenue DESC, total_qty DESC
        LIMIT 10
    """, (pharmacy_id,))
    top_medicines = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacy_admin_sales_reports.html',
                           total_sales=total_sales,
                           today_sales=today_sales,
                           month_sales=month_sales,
                           paid_total=paid_total,
                           pending_total=pending_total,
                           paid_count=paid_count,
                           pending_count=pending_count,
                           daily_sales=daily_sales,
                           top_medicines=top_medicines,
                           active_page='sales_reports')


@user_bp.route('/pharmacy_admin/billing_overview')
@role_required('pharmacy_admin')
def pharmacy_admin_billing_overview():
    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)
    admin_id, pharmacy_id = get_pharmacy_admin_context(cur)
    if not admin_id or not pharmacy_id:
        cur.close()
        return render_template('dashboards/pharmacy_admin_billing_overview.html',
                               bills=[],
                               total_bills=0,
                               paid_bills=0,
                               pending_bills=0,
                               active_page='billing_overview')

    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s
    """, (pharmacy_id,))
    total_bills = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND paymentStatus='paid'
    """, (pharmacy_id,))
    paid_bills = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM pharmacy_dispense_bills
        WHERE pharmacyId=%s AND paymentStatus!='paid'
    """, (pharmacy_id,))
    pending_bills = cur.fetchone()[0]

    cur.execute("""
        SELECT
            b.billId,
            COALESCE(b.invoiceNo, CONCAT('INV-', b.billId)) AS invoice_no,
            COALESCE(c.name, 'Client') AS client_name,
            COALESCE(p.name, 'Pharmacist') AS pharmacist_name,
            COALESCE(b.dispensedMedicine, '-') AS medicine_name,
            b.quantity,
            b.totalAmount,
            b.paymentStatus,
            DATE_FORMAT(b.createdAt, '%%Y-%%m-%%d %%H:%%i') AS created_at,
            COALESCE(DATE_FORMAT(b.paidAt, '%%Y-%%m-%%d %%H:%%i'), '-') AS paid_at
        FROM pharmacy_dispense_bills b
        LEFT JOIN clients c ON c.clientId = b.clientId
        LEFT JOIN pharmacists p ON p.pharmacistId = b.pharmacistId
        WHERE b.pharmacyId=%s
        ORDER BY b.billId DESC
        LIMIT 500
    """, (pharmacy_id,))
    bills = cur.fetchall()
    cur.close()

    return render_template('dashboards/pharmacy_admin_billing_overview.html',
                           bills=bills,
                           total_bills=total_bills,
                           paid_bills=paid_bills,
                           pending_bills=pending_bills,
                           active_page='billing_overview')


@user_bp.route('/client_dashboard')
@role_required('client')
def client_dashboard():
    client_id = get_client_id_from_session()
    if not client_id:
        return render_template('dashboards/client_dashboard.html',
                               active_prescriptions=0, pending_orders=0,
                               completed_orders=0, total_bills=0,
                               my_prescriptions=[], my_orders=[])

    cur = mysql.connection.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM prescriptions
        WHERE clientId=%s AND status IN ('pending', 'routed', 'validated')
    """, (client_id,))
    active_prescriptions = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM prescriptions
        WHERE clientId=%s AND status IN ('pending', 'routed', 'validated')
    """, (client_id,))
    pending_orders = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM prescriptions
        WHERE clientId=%s AND status='dispensed'
    """, (client_id,))
    completed_orders = cur.fetchone()[0]

    # Billing table is not integrated yet in this DB; keep as 0 for now.
    total_bills = 0

    cur.execute("""
        SELECT p.prescriptionId, d.name, COALESCE(p.uploadedDocumentPath, 'Uploaded Document'),
               DATE_FORMAT(p.createsDate, '%%Y-%%m-%%d'), p.status
        FROM prescriptions p
        JOIN doctors d ON d.doctorId = p.doctorId
        WHERE p.clientId=%s AND p.status IN ('pending', 'routed', 'validated')
        ORDER BY p.prescriptionId DESC
        LIMIT 5
    """, (client_id,))
    my_prescriptions = cur.fetchall()

    cur.execute("""
        SELECT p.prescriptionId, COALESCE(ph.name, 'Unassigned Pharmacy'),
               p.status, DATE_FORMAT(p.createsDate, '%%Y-%%m-%%d')
        FROM prescriptions p
        LEFT JOIN pharmacies ph ON ph.pharmacyId = p.pharmacyId
        WHERE p.clientId=%s
        ORDER BY p.prescriptionId DESC
        LIMIT 5
    """, (client_id,))
    my_orders = cur.fetchall()

    cur.close()
    return render_template('dashboards/client_dashboard.html',
                           active_prescriptions=active_prescriptions,
                           pending_orders=pending_orders,
                           completed_orders=completed_orders,
                           total_bills=total_bills,
                           my_prescriptions=my_prescriptions,
                           my_orders=my_orders)


@user_bp.route('/client/prescriptions/upload', methods=['POST'])
@role_required('client')
def client_upload_prescription():
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Invalid client session.'))

    uploaded_file = request.files.get('prescription_file')
    medicine_name = request.form.get('medicine_name', '').strip()

    if not uploaded_file or not uploaded_file.filename:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Please upload a prescription document.'))

    allowed_extensions = {'.pdf', '.txt', '.doc', '.docx'}
    lower_name = uploaded_file.filename.lower()
    dot_index = lower_name.rfind('.')
    ext = lower_name[dot_index:] if dot_index != -1 else ''
    if ext not in allowed_extensions:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Only document files are allowed: PDF, TXT, DOC, DOCX.'))

    cur = mysql.connection.cursor()
    try:
        ensure_prescription_support_columns(cur)

        # Get doctor
        cur.execute("""
            SELECT p.doctorId
            FROM prescriptions p
            WHERE p.clientId=%s
            ORDER BY p.prescriptionId DESC
            LIMIT 1
        """, (client_id,))
        doctor_row = cur.fetchone()

        if not doctor_row:
            cur.execute("""
                SELECT a.doctorId
                FROM appointments a
                WHERE a.clientId=%s
                ORDER BY a.appointmentId DESC
                LIMIT 1
            """, (client_id,))
            doctor_row = cur.fetchone()

        if not doctor_row:
            cur.execute("SELECT doctorId FROM doctors ORDER BY doctorId ASC LIMIT 1")
            doctor_row = cur.fetchone()

        if not doctor_row:
            cur.close()
            return redirect(url_for('user.client_active_prescriptions',
                                    route_error=1,
                                    route_message='No doctor is available in system.'))

        doctor_id = doctor_row[0]

        extracted_text = extract_prescription_text(uploaded_file)
        if not medicine_name:
            extracted_medicine = extract_medicine_from_text(extracted_text)
            medicine_name = (extracted_medicine or "").strip()
        document_key, _ = upload_prescription_document(uploaded_file, client_id)

        col = get_prescription_columns(cur)
        client_col = col["client"] if col["client"] else "clientId"
        uploaded_col = col["uploaded_document"] if col["uploaded_document"] else "uploadedDocumentPath"
        extracted_col = col["extracted_text"]
        medicine_col = col["medicine"] if col["medicine"] else "medicine_name"

        # Check existing
        cur.execute(f"""
            SELECT prescriptionId
            FROM prescriptions
            WHERE {client_col}=%s
              AND doctorId=%s
              AND status IN ('pending','routed','validated')
            ORDER BY prescriptionId DESC
            LIMIT 1
        """, (client_id, doctor_id))

        existing = cur.fetchone()

        if existing:
            prescription_id = existing[0]

            cur.execute(f"""
                UPDATE prescriptions
                SET {uploaded_col}=%s,
                    {extracted_col}=%s,
                    {medicine_col}=%s   -- ✅ UPDATE MEDICINE
                WHERE prescriptionId=%s
            """, (document_key, extracted_text, medicine_name, prescription_id))

        else:
            columns = get_table_columns(cur, "prescriptions")

            prescription_id = insert_prescription_row(cur, columns, {
                "doctor_id": doctor_id,
                "client_id": client_id,
                "pharmacy_id": None,
                "status": "pending",
                "document_text": document_key,
                "extracted_text": extracted_text,
                "medicine_name": medicine_name,   # ✅ STORE MEDICINE
                "quantity": 1,
                "dosage_text": "Uploaded by client",
                "created_at": None,
            })

        # Save document
        ensure_documents_table(cur)
        uploaded_by = session.get('user_id')
        cur.execute("""
            INSERT INTO documents (prescriptionId, filePath, uploadedBy)
            VALUES (%s, %s, %s)
        """, (prescription_id, document_key, uploaded_by))

        # Routing
        backfill_routing_engine(cur)
        route_result = route_prescription_to_best_pharmacy(cur, prescription_id, client_id)

        mysql.connection.commit()
        cur.close()

        if route_result.get("ok"):
            return redirect(url_for('user.client_active_prescriptions',
                                    route_success=1,
                                    route_message='Prescription uploaded and routed successfully'))

        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Uploaded but routing failed'))

    except Exception as exc:
        mysql.connection.rollback()
        cur.close()
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message=f'Upload failed: {str(exc)}'))


@user_bp.route('/client/book_appointment', methods=['GET', 'POST'])
@user_bp.route('/client/appointments', methods=['GET', 'POST'])

@role_required('client')
def client_book_appointment():
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_dashboard'))

    cur = mysql.connection.cursor()
    ensure_appointments_table(cur)

    if request.method == 'POST':
        doctor_id = request.form.get('doctorId')
        appointment_date = request.form.get('appointmentDate')
        appointment_time = None
        reason = request.form.get('reason', '').strip()
        symptoms = request.form.get('symptoms', '').strip()

        cur.execute("""
            INSERT INTO appointments (clientId, doctorId, appointmentDate, appointmentTime, reason, symptoms, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
        """, (client_id, doctor_id, appointment_date, appointment_time, reason, symptoms))
        mysql.connection.commit()
        cur.close()
        return redirect(url_for('user.client_book_appointment', success=1))

    cur.execute("""
        SELECT doctorId, doctor_code, name, COALESCE(specialization, 'General')
        FROM doctors
        ORDER BY name
    """)
    doctors = cur.fetchall()
    cur.execute("""
        SELECT a.appointmentId, d.name,
               DATE_FORMAT(a.appointmentDate, '%%Y-%%m-%%d'),
               CASE
                   WHEN a.appointmentTime IS NULL THEN '-'
                   ELSE DATE_FORMAT(a.appointmentTime, '%%H:%%i')
               END,
               a.status
        FROM appointments a
        JOIN doctors d ON d.doctorId = a.doctorId
        WHERE a.clientId=%s
        ORDER BY a.appointmentDate DESC, a.createdAt DESC
        LIMIT 10
    """, (client_id,))
    appointments = cur.fetchall()
    cur.close()
    return render_template('dashboards/client_book_appointment.html',
                           doctors=doctors,
                           appointments=appointments,
                           success=request.args.get('success'))


@user_bp.route('/client/active_prescriptions')
@role_required('client')
def client_active_prescriptions():
    client_id = get_client_id_from_session()
    cur = mysql.connection.cursor()
    ensure_prescription_support_columns(cur)

    col = get_prescription_columns(cur)
    prescription_columns = get_table_columns(cur, "prescriptions")
    medicine_col = col["medicine"] if col["medicine"] else "medicineName"
    extracted_col = col["extracted_text"]
    dosage_col = first_existing_column(prescription_columns, ["dosageInstructions", "instructions", "dosage"])
    date_col = first_existing_column(get_table_columns(cur, "prescriptions"), ["createsDate", "createdAt", "created_date"])
    doc_expr = f"COALESCE(p.{col['uploaded_document']}, 'Not Uploaded')" if col["uploaded_document"] else "'Not Uploaded'"
    med_expr = first_non_empty_column_expr(
        "p",
        prescription_columns,
        ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"],
        "'Medicine not specified'"
    )
    qty_expr = f"COALESCE(p.{col['quantity']}, 1)" if col["quantity"] else "1"
    date_expr = f"DATE_FORMAT(p.{date_col}, '%%Y-%%m-%%d')" if date_col else "'-'"
    extracted_expr = f"COALESCE(p.{extracted_col}, '')" if extracted_col else "''"
    dosage_expr = f"COALESCE(p.{dosage_col}, '')" if dosage_col else "''"
    client_where_col = col["client"] if col["client"] else "clientId"
    suppress_pending_duplicates = ""

    if extracted_col:
        cur.execute(f"""
            SELECT prescriptionId, COALESCE(p.{extracted_col}, '')
            FROM prescriptions p
            WHERE p.{client_where_col}=%s
              AND p.status IN ('pending', 'routed', 'validated')
              AND (p.{medicine_col} IS NULL OR TRIM(p.{medicine_col})='')
              AND p.{extracted_col} IS NOT NULL
              AND TRIM(p.{extracted_col})!=''
        """, (client_id,))
        missing_medicine_rows = cur.fetchall()
        for prescription_id, extracted_text in missing_medicine_rows:
            extracted_medicine = extract_medicine_from_text(extracted_text)
            if extracted_medicine:
                cur.execute(
                    f"UPDATE prescriptions SET {medicine_col}=%s WHERE prescriptionId=%s",
                    (extracted_medicine.strip(), prescription_id)
                )
        if missing_medicine_rows:
            mysql.connection.commit()

    cur.execute("""
        SELECT p.prescriptionId,
               d.name,
               {doc_expr},
               {date_expr},
               p.status,
               {med_expr},  # FIXED: Explicit medicineName with proper NULL handling
               {qty_expr},
               COALESCE(ph.name, 'Not Routed'),
               COALESCE(rph.name, '-'),
               {extracted_expr},
               {dosage_expr}
        FROM prescriptions p
        JOIN doctors d ON d.doctorId = p.doctorId
        LEFT JOIN pharmacies ph ON ph.pharmacyId = p.pharmacyId
        LEFT JOIN pharmacists rph ON rph.pharmacistId = p.routedPharmacistId
        WHERE p.{client_where_col}=%s AND p.status IN ('pending', 'routed', 'validated')
          {suppress_pending_duplicates}
        ORDER BY p.prescriptionId DESC
    """.format(doc_expr=doc_expr, date_expr=date_expr, med_expr=med_expr, qty_expr=qty_expr,
               extracted_expr=extracted_expr, dosage_expr=dosage_expr,
               client_where_col=client_where_col, suppress_pending_duplicates=suppress_pending_duplicates), (client_id,))
    prescriptions = []
    for row in cur.fetchall():
        row_data = list(row)
        current_medicine = (row_data[5] or "").strip()
        if not current_medicine or current_medicine == "Medicine not specified":
            extracted_medicine = extract_medicine_from_text(" ".join([
                (row_data[9] or "").strip(),
                (row_data[10] or "").strip()
            ]))
            if extracted_medicine:
                row_data[5] = extracted_medicine
        prescriptions.append(tuple(row_data[:9]))
    cur.close()
    print(f"DEBUG client_active: client_id={client_id}, first2={prescriptions[:2] if prescriptions else None}")
    return render_template('dashboards/client_active_prescriptions.html',
                           prescriptions=prescriptions,
                           route_success=request.args.get('route_success'),
                           route_error=request.args.get('route_error'),
                           route_message=request.args.get('route_message'))


@user_bp.route('/client/prescriptions/<int:prescription_id>/view')
@role_required('client')
def client_view_prescription_document(prescription_id):
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Invalid client session.'))

    cur = mysql.connection.cursor()
    ensure_prescription_support_columns(cur)
    col = get_prescription_columns(cur)
    date_col = first_existing_column(get_table_columns(cur, "prescriptions"), ["createsDate", "createdAt", "created_date"])
    quantity_col = col["quantity"] if col["quantity"] else "quantity"
    client_where_col = col["client"] if col["client"] else "clientId"
    date_expr = f"DATE_FORMAT(p.{date_col}, '%%Y-%%m-%%d')" if date_col else "'-'"
    med_expr = first_non_empty_column_expr(
        "p",
        get_table_columns(cur, "prescriptions"),
        ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"],
        "'-'"
    )

    cur.execute(f"""
        SELECT p.prescriptionId,
               COALESCE(d.name, '-'),
               {med_expr},
               COALESCE(p.{quantity_col}, 1),
               COALESCE(p.status, 'pending'),
               {date_expr}
        FROM prescriptions p
        LEFT JOIN doctors d ON d.doctorId = p.doctorId
        WHERE p.prescriptionId=%s AND p.{client_where_col}=%s
        LIMIT 1
    """, (prescription_id, client_id))
    row = cur.fetchone()
    cur.close()
    if not row:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Prescription not found.'))

    lines = [
        f"Prescription ID: {row[0]}",
        f"Doctor: {row[1]}",
        f"Medicine: {row[2]}",
        f"Quantity: {row[3]}",
        f"Status: {row[4]}",
        f"Date: {row[5]}",
    ]
    return Response("\n".join(lines), mimetype="text/plain")


@user_bp.route('/client/prescriptions/<int:prescription_id>/download')
@role_required('client')
def client_download_prescription(prescription_id):
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Invalid client session.'))

    file_format = (request.args.get('format', 'txt') or 'txt').strip().lower()
    if file_format not in ('txt', 'pdf'):
        file_format = 'txt'

    cur = mysql.connection.cursor()
    ensure_prescription_support_columns(cur)
    col = get_prescription_columns(cur)
    date_col = first_existing_column(get_table_columns(cur, "prescriptions"), ["createsDate", "createdAt", "created_date"])
    quantity_col = col["quantity"] if col["quantity"] else "quantity"
    doc_col = col["uploaded_document"] if col["uploaded_document"] else "uploadedDocumentPath"
    extracted_col = col["extracted_text"]
    client_where_col = col["client"] if col["client"] else "clientId"
    date_expr = f"DATE_FORMAT(p.{date_col}, '%%Y-%%m-%%d')" if date_col else "'-'"
    extracted_expr = f"COALESCE(p.{extracted_col}, '')" if extracted_col else "''"
    med_expr = first_non_empty_column_expr(
        "p",
        get_table_columns(cur, "prescriptions"),
        ["medicineName", "medicine", "medication", "prescribedMedicine", "medicineRequired", "drugName"],
        "'-'"
    )

    cur.execute(f"""
        SELECT p.prescriptionId,
               d.name,
               {med_expr},
               COALESCE(p.{quantity_col}, 1),
               COALESCE(p.{doc_col}, 'Not Uploaded'),
               COALESCE(p.status, 'pending'),
               {date_expr},
               {extracted_expr}
        FROM prescriptions p
        JOIN doctors d ON d.doctorId = p.doctorId
        WHERE p.prescriptionId=%s AND p.{client_where_col}=%s
        LIMIT 1
    """, (prescription_id, client_id))
    row = cur.fetchone()
    cur.close()
    if not row:
        return redirect(url_for('user.client_active_prescriptions',
                                route_error=1,
                                route_message='Prescription not found.'))

    rx_id, doctor_name, medicine_name, qty, doc_text, status, rx_date, extracted_text = row
    lines = [
        f"Prescription ID: {rx_id}",
        f"Doctor: Dr. {doctor_name}",
        f"Medicine: {medicine_name}",
        f"Quantity: {qty}",
        f"Status: {status}",
        f"Date: {rx_date}",
    ]
    txt = "\n".join(lines)

    if file_format == 'pdf':
        pdf_bytes = build_simple_pdf_bytes(f"Prescription #{rx_id}", lines)
        return send_file(
            BytesIO(pdf_bytes),
            as_attachment=True,
            download_name=f"prescription_{rx_id}.pdf",
            mimetype="application/pdf",
        )

    return send_file(
        BytesIO(txt.encode("utf-8")),
        as_attachment=True,
        download_name=f"prescription_{rx_id}.txt",
        mimetype="text/plain; charset=utf-8",
    )


@user_bp.route('/client/prescriptions/<int:prescription_id>/send_to_pharmacy', methods=['POST'])
@role_required('client')
def client_send_prescription_to_pharmacy(prescription_id):
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_active_prescriptions', route_error='Invalid client session.'))

    cur = mysql.connection.cursor()
    ensure_prescription_support_columns(cur)
    col = get_prescription_columns(cur)
    if not col["client"]:
        cur.close()
        return redirect(url_for('user.client_active_prescriptions', route_error='Required prescription columns are missing in database.'))

    cur.execute(f"""
        SELECT p.prescriptionId
        FROM prescriptions p
        WHERE p.prescriptionId=%s AND p.{col['client']}=%s AND p.status IN ('pending','routed','validated')
        LIMIT 1
    """, (prescription_id, client_id))
    if not cur.fetchone():
        cur.close()
        return redirect(url_for('user.client_active_prescriptions', route_error='Prescription not found or not active.'))
    backfill_routing_engine(cur)
    route_result = route_prescription_to_best_pharmacy(cur, prescription_id, client_id)
    mysql.connection.commit()
    cur.close()

    if route_result.get("ok"):
        return redirect(url_for('user.client_active_prescriptions',
                                route_success=1,
                                route_message=route_result.get("message")))

    return redirect(url_for('user.client_active_prescriptions',
                            route_error=1,
                            route_message=route_result.get("reason", "Unable to route right now.")))

@user_bp.route('/client/prescription_history')
@role_required('client')
def client_prescription_history():
    client_id = get_client_id_from_session()
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT p.prescriptionId,
               d.name,
               COALESCE(p.uploadedDocumentPath, 'Uploaded Document'),
               DATE_FORMAT(p.createsDate, '%%Y-%%m-%%d'),
               CASE
                   WHEN LOWER(COALESCE(p.status, '')) = 'dispensed' THEN 'dispensed'
                   WHEN b.prescriptionId IS NOT NULL THEN 'dispensed'
                   ELSE 'pending'
               END AS status
        FROM prescriptions p
        JOIN doctors d ON d.doctorId = p.doctorId
        LEFT JOIN pharmacy_dispense_bills b
            ON b.prescriptionId = p.prescriptionId
           AND b.clientId = p.clientId
        WHERE p.clientId=%s
        ORDER BY p.prescriptionId DESC
    """, (client_id,))
    prescriptions = cur.fetchall()
    cur.close()
    return render_template('dashboards/client_prescription_history.html', prescriptions=prescriptions)


@user_bp.route('/client/orders')
@role_required('client')
def client_orders():
    client_id = get_client_id_from_session()
    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)
    cur.execute("""
        SELECT 
            p.prescriptionId,
            COALESCE(ph.name, 'Unassigned Pharmacy'),
            DATE_FORMAT(p.createsDate, '%%Y-%%m-%%d'),
            CASE 
                WHEN LOWER(COALESCE(p.status, '')) = 'dispensed' THEN 'dispensed'
                WHEN pdb.prescriptionId IS NOT NULL THEN 'dispensed'
                WHEN lb.prescriptionId IS NOT NULL THEN 'dispensed'
                ELSE 'pending'
            END AS status,
            COALESCE(
                DATE_FORMAT(pdb.createdAt, '%%Y-%%m-%%d'),
                DATE_FORMAT(lb.createdAt, '%%Y-%%m-%%d'),
                '-'
            ) AS dispensed_date,
            COALESCE(
                NULLIF(TRIM(pdb.invoiceNo), ''),
                NULLIF(TRIM(lb.invoiceNo), ''),
                CASE WHEN lb.billId IS NOT NULL THEN CONCAT('INV-', lb.billId) END,
                '-'
            ) AS invoice_no
        FROM prescriptions p
        LEFT JOIN pharmacies ph ON ph.pharmacyId = p.pharmacyId
        LEFT JOIN pharmacy_dispense_bills pdb
            ON pdb.prescriptionId = p.prescriptionId
           AND pdb.clientId = p.clientId
        LEFT JOIN bills lb
            ON lb.prescriptionId = p.prescriptionId
           AND lb.clientId = p.clientId
        WHERE p.clientId=%s
        ORDER BY p.prescriptionId DESC
    """, (client_id,))
    orders = cur.fetchall()
    cur.close()
    return render_template('dashboards/client_orders.html', orders=orders)


@user_bp.route('/client/billing_payments')
@role_required('client')
def client_billing_payments():
    client_id = get_client_id_from_session()
    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)

    cur.execute("""
        SELECT
            pdb.billId,
            pdb.prescriptionId,
            pdb.totalAmount,
            pdb.pharmacyId,
            pdb.pharmacistId,
            pdb.clientId,
            pdb.requestedMedicine,
            pdb.dispensedMedicine,
            pdb.dispensedContent,
            pdb.quantity,
            pdb.unitPrice,
            pdb.gstRate
        FROM pharmacy_dispense_bills pdb
        WHERE pdb.clientId=%s
          AND NOT EXISTS (
              SELECT 1
              FROM bills b
              WHERE b.clientId = pdb.clientId
                AND b.prescriptionId = pdb.prescriptionId
          )
    """, (client_id,))
    missing_bill_rows = cur.fetchall()
    for missing_bill in missing_bill_rows:
        (
            bill_id,
            prescription_id,
            total_amount,
            pharmacy_id,
            pharmacist_id,
            missing_client_id,
            requested_medicine,
            dispensed_medicine,
            dispensed_content,
            quantity,
            unit_price,
            gst_rate,
        ) = missing_bill

        cur.execute("""
            SELECT 1
            FROM bills
            WHERE clientId=%s AND prescriptionId=%s
            LIMIT 1
        """, (missing_client_id, prescription_id))
        if cur.fetchone():
            continue

        cur.execute("""
            SELECT 1
            FROM bills
            WHERE billId=%s
            LIMIT 1
        """, (bill_id,))
        duplicate_bill_id = cur.fetchone() is not None

        if duplicate_bill_id:
            cur.execute("""
                INSERT INTO bills
                    (prescriptionId, totalAmount, pharmacyId, pharmacistId, clientId,
                     requestedMedicine, dispensedMedicine, dispensedContent, quantity, unitPrice, gstRate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                prescription_id,
                total_amount,
                pharmacy_id,
                pharmacist_id,
                missing_client_id,
                requested_medicine,
                dispensed_medicine,
                dispensed_content,
                quantity,
                unit_price,
                gst_rate,
            ))
        else:
            cur.execute("""
                INSERT INTO bills
                    (billId, prescriptionId, totalAmount, pharmacyId, pharmacistId, clientId,
                     requestedMedicine, dispensedMedicine, dispensedContent, quantity, unitPrice, gstRate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, missing_bill)

    if missing_bill_rows:
        mysql.connection.commit()

    cur.execute("""
        SELECT
            b.billId,
            COALESCE(NULLIF(TRIM(py.invoiceNo), ''), NULLIF(TRIM(b.invoiceNo), ''), CONCAT('INV-', b.billId)) AS invoice_no,
            COALESCE(b.dispensedMedicine, '-') AS medicine_name,
            b.quantity,
            COALESCE(b.gstRate, 0.00) AS gst_rate,
            COALESCE(b.totalAmount, 0.00) AS total_amount,
            LOWER(COALESCE(py.paymentStatus, 'pending')) AS payment_status,
            COALESCE(py.paymentMethod, '-') AS payment_method,
            DATE_FORMAT(b.createdAt, '%%Y-%%m-%%d %%H:%%i') AS created_at,
            COALESCE(DATE_FORMAT(py.paidAt, '%%Y-%%m-%%d %%H:%%i'), '-') AS paid_at
        FROM bills b
        LEFT JOIN payments py ON py.billId = b.billId
        WHERE b.clientId=%s
        ORDER BY b.billId DESC
    """, (client_id,))
    bills = cur.fetchall()
    cur.close()
    return render_template('dashboards/client_billing_payments.html',
                           bills=bills,
                           pay_success=request.args.get('pay_success'),
                           pay_error=request.args.get('pay_error'))


@user_bp.route('/client/bills/<int:bill_id>/download')
@role_required('client')
def client_download_bill(bill_id):
    client_id = get_client_id_from_session()
    if not client_id:
        return redirect(url_for('user.client_billing_payments', pay_error='Invalid client session.'))

    file_format = (request.args.get('format', 'pdf') or 'pdf').strip().lower()
    if file_format not in ('pdf', 'txt'):
        file_format = 'pdf'

    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)
    cur.execute("""
        SELECT
            b.billId,
            COALESCE(b.invoiceNo, CONCAT('INV-', b.billId)) AS invoice_no,
            b.prescriptionId,
            COALESCE(b.requestedMedicine, '-'),
            COALESCE(b.dispensedMedicine, '-'),
            COALESCE(b.dispensedContent, '-'),
            b.quantity,
            b.unitPrice,
            b.subtotalAmount,
            b.gstRate,
            b.gstAmount,
            b.totalAmount,
            b.paymentStatus,
            COALESCE(b.paymentMethod, '-'),
            DATE_FORMAT(b.createdAt, '%%Y-%%m-%%d %%H:%%i'),
            COALESCE(DATE_FORMAT(b.paidAt, '%%Y-%%m-%%d %%H:%%i'), '-'),
            COALESCE(ph.name, '-'),
            COALESCE(pr.name, '-')
        FROM pharmacy_dispense_bills b
        LEFT JOIN pharmacies ph ON ph.pharmacyId = b.pharmacyId
        LEFT JOIN pharmacists pr ON pr.pharmacistId = b.pharmacistId
        WHERE b.billId=%s AND b.clientId=%s
        LIMIT 1
    """, (bill_id, client_id))
    row = cur.fetchone()

    if not row:
        cur.execute("""
            SELECT
                b.billId,
                COALESCE(NULLIF(TRIM(py.invoiceNo), ''), NULLIF(TRIM(b.invoiceNo), ''), CONCAT('INV-', b.billId)) AS invoice_no,
                b.prescriptionId,
                COALESCE(b.requestedMedicine, '-'),
                COALESCE(b.dispensedMedicine, '-'),
                COALESCE(b.dispensedContent, '-'),
                b.quantity,
                b.unitPrice,
                b.subtotalAmount,
                b.gstRate,
                b.gstAmount,
                b.totalAmount,
                COALESCE(py.paymentStatus, 'pending'),
                COALESCE(py.paymentMethod, '-'),
                DATE_FORMAT(b.createdAt, '%%Y-%%m-%%d %%H:%%i'),
                COALESCE(DATE_FORMAT(py.paidAt, '%%Y-%%m-%%d %%H:%%i'), '-'),
                COALESCE(ph.name, '-'),
                COALESCE(pr.name, '-')
            FROM bills b
            LEFT JOIN payments py ON py.billId = b.billId
            LEFT JOIN pharmacies ph ON ph.pharmacyId = b.pharmacyId
            LEFT JOIN pharmacists pr ON pr.pharmacistId = b.pharmacistId
            WHERE b.billId=%s AND b.clientId=%s
            LIMIT 1
        """, (bill_id, client_id))
        row = cur.fetchone()
    cur.close()

    if not row:
        return redirect(url_for('user.client_billing_payments', pay_error='Invoice not found for this client.'))

    (
        _, invoice_no, prescription_id, requested_medicine, dispensed_medicine, dispensed_content,
        quantity, unit_price, subtotal_amount, gst_rate, gst_amount, total_amount,
        payment_status, payment_method, created_at, paid_at, pharmacy_name, pharmacist_name
    ) = row

    lines = [
        f"Invoice No: {invoice_no}",
        f"Bill ID: {bill_id}",
        f"Prescription ID: {prescription_id}",
        f"Pharmacy: {pharmacy_name}",
        f"Pharmacist: {pharmacist_name}",
        "",
        f"Requested Medicine: {requested_medicine}",
        f"Dispensed Medicine: {dispensed_medicine}",
        f"Content: {dispensed_content}",
        f"Quantity: {quantity}",
        f"Unit Price: {unit_price}",
        f"Subtotal: {subtotal_amount}",
        f"GST ({gst_rate}%): {gst_amount}",
        f"Total: {total_amount}",
        "",
        f"Payment Status: {payment_status}",
        f"Payment Method: {payment_method}",
        f"Created At: {created_at}",
        f"Paid At: {paid_at}",
    ]

    if file_format == 'txt':
        txt = "\n".join(lines)
        return send_file(
            BytesIO(txt.encode("utf-8")),
            as_attachment=True,
            download_name=f"invoice_{invoice_no}.txt",
            mimetype="text/plain; charset=utf-8",
        )

    pdf_bytes = build_simple_pdf_bytes(f"Invoice {invoice_no}", lines)
    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"invoice_{invoice_no}.pdf",
        mimetype="application/pdf",
    )


@user_bp.route('/client/bills/<int:bill_id>/pay', methods=['POST'])
@role_required('client')
def client_pay_bill(bill_id):
    client_id = get_client_id_from_session()
    payment_method = request.form.get('payment_method', '').strip().lower()
    allowed_methods = {'upi', 'card', 'netbanking', 'cash'}
    if payment_method not in allowed_methods:
        return redirect(url_for('user.client_billing_payments', pay_error='Please choose a valid payment method.'))

    cur = mysql.connection.cursor()
    ensure_dispense_bills_table(cur)
    cur.execute("""
        SELECT b.billId,
               COALESCE(NULLIF(TRIM(py.invoiceNo), ''), NULLIF(TRIM(b.invoiceNo), ''), ''),
               LOWER(COALESCE(py.paymentStatus, 'pending')),
               b.prescriptionId,
               b.totalAmount,
               b.pharmacyId,
               b.pharmacistId,
               b.clientId,
               b.requestedMedicine,
               b.dispensedMedicine,
               b.dispensedContent,
               b.quantity,
               b.unitPrice,
               COALESCE(b.subtotalAmount, 0.00),
               b.gstRate,
               COALESCE(b.gstAmount, 0.00)
        FROM bills b
        LEFT JOIN payments py ON py.billId = b.billId
        WHERE b.billId=%s AND b.clientId=%s
        LIMIT 1
    """, (bill_id, client_id))
    row = cur.fetchone()
    if not row:
        cur.close()
        return redirect(url_for('user.client_billing_payments', pay_error='Bill not found for this client.'))

    (
        _,
        current_invoice,
        payment_status,
        prescription_id,
        total_amount,
        pharmacy_id,
        pharmacist_id,
        client_id,
        requested_medicine,
        dispensed_medicine,
        dispensed_content,
        quantity,
        unit_price,
        subtotal_amount,
        gst_rate,
        gst_amount,
    ) = row
    if payment_status == 'paid':
        cur.close()
        return redirect(url_for('user.client_billing_payments', pay_success='Bill already paid.'))

    invoice_no = current_invoice.strip() if current_invoice else ""
    if not invoice_no:
        invoice_no = f"INV-{prescription_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    cur.execute("""
        UPDATE bills
        SET invoiceNo=%s
        WHERE billId=%s AND clientId=%s
    """, (invoice_no, bill_id, client_id))
    target_bill_id = bill_id

    cur.execute("""
        UPDATE pharmacy_dispense_bills
        SET invoiceNo=%s,
            paymentStatus='paid',
            paymentMethod=%s,
            paidAt=NOW(),
            paymentNotified=0
        WHERE (billId=%s OR prescriptionId=%s) AND clientId=%s
    """, (invoice_no, payment_method, bill_id, prescription_id, client_id))

    # Record payment in the payments table (if present) for reporting.
    cur.execute("SHOW TABLES LIKE 'payments'")
    if cur.fetchone():
        exists = False
        cur.execute("""
            SELECT 1
            FROM payments
            WHERE billId=%s AND clientId=%s
            LIMIT 1
        """, (target_bill_id, client_id))
        exists = cur.fetchone() is not None

        if not exists:
            cur.execute("""
                INSERT INTO payments
                    (billId, clientId, paymentMethod, paymentStatus, paidAt, createdAt, invoiceNo)
                VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
            """, (target_bill_id, client_id, payment_method, "paid", invoice_no))
        else:
            cur.execute("""
                UPDATE payments
                SET paymentMethod=%s,
                    paymentStatus='paid',
                    paidAt=NOW(),
                    invoiceNo=%s
                WHERE billId=%s AND clientId=%s
            """, (payment_method, invoice_no, target_bill_id, client_id))

    # Once payment is done, mark prescription as dispensed.
    cur.execute("""
        UPDATE prescriptions
        SET status='dispensed'
        WHERE prescriptionId=%s
    """, (prescription_id,))

    mysql.connection.commit()
    cur.close()
    return redirect(url_for('user.client_billing_payments',
                            pay_success=f'Payment successful. Invoice {invoice_no} generated and pharmacist notified.'))


@user_bp.route('/client/profile', methods=['GET', 'POST'])
@role_required('client')
def client_profile():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        date_of_birth = request.form.get('dateOfBirth') or None
        phone = request.form.get('phone', '').strip()
        address = request.form.get('address', '').strip()
        allergies = request.form.get('allergies', '').strip()

        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE clients
            SET name=%s, dateOfBirth=%s, phone=%s, address=%s, allergies=%s
            WHERE userId=%s
        """, (name, date_of_birth, phone, address, allergies, session.get('user_id')))
        cur.execute("UPDATE users SET name=%s WHERE userId=%s", (name, session.get('user_id')))
        mysql.connection.commit()
        cur.close()

        session['name'] = name
        return redirect(url_for('user.client_profile', success=1))

    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT c.client_code, c.name, c.email, c.dateOfBirth, c.phone, c.address, c.allergies
        FROM clients c
        WHERE c.userId=%s
    """, (session.get('user_id'),))
    profile = cur.fetchone()
    cur.close()
    return render_template('dashboards/client_profile.html',
                           profile=profile,
                           success=request.args.get('success'))

