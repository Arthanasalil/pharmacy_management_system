from flask import Blueprint
import re

inventory_bp = Blueprint('inventory', __name__)


def get_table_columns(cur, table_name):
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row[0] for row in cur.fetchall()}


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


def normalize_medicine_name(name):
    text = (name or "").strip().lower()
    if not text:
        return ""
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
        JOIN medicines m ON m.medicineId = i.medicineId
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
            JOIN medicines m ON m.medicineId = i.medicineId
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
            JOIN medicines m ON m.medicineId = i.medicineId
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
            JOIN medicines m ON m.medicineId = i.medicineId
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
            JOIN medicines m ON m.medicineId = i.medicineId
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
        JOIN medicines m ON m.medicineId = i.medicineId
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
