from modules.inventory_management import ensure_medicine_inventory_tables
import re


def get_table_columns(cur, table_name):
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row[0] for row in cur.fetchall()}


def first_existing_column(columns, candidates):
    for name in candidates:
        if name in columns:
            return name
    return None


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


def get_prescription_columns(cur):
    columns = get_table_columns(cur, "prescriptions")
    return {
        "medicine": first_existing_column(columns, ["medicineName", "medicine", "medication", "prescribedMedicine"]),
        "quantity": first_existing_column(columns, ["quantity", "qty", "prescribedQty", "medicineQty"]),
        "status": first_existing_column(columns, ["status"]),
        "pharmacy": first_existing_column(columns, ["pharmacyId", "pharmacy_id"]),
        "routed_pharmacist": first_existing_column(columns, ["routedPharmacistId"]),
        "uploaded_document": first_existing_column(columns, ["uploadedDocumentPath", "document_path"]),
        "client": first_existing_column(columns, ["clientId", "client_id"]),
    }


def route_prescription_to_best_pharmacy(cur, prescription_id, client_id):
    # Inventory is required for routing; some installs use `medicines` instead of `pharmacy_medicines`.
    cur.execute("SHOW TABLES LIKE 'inventory'")
    if not cur.fetchone():
        return {"ok": False, "reason": "Inventory table not found for routing."}
    ensure_prescription_support_columns(cur)
    col = get_prescription_columns(cur)

    def _get_medicine_table():
        cur.execute("SHOW TABLES LIKE 'medicines'")
        if cur.fetchone():
            return "medicines"
        cur.execute("SHOW TABLES LIKE 'pharmacy_medicines'")
        if cur.fetchone():
            return "pharmacy_medicines"
        return None

    medicine_table = _get_medicine_table()
    if not medicine_table:
        return {"ok": False, "reason": "No medicines table found for routing."}

    select_fields = ["p.prescriptionId"]
    if col["medicine"]:
        select_fields.append(f"p.{col['medicine']}")
    if col["quantity"]:
        select_fields.append(f"p.{col['quantity']}")
    if col["status"]:
        select_fields.append(f"p.{col['status']}")
    if col["client"]:
        select_fields.append(f"p.{col['client']}")

    where_parts = ["p.prescriptionId=%s"]
    params = [prescription_id]
    if col["client"]:
        where_parts.append(f"p.{col['client']}=%s")
        params.append(client_id)

    cur.execute(f"""
        SELECT {", ".join(select_fields)}
        FROM prescriptions p
        WHERE {" AND ".join(where_parts)}
        LIMIT 1
    """, tuple(params))
    row = cur.fetchone()
    if not row:
        return {"ok": False, "reason": "Prescription not found for this client."}

    idx = 1
    medicine_name = (row[idx] or "").strip() if col["medicine"] else ""
    idx += 1 if col["medicine"] else 0
    quantity = row[idx] if col["quantity"] else 1
    idx += 1 if col["quantity"] else 0
    current_status = row[idx] if col["status"] else "pending"

    try:
        quantity = max(1, int(quantity or 1))
    except (TypeError, ValueError):
        quantity = 1

    if current_status == "dispensed":
        return {"ok": False, "reason": "Prescription already dispensed."}

    cur.execute(f"""
        SELECT content
        FROM {medicine_table}
        WHERE isActive=1 AND LOWER(name)=LOWER(%s)
        LIMIT 1
    """, (medicine_name,))
    content_row = cur.fetchone()
    requested_content = (content_row[0] if content_row else "").strip()

    def _normalize_med_name(name):
        if not name:
            return ""
        name = name.lower()
        # Remove common strength tokens (e.g., 2 mg, 500mg, 5 ml)
        name = re.sub(r"\b\d+(\.\d+)?\s*(mg|ml|mcg|g|iu|units?)\b", "", name)
        name = re.sub(r"[^a-z0-9\s]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    base_name = _normalize_med_name(medicine_name)
    candidate_meds = []
    candidate_med_ids = ()
    if base_name:
        like_term = f"%{base_name}%"
        cur.execute(f"""
            SELECT medicineId, name, content
            FROM {medicine_table}
            WHERE isActive=1 AND LOWER(name) LIKE %s
        """, (like_term,))
        candidate_meds = cur.fetchall() or []
        candidate_med_ids = tuple({m[0] for m in candidate_meds})

    cur.execute("""
        SELECT p.pharmacyId,
               p.name,
               x.primary_pharmacist,
               COALESCE(x.pharmacist_count, 0) AS pharmacist_count
        FROM pharmacies p
        LEFT JOIN (
            SELECT pharmacyId, MIN(pharmacistId) AS primary_pharmacist, COUNT(*) AS pharmacist_count
            FROM pharmacists
            GROUP BY pharmacyId
        ) x ON x.pharmacyId = p.pharmacyId
        WHERE COALESCE(p.status, 'active')='active'
        ORDER BY p.pharmacyId
    """)
    pharmacy_rows = cur.fetchall()
    if not pharmacy_rows:
        return {"ok": False, "reason": "No active pharmacy/pharmacist available."}

    if not medicine_name:
        pharmacy_id, pharmacy_name, pharmacist_id, _ = max(
            pharmacy_rows,
            key=lambda item: (item[3], -item[0])
        )
        if col["pharmacy"] and col["routed_pharmacist"] and col["status"] and pharmacist_id:
            cur.execute(f"""
                UPDATE prescriptions
                SET {col['pharmacy']}=%s,
                    {col['routed_pharmacist']}=%s,
                    {col['status']}='routed'
                WHERE prescriptionId=%s
            """, (pharmacy_id, pharmacist_id, prescription_id))
        elif col["pharmacy"] and col["status"]:
            cur.execute(f"""
                UPDATE prescriptions
                SET {col['pharmacy']}=%s,
                    {col['status']}='routed'
                WHERE prescriptionId=%s
            """, (pharmacy_id, prescription_id))
        pharmacist_name = "Assigned Pharmacist"
        if pharmacist_id:
            cur.execute("SELECT name FROM pharmacists WHERE pharmacistId=%s", (pharmacist_id,))
            ph_row = cur.fetchone()
            pharmacist_name = ph_row[0] if ph_row else pharmacist_name
        return {"ok": True, "message": f"Routed to {pharmacy_name} ({pharmacist_name}). No medicine specified - pharmacy will verify from document."}

    best = None
    for pharmacy_id, pharmacy_name, pharmacist_id, pharmacist_count in pharmacy_rows:
        cur.execute(f"""
            SELECT m.name, m.content, i.quantityAvailable
            FROM inventory i
            JOIN {medicine_table} m ON m.medicineId = i.medicineId
            WHERE i.pharmacyId=%s
              AND i.isActive=1
              AND m.isActive=1
              AND LOWER(m.name)=LOWER(%s)
              AND i.quantityAvailable >= %s
            ORDER BY i.quantityAvailable DESC
            LIMIT 1
        """, (pharmacy_id, medicine_name, quantity))
        exact = cur.fetchone()
        if exact:
            candidate = {
                "priority": 2,
                "pharmacy_id": pharmacy_id,
                "pharmacy_name": pharmacy_name,
                "pharmacist_id": pharmacist_id,
                "pharmacist_count": pharmacist_count,
                "dispense_name": exact[0],
                "content": exact[1],
                "stock": exact[2],
                "substitute": False,
            }
            if best is None or (candidate["priority"], candidate["stock"], candidate["pharmacist_count"]) > (best["priority"], best["stock"], best["pharmacist_count"]):
                best = candidate
            continue

        if requested_content:
            cur.execute(f"""
                SELECT m.name, m.content, i.quantityAvailable
                FROM inventory i
                JOIN {medicine_table} m ON m.medicineId = i.medicineId
                WHERE i.pharmacyId=%s
                  AND i.isActive=1
                  AND m.isActive=1
                  AND LOWER(m.content)=LOWER(%s)
                  AND i.quantityAvailable >= %s
                ORDER BY i.quantityAvailable DESC
                LIMIT 1
            """, (pharmacy_id, requested_content, quantity))
            substitute = cur.fetchone()
            if substitute:
                candidate = {
                    "priority": 1,
                    "pharmacy_id": pharmacy_id,
                    "pharmacy_name": pharmacy_name,
                    "pharmacist_id": pharmacist_id,
                    "pharmacist_count": pharmacist_count,
                    "dispense_name": substitute[0],
                    "content": substitute[1],
                    "stock": substitute[2],
                    "substitute": True,
                }
                if best is None or (candidate["priority"], candidate["stock"], candidate["pharmacist_count"]) > (best["priority"], best["stock"], best["pharmacist_count"]):
                    best = candidate

    if not best and candidate_meds:
        med_ids = candidate_med_ids
        placeholders = ", ".join(["%s"] * len(med_ids))
        cur.execute(f"""
            SELECT p.pharmacyId, p.name, x.primary_pharmacist, x.pharmacist_count,
                   m.name, m.content, i.quantityAvailable
            FROM inventory i
            JOIN {medicine_table} m ON m.medicineId = i.medicineId
            JOIN pharmacies p ON p.pharmacyId = i.pharmacyId
            LEFT JOIN (
                SELECT pharmacyId, MIN(pharmacistId) AS primary_pharmacist, COUNT(*) AS pharmacist_count
                FROM pharmacists
                GROUP BY pharmacyId
            ) x ON x.pharmacyId = p.pharmacyId
            WHERE i.isActive=1
              AND m.isActive=1
              AND i.quantityAvailable >= %s
              AND COALESCE(p.status, 'active')='active'
              AND m.medicineId IN ({placeholders})
            ORDER BY i.quantityAvailable DESC
            LIMIT 1
        """, (quantity, *med_ids))
        approx = cur.fetchone()
        if approx:
            pharmacy_id, pharmacy_name, pharmacist_id, pharmacist_count, med_name, med_content, stock_qty = approx
            candidate = {
                "priority": 0,
                "pharmacy_id": pharmacy_id,
                "pharmacy_name": pharmacy_name,
                "pharmacist_id": pharmacist_id,
                "pharmacist_count": pharmacist_count,
                "dispense_name": med_name,
                "content": med_content,
                "stock": stock_qty,
                "substitute": True,
            }
            best = candidate if best is None else best

    if not best:
        debug_parts = [
            f"medicine_table={medicine_table}",
            f"medicine_name={medicine_name}",
            f"normalized={base_name}",
            f"candidate_meds={len(candidate_meds)}",
        ]
        return {"ok": False, "reason": "No pharmacy has stock for this medicine/content. (" + ", ".join(debug_parts) + ")"}

    if col["pharmacy"] and col["routed_pharmacist"] and col["status"] and best["pharmacist_id"]:
        cur.execute(f"""
            UPDATE prescriptions
            SET {col['pharmacy']}=%s,
                {col['routed_pharmacist']}=%s,
                {col['status']}='routed'
            WHERE prescriptionId=%s
        """, (best["pharmacy_id"], best["pharmacist_id"], prescription_id))
    elif col["pharmacy"] and col["status"]:
        cur.execute(f"""
            UPDATE prescriptions
            SET {col['pharmacy']}=%s,
                {col['status']}='routed'
            WHERE prescriptionId=%s
        """, (best["pharmacy_id"], prescription_id))

    pharmacist_name = "Assigned Pharmacist"
    if best["pharmacist_id"]:
        cur.execute("SELECT name FROM pharmacists WHERE pharmacistId=%s", (best["pharmacist_id"],))
        ph_row = cur.fetchone()
        pharmacist_name = ph_row[0] if ph_row else pharmacist_name
    message = f"Routed to {best['pharmacy_name']} ({pharmacist_name})"
    if best["substitute"]:
        message += f" using same-content option: {best['dispense_name']}."
    else:
        message += "."

    return {"ok": True, "message": message}