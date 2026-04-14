from app import app
from database.db import mysql

def check_medicine_data():
    with app.app_context():
        conn = mysql.connection
        cur = conn.cursor()
        print("=== MEDICINE DATA CHECK (Updated) ===")
        
        print("\n--- Prescription 16 Details ---")
        cur.execute("SELECT prescriptionId, clientId, doctorId, medicineName, status FROM prescriptions WHERE prescriptionId = 16")
        row = cur.fetchone()
        if row:
            print(f"ID:{row[0]} Client:{row[1]} Doctor:{row[2]} Med:'{row[3] or 'NULL'}' Status:{row[4]}")
        else:
            print("Prescription 16 not found")
        
        print("\n--- ACTIVE Prescriptions WITHOUT Medicine ---")
        cur.execute("""
            SELECT prescriptionId, clientId, doctorId, status 
            FROM prescriptions 
            WHERE status IN ('pending','routed','validated') 
            AND (medicineName IS NULL OR TRIM(COALESCE(medicineName, '')) = '')
            ORDER BY prescriptionId DESC LIMIT 5
        """)
        empty_active = cur.fetchall()
        for row in empty_active:
            print(f"ID:{row[0]} Client:{row[1]} Doctor:{row[2]} Status:{row[3]}")
        
        print("\n--- Top 10 Prescriptions WITH Medicine ---")
        cur.execute("""
            SELECT prescriptionId, clientId, doctorId, medicineName, status 
            FROM prescriptions 
            WHERE medicineName IS NOT NULL AND TRIM(medicineName) != '' 
            ORDER BY prescriptionId DESC LIMIT 10
        """)
        data = cur.fetchall()
        for row in data:
            print(f"ID:{row[0]} Client:{row[1]} Doctor:{row[2]} Med:'{row[3]}' Status:{row[4]}")
        
        cur.close()

if __name__ == "__main__":
    check_medicine_data()
