from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    unique_id = db.Column(db.String(20), unique=True, nullable=False)
    username = db.Column(db.String(50), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False)
    status = db.Column(db.String(20), default='pending') # pending, approved, cancelled
    # New Column for Timestamp
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
# ==========================
# USERS TABLE
# ==========================

CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role ENUM('system_admin','pharmacy_admin','pharmacist','doctor','client') NOT NULL,
    status VARCHAR(50) DEFAULT 'pending'
);
"""


# ==========================
# PHARMACY TABLE
# ==========================

CREATE_PHARMACY_TABLE = """
CREATE TABLE IF NOT EXISTS pharmacy (
    pharmacy_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100),
    contact_number VARCHAR(20),
    license_number VARCHAR(50),
    status VARCHAR(50)
);
"""


# ==========================
# MEDICINE TABLE
# ==========================

CREATE_MEDICINE_TABLE = """
CREATE TABLE IF NOT EXISTS medicine (
    medicine_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    manufacturer VARCHAR(100),
    price DECIMAL(10,2),
    description TEXT
);
"""


# ==========================
# INVENTORY TABLE
# ==========================

CREATE_INVENTORY_TABLE = """
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id INT AUTO_INCREMENT PRIMARY KEY,
    pharmacy_id INT,
    medicine_id INT,
    quantity_available INT,
    expiry_date DATE,
    FOREIGN KEY (pharmacy_id) REFERENCES pharmacy(pharmacy_id),
    FOREIGN KEY (medicine_id) REFERENCES medicine(medicine_id)
);
"""


# ==========================
# PRESCRIPTION TABLE
# ==========================

CREATE_PRESCRIPTION_TABLE = """
CREATE TABLE IF NOT EXISTS prescription (
    prescription_id INT AUTO_INCREMENT PRIMARY KEY,
    doctor_id INT,
    client_id INT,
    document_path VARCHAR(255),
    status VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (doctor_id) REFERENCES users(user_id),
    FOREIGN KEY (client_id) REFERENCES users(user_id)
);
"""


# ==========================
# ROUTING ENGINE TABLE
# ==========================

CREATE_ROUTING_TABLE = """
CREATE TABLE IF NOT EXISTS routing_engine (
    routing_id INT AUTO_INCREMENT PRIMARY KEY,
    prescription_id INT,
    pharmacy_id INT,
    allocation_status VARCHAR(50),
    FOREIGN KEY (prescription_id) REFERENCES prescription(prescription_id),
    FOREIGN KEY (pharmacy_id) REFERENCES pharmacy(pharmacy_id)
);
"""


# ==========================
# BILL TABLE
# ==========================

CREATE_BILL_TABLE = """
CREATE TABLE IF NOT EXISTS bill (
    bill_id INT AUTO_INCREMENT PRIMARY KEY,
    prescription_id INT,
    total_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    payment_status VARCHAR(50),
    generated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (prescription_id) REFERENCES prescription(prescription_id)
);
"""


# ==========================
# PAYMENT TABLE
# ==========================

CREATE_PAYMENT_TABLE = """
CREATE TABLE IF NOT EXISTS payment (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    bill_id INT,
    client_id INT,
    amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bill_id) REFERENCES bill(bill_id),
    FOREIGN KEY (client_id) REFERENCES users(user_id)
);
"""


from database.db import mysql

def create_tables(app):
    with app.app_context():
        cur = mysql.connection.cursor()

        cur.execute(CREATE_USERS_TABLE)
        cur.execute(CREATE_PHARMACY_TABLE)
        cur.execute(CREATE_MEDICINE_TABLE)
        cur.execute(CREATE_INVENTORY_TABLE)
        cur.execute(CREATE_PRESCRIPTION_TABLE)
        cur.execute(CREATE_ROUTING_TABLE)
        cur.execute(CREATE_BILL_TABLE)
        cur.execute(CREATE_PAYMENT_TABLE)
        cur.execute(CREATE_REPORT_TABLE)

        mysql.connection.commit()
        cur.close()
