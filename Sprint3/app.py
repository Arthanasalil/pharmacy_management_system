from flask import Flask, render_template, request
from datetime import datetime, timedelta
from flask_mysqldb import MySQL
from dotenv import load_dotenv
from modules.user_management import user_bp
from modules.prescription_management import prescription_bp
from modules.inventory_management import inventory_bp
from modules.reports import (
    report_bp,
    build_sales_report,
    build_inventory_report,
    build_user_report,
    build_prescription_report,
)
from config import Config

load_dotenv()

app = Flask(__name__)
app.secret_key = "pharmacy123"
app.config.from_object(Config)

mysql = MySQL(app)

# Register blueprints
app.register_blueprint(user_bp)
app.register_blueprint(prescription_bp)
app.register_blueprint(inventory_bp)
app.register_blueprint(report_bp)


@app.route("/testdb")
def test_db():
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT 1")
        return "Database connected!"
    except Exception as e:
        return str(e)

@app.route("/reports/system")
def system_report_view():
    sales_report = {"success": False, "data": [], "totals": {}, "period": {}, "note": "Report not loaded."}
    inventory_report = {"success": False, "data": [], "total": 0, "note": "Report not loaded."}
    user_report = {"success": False, "summary": {"total": 0, "active": 0, "inactive": 0, "by_role": {}}, "data": [], "note": "Report not loaded."}
    prescription_report = {"success": False, "summary": {"total": 0, "by_status": {}}, "daily": [], "note": "Report not loaded."}

    sales_from = request.args.get("sales_from")
    sales_to = request.args.get("sales_to")
    sales_custom_range = bool(sales_from or sales_to)

    try:
        if sales_custom_range:
            try:
                date_from = datetime.strptime(sales_from, "%Y-%m-%d") if sales_from else datetime.utcnow() - timedelta(days=30)
                date_to = datetime.strptime(sales_to, "%Y-%m-%d") if sales_to else datetime.utcnow()
            except ValueError:
                date_from = datetime.utcnow() - timedelta(days=30)
                date_to = datetime.utcnow()
                sales_custom_range = False
        else:
            date_from = datetime.utcnow() - timedelta(days=30)
            date_to = datetime.utcnow()

        sales_report = build_sales_report(
            branch="all",
            date_from=date_from,
            date_to=date_to,
            allow_fallback=not sales_custom_range,
        )
    except Exception:
        sales_report["note"] = "Failed to load sales report."

    try:
        inventory_report = build_inventory_report(branch="all")
    except Exception:
        inventory_report["note"] = "Failed to load inventory report."

    try:
        user_report = build_user_report(branch="all")
    except Exception:
        user_report["note"] = "Failed to load user summary."

    try:
        prescription_report = build_prescription_report(date_from=date_from, date_to=date_to)
    except Exception:
        prescription_report["note"] = "Failed to load prescription activity."

    max_sales = max((row.get("total_sales", 0) for row in sales_report.get("data", [])), default=0)
    max_fill = max((row.get("fill_rate", 0) for row in inventory_report.get("data", [])), default=0)
    max_daily_rx = max((row.get("count", 0) for row in prescription_report.get("daily", [])), default=0)

    return render_template(
        "dashboards/system_report.html",
        sales_report=sales_report,
        inventory_report=inventory_report,
        user_report=user_report,
        prescription_report=prescription_report,
        sales_from=sales_from,
        sales_to=sales_to,
        max_sales=max_sales,
        max_fill=max_fill,
        max_daily_rx=max_daily_rx,
    )

if __name__ == "__main__":
    app.run(debug=True)
