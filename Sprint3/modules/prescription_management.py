from flask import Blueprint
from flask import render_template

prescription_bp = Blueprint('prescription', __name__)

@prescription_bp.route('/doctor_dashboard')
def doctor_dashboard():
    return render_template('dashboards/doctor_dashboard.html')

@prescription_bp.route('/client_dashboard')
def client_dashboard():
    return render_template('dashboards/client_dashboard.html')
