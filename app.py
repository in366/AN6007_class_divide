"""
Smart Meter Reading API with Flask
Author: wdmzj
Created on: Mon Feb 10 19:56:11 2025
"""

from flask import Flask, request, jsonify, render_template
import json, random, os, datetime, shutil
import pandas as pd
import calendar

app = Flask(__name__)

# File path configurations
ACCOUNTS_FILE = "all_account.json"
CURRENT_TIME_FILE = "current_time.json"
DAILY_READINGS_DIR = "daily_readings"  # Directory organized by month
DENORMALIZED_DIR = "denormalized_readings"  # Directory organized by month
MONTHLY_CONSUMPTION_FILE = "monthly_consumption.csv"  # Historical monthly power consumption archive

# Ensure required directories exist
os.makedirs(DAILY_READINGS_DIR, exist_ok=True)
os.makedirs(DENORMALIZED_DIR, exist_ok=True)

# Memory cache
latest_readings = {}
daily_cache = []

def get_month_directory(base_dir, date):
    """Get the data directory for the specified month"""
    month_dir = os.path.join(base_dir, date.strftime("%Y%m"))
    os.makedirs(month_dir, exist_ok=True)
    return month_dir

def get_daily_file_path(date):
    """Get the file path for daily readings"""
    month_dir = get_month_directory(DAILY_READINGS_DIR, date)
    return os.path.join(month_dir, f"readings_{date.strftime('%Y%m%d')}.csv")

def get_denormalized_file_path(date):
    """Get the path for denormalized data file"""
    month_dir = get_month_directory(DENORMALIZED_DIR, date)
    return os.path.join(month_dir, f"denormalized_{date.strftime('%Y%m')}.csv")

def ensure_month_directories(date):
    """Ensure directories for the specified month exist"""
    month_dir_daily = os.path.join(DAILY_READINGS_DIR, date.strftime("%Y%m"))
    month_dir_denorm = os.path.join(DENORMALIZED_DIR, date.strftime("%Y%m"))
    os.makedirs(month_dir_daily, exist_ok=True)
    os.makedirs(month_dir_denorm, exist_ok=True)

def init_or_load_current_time():
    """Initialize or load the current simulation time"""
    if os.path.exists(CURRENT_TIME_FILE):
        with open(CURRENT_TIME_FILE, "r") as f:
            return datetime.datetime.fromisoformat(json.load(f)["current_time"])
    else:
        initial_time = datetime.datetime(2024, 5, 1)
        save_current_time(initial_time)
        return initial_time

def save_current_time(current_time):
    """Save the current simulation time"""
    with open(CURRENT_TIME_FILE, "w") as f:
        json.dump({"current_time": current_time.isoformat()}, f)

def load_accounts():
    """Load all registered accounts"""
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            try:
                accounts = json.load(f)
            except json.JSONDecodeError:
                accounts = []
    else:
        accounts = []
    return accounts

def save_accounts(accounts):
    """Save account information"""
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)

def append_to_daily_cache(reading):
    """Add reading to daily cache"""
    daily_cache.append(reading)

def is_first_day_maintenance(date):
    """Check if it's maintenance time on the first day of month (0-1 AM)"""
    return date.day == 1 and date.hour == 0

def calculate_monthly_consumption(year, month):
    """Calculate power consumption for specified month"""
    month_dir = os.path.join(DENORMALIZED_DIR, f"{year}{month:02d}")
    if not os.path.exists(month_dir):
        return []
    
    denorm_file = os.path.join(month_dir, f"denormalized_{year}{month:02d}.csv")
    if not os.path.exists(denorm_file):
        return []
    
    # Read monthly data
    df = pd.read_csv(denorm_file)
    
    # Calculate monthly consumption grouped by meter_ID
    monthly_data = []
    for meter_id, group in df.groupby('meter_ID'):
        # Get readings from start and end of month
        readings = group.sort_values('reading_time')
        first_reading = readings.iloc[0]['meter_value']
        last_reading = readings.iloc[-1]['meter_value']
        consumption = last_reading - first_reading
        
        # Get account information
        area = readings.iloc[0]['area']
        dwelling = readings.iloc[0]['dwelling']
        
        monthly_data.append({
            'year': year,
            'month': month,
            'meter_ID': meter_id,
            'area': area,
            'dwelling': dwelling,
            'consumption': round(consumption, 3)
        })
    
    return monthly_data

def archive_and_prepare_monthly_data(current_date):
    """Process historical data and prepare new month during maintenance time"""
    # 1. Calculate relevant months
    first_of_current = current_date.replace(day=1)  # First day of current month
    last_month = first_of_current - datetime.timedelta(days=1)  # Last day of previous month
    last_month_first = last_month.replace(day=1)  # First day of previous month
    two_months_ago = last_month_first - datetime.timedelta(days=1)  # Last day of two months ago
    
    archive_year = two_months_ago.year
    archive_month = two_months_ago.month
    
    # 2. Calculate and archive consumption for two months ago
    monthly_data = calculate_monthly_consumption(archive_year, archive_month)
    if monthly_data:
        df = pd.DataFrame(monthly_data)
        if os.path.exists(MONTHLY_CONSUMPTION_FILE):
            df.to_csv(MONTHLY_CONSUMPTION_FILE, mode='a', header=False, index=False)
        else:
            df.to_csv(MONTHLY_CONSUMPTION_FILE, index=False)
    
    # 3. Clean up data from two months ago
    old_daily_dir = os.path.join(DAILY_READINGS_DIR, f"{archive_year}{archive_month:02d}")
    old_denorm_dir = os.path.join(DENORMALIZED_DIR, f"{archive_year}{archive_month:02d}")
    
    if os.path.exists(old_daily_dir):
        shutil.rmtree(old_daily_dir)
    if os.path.exists(old_denorm_dir):
        shutil.rmtree(old_denorm_dir)
    
    # 4. Ensure directory structure for new month
    new_month = current_date.replace(day=1)
    ensure_month_directories(new_month)

def process_daily_data(current_date):
    """Process daily data, save both simplified readings and complete denormalized data"""
    if not daily_cache:
        return
    
    # Save basic reading data
    basic_data = [{
        "date": current_date.strftime("%Y-%m-%d"),
        "time": datetime.datetime.fromisoformat(reading["reading_time"]).strftime("%H:%M"),
        "meter_ID": reading["meter_ID"],
        "meter_value": reading["meter_value"]
    } for reading in daily_cache]
    
    if basic_data:
        df_basic = pd.DataFrame(basic_data)
        daily_file = get_daily_file_path(current_date)
        os.makedirs(os.path.dirname(daily_file), exist_ok=True)
        
        # Create new file if it's a new month; otherwise append to existing file
        if current_date.day == 1:
            df_basic.to_csv(daily_file, index=False)
        else:
            if os.path.exists(daily_file):
                df_basic.to_csv(daily_file, mode='a', header=False, index=False)
            else:
                df_basic.to_csv(daily_file, index=False)
    
    # Prepare denormalized data
    accounts = {acc["meter_ID"]: acc for acc in load_accounts()}
    denormalized_data = []
    
    for reading in daily_cache:
        meter_id = reading["meter_ID"]
        if meter_id in accounts:
            denormalized_reading = {
                "date": current_date.strftime("%Y-%m-%d"),
                "time": datetime.datetime.fromisoformat(reading["reading_time"]).strftime("%H:%M"),
                "meter_ID": meter_id,
                "area": accounts[meter_id]["area"],
                "dwelling": accounts[meter_id]["dwelling"],
                "meter_value": reading["meter_value"],
                "reading_time": reading["reading_time"]
            }
            denormalized_data.append(denormalized_reading)
    
    # Save denormalized data
    if denormalized_data:
        df_denorm = pd.DataFrame(denormalized_data)
        denorm_file = get_denormalized_file_path(current_date)
        
        # Create new file if it's a new month; otherwise append to existing file
        if current_date.day == 1:
            df_denorm.to_csv(denorm_file, index=False)
        else:
            if os.path.exists(denorm_file):
                df_denorm.to_csv(denorm_file, mode='a', header=False, index=False)
            else:
                df_denorm.to_csv(denorm_file, index=False)
    
    daily_cache.clear()

@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    """Handle meter registration"""
    if request.method == "POST":
        area = request.form.get("area")
        dwelling = request.form.get("dwelling")
        meter_id = str(int(datetime.datetime.now().timestamp() * 1000)) + str(random.randint(100, 999))
        current_time = init_or_load_current_time()
        
        account = {
            "meter_ID": meter_id,
            "area": area,
            "dwelling": dwelling,
            "register_time": current_time.isoformat()
        }
        
        # Initialize reading to 0 at registration
        reading = {
            "meter_ID": meter_id,
            "reading_time": current_time.isoformat(),
            "meter_value": 0
        }
        
        accounts = load_accounts()
        accounts.append(account)
        save_accounts(accounts)
        
        # Save initial reading
        latest_readings[meter_id] = 0
        append_to_daily_cache(reading)
        
        return jsonify({"message": "Registration successful", "account": account})
    else:
        return render_template("register.html")

@app.route("/meter_reading", methods=["POST"])
def meter_reading():
    """Handle meter reading collection"""
    accounts = load_accounts()
    if not accounts:
        return jsonify({"message": "No registered accounts, please register first"}), 400

    current_time = init_or_load_current_time()
    
    # Process current day's data
    process_daily_data(current_time)
    
    # Check if it's maintenance time at start of month
    if is_first_day_maintenance(current_time):
        # Execute monthly maintenance tasks
        archive_and_prepare_monthly_data(current_time)
    
    # Update to next day
    next_day = current_time + datetime.timedelta(days=1)
    save_current_time(next_day)

    all_readings = []
    
    # Ensure directories exist for current month
    ensure_month_directories(next_day)
    
    # Generate readings for new day
    for hour in range(24):
        for minute in [0, 30]:
            reading_time = next_day.replace(hour=hour, minute=minute)
            
            for account in accounts:
                meter_id = account["meter_ID"]
                previous_value = latest_readings.get(meter_id, 0)
                increment = random.uniform(0, 1)
                meter_value = previous_value + increment
                latest_readings[meter_id] = meter_value
                
                reading = {
                    "meter_ID": meter_id,
                    "reading_time": reading_time.isoformat(),
                    "meter_value": round(meter_value, 3)
                }
                all_readings.append(reading)
                append_to_daily_cache(reading)

    return jsonify({
        "message": f"Power readings collected successfully, date: {next_day.date()}",
        "readings_count": len(all_readings),
        "sample_readings": all_readings[:3],
        "daily_processed": True,
        "directories_ensured": True
    })

if __name__ == "__main__":
    app.run()