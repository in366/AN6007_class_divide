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
MONTHLY_READINGS_DIR = "month_readings"  # Directory for monthly analysis files
MONTHLY_CONSUMPTION_FILE = "monthly_consumption.csv"  # Historical monthly power consumption archive

# Ensure required directories exist
os.makedirs(DAILY_READINGS_DIR, exist_ok=True)
os.makedirs(MONTHLY_READINGS_DIR, exist_ok=True)

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

def ensure_month_directories(date):
    """Ensure directories for the specified month exist"""
    month_dir_daily = os.path.join(DAILY_READINGS_DIR, date.strftime("%Y%m"))
    os.makedirs(month_dir_daily, exist_ok=True)

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
    month_dir = os.path.join(DAILY_READINGS_DIR, f"{year}{month:02d}")
    if not os.path.exists(month_dir):
        return []
    
    # Process daily files for the month
    monthly_data = []
    daily_files = [f for f in os.listdir(month_dir) if f.startswith('readings_')]
    
    if not daily_files:
        return []
    
    # Get account information
    accounts = {str(acc["meter_ID"]): acc for acc in load_accounts()}
    
    # Process each meter's data
    for meter_id in accounts:
        readings = []
        for file in daily_files:
            file_path = os.path.join(month_dir, file)
            df = pd.read_csv(file_path)
            meter_readings = df[df['meter_ID'].astype(str) == meter_id]['meter_value']
            if not meter_readings.empty:
                readings.extend(meter_readings)
        
        if readings:
            consumption = max(readings) - min(readings)
            monthly_data.append({
                'year': year,
                'month': month,
                'meter_ID': meter_id,
                'area': accounts[meter_id]['area'],
                'dwelling': accounts[meter_id]['dwelling'],
                'consumption': round(consumption, 3)
            })
    
    return monthly_data

def archive_and_prepare_monthly_data(current_date):
    """Process historical data and prepare new month during maintenance time"""
    # 1. Calculate relevant months
    first_of_current = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_month = first_of_current - datetime.timedelta(days=1)
    last_month_first = last_month.replace(day=1)
    
    # 2. Process last month's data
    last_month_daily_dir = get_month_directory(DAILY_READINGS_DIR, last_month)
    last_month_monthly_dir = get_month_directory(MONTHLY_READINGS_DIR, last_month)
    
    if os.path.exists(last_month_daily_dir):
        # Get all daily files from last month
        daily_files = [f for f in os.listdir(last_month_daily_dir) if f.startswith('readings_')]
        all_readings = []
        
        # Get account information
        accounts = {str(acc["meter_ID"]): acc for acc in load_accounts()}
        
        # Read all daily files
        for file in daily_files:
            file_path = os.path.join(last_month_daily_dir, file)
            df = pd.read_csv(file_path)
            df['meter_ID'] = df['meter_ID'].astype(str)
            df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
            all_readings.append(df)
        
        if all_readings:
            # Combine all readings
            df_combined = pd.concat(all_readings, ignore_index=True)
            df_combined['meter_value'] = df_combined['meter_value'].astype(float)
            
            # Get first and last readings for each meter
            first_readings = df_combined.sort_values('date_time').groupby('meter_ID').first()
            last_readings = df_combined.sort_values('date_time').groupby('meter_ID').last()
            
            # Prepare monthly consumption data
            monthly_consumption_data = []
            
            # Process each meter's data
            for meter_id in first_readings.index:
                if meter_id in accounts:
                    first_reading = first_readings.loc[meter_id]
                    last_reading = last_readings.loc[meter_id]
                    
                    # Calculate monthly consumption
                    consumption = last_reading['meter_value'] - first_reading['meter_value']
                    
                    # Add to monthly consumption summary
                    monthly_consumption_data.append({
                        'meter_ID': meter_id,
                        'month_consumption': round(consumption, 3)
                    })
            
            # Save monthly consumption summary
            if monthly_consumption_data:
                month_summary_file = os.path.join(
                    last_month_monthly_dir,
                    f"monthly_summary_{last_month.strftime('%Y%m')}.csv"
                )
                df_summary = pd.DataFrame(monthly_consumption_data)
                df_summary.to_csv(month_summary_file, sep=';', index=False)
            
            # Process daily area analysis
            daily_area_analysis = []
            
            # Process each day's data
            for date in pd.date_range(start=last_month_first, end=last_month):
                date_str = date.strftime('%Y-%m-%d')
                daily_data = df_combined[df_combined['date'] == date_str]
                
                if not daily_data.empty:
                    # Group by area and dwelling type
                    for meter_id in accounts:
                        meter_data = daily_data[daily_data['meter_ID'] == meter_id]
                        if not meter_data.empty:
                            area = accounts[meter_id]['area']
                            dwelling = accounts[meter_id]['dwelling']
                            
                            # Calculate daily consumption
                            consumption = meter_data['meter_value'].max() - meter_data['meter_value'].min()
                            
                            daily_area_analysis.append({
                                'DateID': date_str,
                                'AreaID': area,
                                'dwelling_type_id': dwelling,
                                'kwh_per_acc': round(consumption, 3)
                            })
            
            # Save area analysis
            if daily_area_analysis:
                area_analysis_file = os.path.join(
                    last_month_monthly_dir,
                    f"area_analysis_{last_month.strftime('%Y%m')}.csv"
                )
                df_area = pd.DataFrame(daily_area_analysis)
                df_area.to_csv(area_analysis_file, sep=';', index=False)
    
    # 3. Clean up old daily readings (only keep current and last month)
    if os.path.exists(DAILY_READINGS_DIR):
        for year_month_dir in os.listdir(DAILY_READINGS_DIR):
            try:
                year = int(year_month_dir[:4])
                month = int(year_month_dir[4:])
                dir_date = datetime.datetime(year, month, 1)
                
                if dir_date < last_month_first:
                    dir_path = os.path.join(DAILY_READINGS_DIR, year_month_dir)
                    shutil.rmtree(dir_path)
            except ValueError:
                continue
    
    # 4. Ensure directory structure for new month
    new_month = current_date.replace(day=1)
    get_month_directory(DAILY_READINGS_DIR, new_month)
    get_month_directory(MONTHLY_READINGS_DIR, new_month)

def process_daily_data(current_date):
    """Process and save daily data"""
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
    
    daily_cache.clear()

@app.route("/current_time", methods=["GET"])
def get_current_time():
    """Get current simulation time"""
    current_time = init_or_load_current_time()
    return jsonify({
        "Current Simulation Time": {
            "Date": current_time.strftime("%Y-%m-%d"),
            "Time": current_time.strftime("%H:%M:%S"),
            "Weekday": current_time.strftime("%A")
        }
    })

@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")

@app.route('/collect')
def collect():
    return render_template('collect.html')

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
    """Handle meter reading collection with configurable time increment"""
    # Get time increment from request
    data = request.get_json()
    increment_unit = data.get('unit', 'days')  # 默认为天
    increment_value = data.get('value', 1)     # 默认增加1个单位
    
    accounts = load_accounts()
    if not accounts:
        return jsonify({"message": "No registered accounts, please register first"}), 400

    current_time = init_or_load_current_time()
    
    # Calculate next time based on increment
    if increment_unit == 'minutes':
        next_time = current_time + datetime.timedelta(minutes=increment_value)
    elif increment_unit == 'hours':
        next_time = current_time + datetime.timedelta(hours=increment_value)
    elif increment_unit == 'days':
        next_time = current_time + datetime.timedelta(days=increment_value)
    elif increment_unit == 'months':
        # Handle month increment
        next_month = current_time.month + increment_value
        next_year = current_time.year + (next_month - 1) // 12
        next_month = ((next_month - 1) % 12) + 1
        next_time = current_time.replace(year=next_year, month=next_month)
    else:
        return jsonify({"message": "Invalid time unit"}), 400
    
    # Generate readings for the time period
    all_readings = []
    current = current_time
    last_processed_date = current.date()
    
    while current < next_time:
        # Determine next reading time (every 30 minutes)
        minutes_to_add = 30 - (current.minute % 30)
        reading_time = current + datetime.timedelta(minutes=minutes_to_add)
        if reading_time > next_time:
            reading_time = next_time
            
        # Generate readings for this time point
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
        
        # If we've moved to a new date, process the previous day's data
        if reading_time.date() > last_processed_date:
            process_daily_data(current.replace(hour=23, minute=59))
            
            # Check if we need to run monthly maintenance
            if reading_time.day == 1 and reading_time.hour == 0:
                archive_and_prepare_monthly_data(reading_time)
            
            last_processed_date = reading_time.date()
            
        current = reading_time
    
    # Process any remaining data for the final day
    if daily_cache:
        process_daily_data(next_time)
    
    # Save the new current time
    save_current_time(next_time)
    
    return jsonify({
        "message": f"Power readings collected successfully from {current_time} to {next_time}",
        "readings_count": len(all_readings),
        "sample_readings": all_readings[:3] if all_readings else [],
        "time_increment": f"{increment_value} {increment_unit}",
        "new_time": next_time.isoformat()
    })


if __name__ == "__main__":
    app.run()