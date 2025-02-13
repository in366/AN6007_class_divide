import os
import json
import random
import shutil
import calendar
import datetime
from dataclasses import dataclass
from typing import Dict, List
import pandas as pd
from flask import Flask, request, jsonify, render_template

# ==========================
# 数据结构定义
# ==========================

@dataclass
class MeterReading:
    meter_id: str
    reading_time: str
    meter_value: float

# ==========================
# 目录管理：负责文件夹和路径管理
# ==========================

class DirectoryManager:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.data_dir = os.path.join(base_dir, 'data')
        self.daily_readings_dir = os.path.join(self.data_dir, "daily_readings")
        self.monthly_readings_dir = os.path.join(self.data_dir, "month_readings")
        self.accounts_file = os.path.join(self.data_dir, "all_account.json")
        self.current_time_file = os.path.join(self.data_dir, "current_time.json")
        self.ensure_directories()

    def ensure_directories(self):
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.daily_readings_dir, exist_ok=True)
        os.makedirs(self.monthly_readings_dir, exist_ok=True)

    def get_month_directory(self, base: str, date: datetime.datetime) -> str:
        month_dir = os.path.join(base, date.strftime("%Y%m"))
        os.makedirs(month_dir, exist_ok=True)
        return month_dir

# ==========================
# 账户管理：负责账户的加载、保存与注册
# ==========================

class AccountManager:
    def __init__(self, accounts_file: str):
        self.accounts_file = accounts_file

    def load_accounts(self) -> List[dict]:
        if os.path.exists(self.accounts_file):
            with open(self.accounts_file, "r", encoding="utf-8") as f:
                try:
                    accounts = json.load(f)
                    return accounts if isinstance(accounts, list) else []
                except json.JSONDecodeError:
                    return []
        return []

    def save_accounts(self, accounts: List[dict]):
        os.makedirs(os.path.dirname(self.accounts_file), exist_ok=True)
        with open(self.accounts_file, "w", encoding="utf-8") as f:
            json.dump(accounts, f, ensure_ascii=False, indent=2)

    def register_account(self, meter_id: str, area: str, dwelling: str, register_time: str) -> dict:
        accounts = self.load_accounts()
        if any(acc["meter_ID"] == meter_id for acc in accounts):
            raise ValueError("Meter ID already exists")
        account = {
            "meter_ID": meter_id,
            "area": area,
            "dwelling": dwelling,
            "register_time": register_time
        }
        accounts.append(account)
        self.save_accounts(accounts)
        return account

# ==========================
# 时间管理：处理当前模拟时间的获取与更新
# ==========================

class TimeManager:
    def __init__(self, current_time_file: str):
        self.current_time_file = current_time_file

    def get_current_time(self) -> datetime.datetime:
        if os.path.exists(self.current_time_file):
            with open(self.current_time_file, "r") as f:
                data = json.load(f)
                return datetime.datetime.fromisoformat(data["current_time"])
        else:
            initial_time = datetime.datetime(2024, 5, 1)
            self.save_current_time(initial_time)
            return initial_time

    def save_current_time(self, current_time: datetime.datetime):
        with open(self.current_time_file, "w") as f:
            json.dump({"current_time": current_time.isoformat()}, f)

# ==========================
# 数据采集器：生成电表读数，并维护最新读数和每日缓存
# ==========================

class ReadingGenerator:
    def __init__(self, time_manager: TimeManager, account_manager: AccountManager):
        self.time_manager = time_manager
        self.account_manager = account_manager
        self.latest_readings: Dict[str, float] = {}
        self.daily_cache: List[MeterReading] = []

    def _calculate_next_time(
        self, current_time: datetime.datetime, increment_unit: str, increment_value: int
    ) -> datetime.datetime:
        if increment_unit == 'minutes':
            return current_time + datetime.timedelta(minutes=increment_value)
        elif increment_unit == 'hours':
            return current_time + datetime.timedelta(hours=increment_value)
        elif increment_unit == 'days':
            return current_time + datetime.timedelta(days=increment_value)
        elif increment_unit == 'months':
            next_month = current_time.month + increment_value
            next_year = current_time.year + (next_month - 1) // 12
            next_month = ((next_month - 1) % 12) + 1
            last_day_of_next_month = calendar.monthrange(next_year, next_month)[1]
            next_day = min(current_time.day, last_day_of_next_month)
            return current_time.replace(year=next_year, month=next_month, day=next_day)
        else:
            raise ValueError("Invalid time unit")

    def generate_readings_for_day(
        self, day_start: datetime.datetime, day_end: datetime.datetime
    ) -> List[dict]:
        """
        在同一天内生成数据：
        - 如果起始时间在0点，则跳过0:00～1:00（维护时段）。
        - 每30分钟生成一个数据点，直到达到 day_end。
        """
        accounts = self.account_manager.load_accounts()
        daily_readings = []
        # 将起始时间归整：如果在维护时段，则从1点开始
        current = day_start.replace(minute=0, second=0, microsecond=0)
        if current.hour == 0:
            current = current.replace(hour=1)
        
        while current < day_end:
            next_time = current + datetime.timedelta(minutes=30)
            # 如果下一个时间点超过结束时间，则退出
            if next_time > day_end:
                break
            # 如果下一个时间点进入维护时段，则结束当天生成（维护时段不生成数据）
            if next_time.hour == 0:
                break

            for account in accounts:
                meter_id = account["meter_ID"]
                previous_value = self.latest_readings.get(meter_id, 0)
                increment = random.uniform(0, 1)
                meter_value = previous_value + increment
                self.latest_readings[meter_id] = meter_value

                reading = {
                    "meter_ID": meter_id,
                    "reading_time": next_time.isoformat(),
                    "meter_value": round(meter_value, 3)
                }
                daily_readings.append(reading)
                self.daily_cache.append(MeterReading(meter_id, next_time.isoformat(), round(meter_value, 3)))
            
            current = next_time

        return daily_readings

    def generate_readings(
        self, start_time: datetime.datetime, end_time: datetime.datetime
    ) -> List[dict]:
        """
        根据起始和结束时间，按天生成数据。如果跨天，则遍历每一天调用 generate_readings_for_day。
        """
        readings = []
        # 如果在同一天内，直接生成数据
        if start_time.date() == end_time.date():
            return self.generate_readings_for_day(start_time, end_time)
        
        # 处理起始天
        first_day_end = datetime.datetime.combine(start_time.date(), datetime.time(23, 59, 59))
        readings.extend(self.generate_readings_for_day(start_time, first_day_end))

        # 处理中间整天
        next_day = start_time.date() + datetime.timedelta(days=1)
        while next_day < end_time.date():
            day_start = datetime.datetime.combine(next_day, datetime.time(0, 0))
            day_end = datetime.datetime.combine(next_day, datetime.time(23, 59, 59))
            readings.extend(self.generate_readings_for_day(day_start, day_end))
            next_day += datetime.timedelta(days=1)

        # 处理结束天
        last_day_start = datetime.datetime.combine(end_time.date(), datetime.time(0, 0))
        readings.extend(self.generate_readings_for_day(last_day_start, end_time))
        return readings

    def collect(self, increment_unit: str, increment_value: int) -> dict:
        current_time = self.time_manager.get_current_time()
        next_time = self._calculate_next_time(current_time, increment_unit, increment_value)
        readings = self.generate_readings(current_time, next_time)
        self.time_manager.save_current_time(next_time)
        return {
            "message": f"Readings collected from {current_time} to {next_time}",
            "readings_count": len(readings),
            "sample_readings": readings[:3] if readings else [],
            "new_time": next_time.isoformat()
        }


# ==========================
# 日数据处理：整理每日缓存数据并保存为 JSON 文件
# ==========================

class DailyProcessor:
    def __init__(self, directory_manager: DirectoryManager):
        self.directory_manager = directory_manager

    def process(self, daily_cache: List[MeterReading], process_date: datetime.datetime):
        if not daily_cache:
            return

        daily_data = {}
        for reading in daily_cache:
            meter_id = reading.meter_id
            if meter_id not in daily_data:
                daily_data[meter_id] = {
                    "date": process_date.strftime("%Y-%m-%d"),
                    "readings": []
                }
            time_part = datetime.datetime.fromisoformat(reading.reading_time).strftime("%H:%M")
            daily_data[meter_id]["readings"].append({
                "time": time_part,
                "value": round(reading.meter_value, 3)
            })

        daily_file = self.get_daily_file_path(process_date)
        os.makedirs(os.path.dirname(daily_file), exist_ok=True)
        with open(daily_file, "w", encoding="utf-8") as f:
            json.dump(daily_data, f, ensure_ascii=False, indent=2)

    def get_daily_file_path(self, date: datetime.datetime) -> str:
        month_dir = self.directory_manager.get_month_directory(
            self.directory_manager.daily_readings_dir, date
        )
        return os.path.join(month_dir, f"readings_{date.strftime('%Y%m%d')}.json")
    
    def process_all(self, daily_cache: List[MeterReading]):
        """
        按照日期对 daily_cache 分组，每天分别归档数据
        """
        if not daily_cache:
            return
        
        readings_by_date = {}
        for reading in daily_cache:
            # 提取读数对应的日期字符串
            date_str = datetime.datetime.fromisoformat(reading.reading_time).strftime("%Y-%m-%d")
            if date_str not in readings_by_date:
                readings_by_date[date_str] = []
            readings_by_date[date_str].append(reading)
        
        # 对每个日期调用 process
        for date_str, readings in readings_by_date.items():
            # 使用该日期最后一个读数的时间作为归档日期
            process_date = datetime.datetime.fromisoformat(readings[-1].reading_time)
            self.process(readings, process_date)


# ==========================
# 月数据归档：归档月数据、生成月用电量及清理旧数据
# ==========================

class MonthlyProcessor:
    def __init__(self, directory_manager: DirectoryManager):
        self.directory_manager = directory_manager

    def archive(self, current_date: datetime.datetime):
        """
        归档月度数据并清理旧的日数据
        - 归档：上上个月的数据
        - 保留：当前月和上个月的日数据
        """
        # 当前月第一天
        first_of_current = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # 上个月最后一天、第一天
        last_month = first_of_current - datetime.timedelta(days=1)
        last_month_first = last_month.replace(day=1)
        # 归档目标月份：上上个月
        month_to_process = last_month_first - datetime.timedelta(days=1)
        month_to_process = month_to_process.replace(day=1)
        
        if month_to_process < datetime.datetime(2024, 5, 1):
            return

        # 得到目标月份对应的 daily_readings 目录
        process_month_dir = self.directory_manager.get_month_directory(
            self.directory_manager.daily_readings_dir, month_to_process
        )
        process_monthly_file = os.path.join(self.directory_manager.monthly_readings_dir, "month_readings.json")
        if os.path.exists(process_monthly_file):
            with open(process_monthly_file, "r", encoding="utf-8") as f:
                monthly_data = json.load(f)
        else:
            monthly_data = {}

        first_readings = {}
        last_readings = {}

        if os.path.exists(process_month_dir):
            # 遍历目标月份下的所有日数据文件
            for daily_file in sorted(os.listdir(process_month_dir)):
                if daily_file.endswith(".json"):
                    daily_path = os.path.join(process_month_dir, daily_file)
                    with open(daily_path, 'r', encoding='utf-8') as f:
                        daily_data = json.load(f)
                    for meter_id, meter_data in daily_data.items():
                        readings = sorted(meter_data["readings"], key=lambda x: x["time"])
                        if meter_id not in first_readings:
                            first_readings[meter_id] = readings[0]["value"]
                        last_readings[meter_id] = readings[-1]["value"]

        # 计算每个电表的本月用量
        for meter_id in first_readings.keys():
            if meter_id in last_readings:
                month_key = month_to_process.strftime("%Y-%m")
                month_total = last_readings[meter_id] - first_readings[meter_id]
                if meter_id not in monthly_data:
                    monthly_data[meter_id] = {}
                monthly_data[meter_id][month_key] = round(month_total, 3)

        os.makedirs(self.directory_manager.monthly_readings_dir, exist_ok=True)
        with open(process_monthly_file, "w", encoding="utf-8") as f:
            json.dump(monthly_data, f, ensure_ascii=False, indent=2)

        # 清理 2 个月前的 daily_readings 数据（即清理所有日期早于上个月第一天的目录）
        self._cleanup_old_readings(last_month_first)

    def _cleanup_old_readings(self, current_month_first: datetime.datetime):
        """
        清理日数据，只保留最近两个月的数据
        参数 current_month_first: 当前月份的第一天
        保留：当前月和上个月的数据
        删除：更早的数据
        """
        if os.path.exists(self.directory_manager.daily_readings_dir):
            # 计算上个月第一天（这是最早需要保留的日期）
            earliest_keep_date = current_month_first - datetime.timedelta(days=1)  # 上个月最后一天
            earliest_keep_date = earliest_keep_date.replace(day=1)  # 上个月第一天

            for year_month_dir in os.listdir(self.directory_manager.daily_readings_dir):
                try:
                    year = int(year_month_dir[:4])
                    month = int(year_month_dir[4:])
                    dir_date = datetime.datetime(year, month, 1)
                    # 删除早于上个月的数据
                    if dir_date < earliest_keep_date:
                        dir_path = os.path.join(self.directory_manager.daily_readings_dir, year_month_dir)
                        shutil.rmtree(dir_path)
                except ValueError:
                    continue
# ==========================
# 整个智能电表系统的门面类：将各个模块组合
# ==========================

class SmartMeterSystem:
    def __init__(self, base_dir: str):
        self.directory_manager = DirectoryManager(base_dir)
        self.account_manager = AccountManager(self.directory_manager.accounts_file)
        self.time_manager = TimeManager(self.directory_manager.current_time_file)
        self.reading_generator = ReadingGenerator(self.time_manager, self.account_manager)
        self.daily_processor = DailyProcessor(self.directory_manager)
        self.monthly_processor = MonthlyProcessor(self.directory_manager)

    def register_meter(self, meter_id: str, area: str, dwelling: str) -> dict:
        current_time = self.time_manager.get_current_time()
        formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        account = self.account_manager.register_account(meter_id, area, dwelling, formatted_time)
        # 初始化电表读数
        self.reading_generator.latest_readings[meter_id] = 0
        self.reading_generator.daily_cache.append(MeterReading(meter_id, formatted_time, 0))
        return account

    def collect_readings(self, increment_unit: str = 'days', increment_value: int = 1) -> dict:
        # 记录采集前的当前时间
        old_time = self.time_manager.get_current_time()
        result = self.reading_generator.collect(increment_unit, increment_value)
        # 按日期归档 daily_cache 中的数据
        self.daily_processor.process_all(self.reading_generator.daily_cache)
        # 清空缓存
        self.reading_generator.daily_cache.clear()
        new_time = datetime.datetime.fromisoformat(result["new_time"])
        # 如果采集前后月份发生了变化，则触发归档（归档 n-2 个月的数据）
        if old_time.month != new_time.month:
            self.monthly_processor.archive(new_time)
        return result

    def reset_system(self) -> bool:
        try:
            # 清空 daily_readings 和 monthly_readings 目录
            for directory in [self.directory_manager.daily_readings_dir, self.directory_manager.monthly_readings_dir]:
                if os.path.exists(directory):
                    shutil.rmtree(directory)
                os.makedirs(directory)
            # 重置账户文件
            with open(self.directory_manager.accounts_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            # 重新设定时间
            self.time_manager.save_current_time(datetime.datetime(2024, 5, 1))
            # 清空缓存
            self.reading_generator.latest_readings.clear()
            self.reading_generator.daily_cache.clear()
            return True
        except Exception as e:
            import traceback
            print("Reset failed:")
            traceback.print_exc()
            return False

# ==========================
# Flask 应用部分
# ==========================

app = Flask(__name__, 
            template_folder='templates',  # 指定模板目录
            static_folder='static'         # 指定静态文件目录
)
meter_system = SmartMeterSystem(os.path.dirname(os.path.abspath(__file__)))

@app.route("/")
def index():
    return render_template("index.html")

@app.route('/collect')
def collect():
    return render_template('collect.html')

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html")
    try:
        data = request.get_json()
        account = meter_system.register_meter(
            data["meterId"],
            data["area"],
            data["dwelling"]
        )
        return jsonify({"success": True, "account": account})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@app.route("/current_time", methods=["GET"])
def get_current_time():
    current_time = meter_system.time_manager.get_current_time()
    return jsonify({
        "Current Simulation Time": {
            "Date": current_time.strftime("%Y-%m-%d"),
            "Time": current_time.strftime("%H:%M:%S"),
            "Weekday": current_time.strftime("%A")
        }
    })

@app.route("/meter_reading", methods=["POST"])
def meter_reading():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        unit = data.get('unit', 'days')
        try:
            value = int(data.get('value', 1))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid value format"}), 400

        result = meter_system.collect_readings(unit, value)
        return jsonify(result), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        import traceback
        print("Error in meter_reading:", str(e))
        print(traceback.format_exc())
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route("/api/areas", methods=["GET"])
def get_areas():
    area_data_file = os.path.join(app.static_folder, 'js', 'area_data.json')
    try:
        with open(area_data_file, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    except FileNotFoundError:
        return jsonify({"error": "Area data file not found"}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid area data format"}), 500

@app.route('/reset')
def reset():
    if meter_system.reset_system():
        return """
        <script>
            alert('Reset Success!');
            window.location.href = '/';
        </script>
        """
    else:
        return """
        <script>
            alert('Reset Failed');
            window.location.href = '/';
        </script>
        """

if __name__ == "__main__":
    app.run()
