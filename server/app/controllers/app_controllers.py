from fastapi import HTTPException,WebSocket,BackgroundTasks,Response
from app.database.db_queries import fetch_max_timestamp,fetch_distinct_store_ids,fetch_store_data,fetch_timezone,fetch_menu_hours_data,report_status,report_files
from datetime import datetime, timedelta
import random
import string
import pandas as pd
import pytz
import asyncio
import threading
import time
import csv
from io import StringIO
import multiprocessing
from multiprocessing import Lock, Pool, Manager 


def calculate_uptime(store_status_df, start_time, end_time, menu_hours_df, timezone):

    filtered_df = store_status_df[(store_status_df['timestamp_utc'] >= start_time) &
                                  (store_status_df['timestamp_utc'] <= end_time)]

    day_of_week = start_time.weekday()
    menu_hours = menu_hours_df[menu_hours_df['day'] == day_of_week]
    
    start_times = []
    end_times = []
    for _, row in menu_hours.iterrows():
        start_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['start_time_local']).time())
        end_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['end_time_local']).time())
        start_time_utc = start_time_local.replace(tzinfo=pytz.timezone(timezone)).astimezone(pytz.utc)
        end_time_utc = end_time_local.replace(tzinfo=pytz.timezone(timezone)).astimezone(pytz.utc)
        start_times.append(start_time_utc)
        end_times.append(end_time_utc)

    total_menu_hours = sum([(end - start).total_seconds()/3600 for start, end in zip(start_times, end_times)])
    
    total_uptime = 0
    for _, row in filtered_df.iterrows():
        if row['status'] == 'active':
            timestamp_local = row['timestamp_utc'].astimezone(pytz.timezone(timezone)) 
            for start, end in zip(start_times, end_times):
                if start <= timestamp_local <= end:
                    total_uptime += (end - start).total_seconds()/3600
                    break
            
    uptime_ratio = total_uptime / total_menu_hours if total_menu_hours > 0 else 0
    extrapolated_uptime = uptime_ratio * 24 
    
    return extrapolated_uptime

def calculate_downtime(store_status_df, start_time, end_time, menu_hours_df, timezone):

    filtered_df = store_status_df[(store_status_df['timestamp_utc'] >= start_time) &  
                                  (store_status_df['timestamp_utc'] <= end_time)]

    day_of_week = start_time.weekday()
    menu_hours = menu_hours_df[menu_hours_df['day'] == day_of_week]
    
    start_times = []
    end_times = []
    for _, row in menu_hours.iterrows():
        start_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['start_time_local']).time())
        end_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['end_time_local']).time())
        start_time_utc = start_time_local.replace(tzinfo=pytz.timezone(timezone)).astimezone(pytz.utc)
        end_time_utc = end_time_local.replace(tzinfo=pytz.timezone(timezone)).astimezone(pytz.utc)
        start_times.append(start_time_utc)
        end_times.append(end_time_utc)

    total_menu_hours = sum([(end - start).total_seconds()/3600 for start, end in zip(start_times, end_times)])
    
    total_downtime = 0
    for _, row in filtered_df.iterrows():
        if row['status'] == 'inactive':
            timestamp_local = row['timestamp_utc'].astimezone(pytz.timezone(timezone))
            for start, end in zip(start_times, end_times):
                if start <= timestamp_local <= end:
                    total_downtime += (end - start).total_seconds()/3600
                    break
                    
    downtime_ratio = total_downtime / total_menu_hours if total_menu_hours > 0 else 0
    extrapolated_downtime = downtime_ratio * 24
    
    return extrapolated_downtime


def generate_report_for_store(store_id, current_timestamp, report_data, report_data_lock):
    try:
        menu_hours_data = fetch_menu_hours_data(store_id)
        for item in menu_hours_data:
            item.pop('_id', None)
        menu_hours_df = pd.DataFrame(menu_hours_data, columns=['store_id', 'day', 'start_time_local', 'end_time_local'])
        timezone = fetch_timezone(store_id)
        store_data = fetch_store_data(store_id)
        store_status_df = pd.DataFrame(store_data, columns=['store_id', 'status', 'timestamp_utc'])
        store_status_df['timestamp_utc'] = pd.to_datetime(store_status_df['timestamp_utc'], format='mixed')

        # Calculate uptime and downtime
        uptime_last_hour = calculate_uptime(store_status_df, current_timestamp - timedelta(hours=1),
                                            current_timestamp, menu_hours_df, timezone)
        uptime_last_day = calculate_uptime(store_status_df, current_timestamp - timedelta(days=1),
                                           current_timestamp, menu_hours_df, timezone)
        uptime_last_week = calculate_uptime(store_status_df, current_timestamp - timedelta(weeks=1),
                                            current_timestamp, menu_hours_df, timezone)
        downtime_last_hour = calculate_downtime(store_status_df, current_timestamp - timedelta(hours=1),
                                                current_timestamp, menu_hours_df, timezone)
        downtime_last_day = calculate_downtime(store_status_df, current_timestamp - timedelta(days=1),
                                               current_timestamp, menu_hours_df, timezone)
        downtime_last_week = calculate_downtime(store_status_df, current_timestamp - timedelta(weeks=1),
                                                current_timestamp, menu_hours_df, timezone)

        report = {
            'store_id': store_id,
            'uptime_last_hour': uptime_last_hour,
            'uptime_last_day': uptime_last_day,
            'uptime_last_week': uptime_last_week,
            'downtime_last_hour': downtime_last_hour,
            'downtime_last_day': downtime_last_day,
            'downtime_last_week': downtime_last_week
        }

        with report_data_lock:
            report_data.append(report)
    except Exception as e:
        print(e)
    return report_data[0]

def generate_report(report_id):
    print("start")
    start_time = time.time()
    max_timestamp = fetch_max_timestamp()
    if max_timestamp is None:
        raise HTTPException(
            status_code=500, detail={"error": "No max timestamp found in store_status collection."}
        )

    current_timestamp = pd.to_datetime(max_timestamp)
    store_ids = fetch_distinct_store_ids()
    print(f'Generating report for {len(store_ids)} stores...')

    report_data = []

    with Manager() as manager:
        report_data_lock = manager.Lock()

        with Pool() as pool:
            results = pool.starmap(
                generate_report_for_store, [(store_id, current_timestamp, report_data, report_data_lock) for store_id in store_ids]
            )

    report_file = f'report_{report_id}'
    end_time = time.time()  
    report_document = {
        'report_id': report_id,
        'filename': report_file,
        'data': results,
    }
    print("report_document",report_document)
    report_files.insert_one(report_document)
    report_status.update_one({'report_id': report_id}, {'$set': {'status': 'Completed'}})
    execution_time = end_time - start_time  
    print(f'Generated report in {execution_time} seconds.')

class BackgroundTasks(threading.Thread):
    def __init__(self, report_id,websocket):
        super().__init__()
        self.report_id = report_id
        self.websocket = websocket

    def run(self):
        asyncio.run(self.generate_report())

    async def generate_report(self):
        report_id = self.report_id
        websocket = self.websocket
        generate_report(report_id)

async def trigger_report_controller(websocket: WebSocket, background_tasks: BackgroundTasks):
    await websocket.accept()
    report_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    report_status.insert_one({'report_id': report_id, 'status': 'Running'})
    await websocket.send_json({'report_id': report_id})
    t = BackgroundTasks(report_id,websocket)
    t.start()
    
    return {'report_id': report_id}


async def get_report_controller(report_id):
    status_doc = report_status.find_one({'report_id': report_id})
    report_doc = report_files.find_one({'report_id': report_id})
    
    if status_doc is None:
        return {'status': "File not found"}
    elif status_doc['status'] == 'Running':
        return {'status': "Running"}
    else:
        report_file = f'report_{report_id}.csv'
        output = StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])
        csv_writer.writeheader()
        csv_writer.writerows(report_doc['data'])

        response = Response(content=output.getvalue())
        response.headers["Content-Disposition"] = f"attachment; filename={report_file}"
        response.headers["Content-Type"] = "text/csv"
        
        return {'status': "Completed",'filename': report_file,'response':response.body}

