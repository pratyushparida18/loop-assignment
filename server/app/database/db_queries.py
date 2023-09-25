from app.database.db_connection import get_database

database = get_database()
store_status = database['store_status']
menu_hours = database['menu_hours']
bq_results = database['bq_results']
report_status = database['status']
report_files = database['files']

def fetch_max_timestamp():
    max_timestamp = store_status.find_one({}, sort=[('timestamp_utc', -1)])
    return max_timestamp['timestamp_utc'] if max_timestamp else None

def fetch_distinct_store_ids():
    distinct_store_ids = store_status.distinct("store_id")
    return distinct_store_ids

def fetch_store_data(store_id):
    store_data = list(store_status.find({"store_id": store_id}))
    return store_data

def fetch_timezone(store_id):
    result = bq_results.find_one({"store_id": store_id})
    return result['timezone_str'] if result else 'America/Chicago'

def fetch_menu_hours_data(store_id):
    menu_hours_data = list(menu_hours.find({"store_id": store_id}))
    return menu_hours_data