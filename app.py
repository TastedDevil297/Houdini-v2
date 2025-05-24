import os
import pandas as pd
from datetime import datetime, date, time as dt_time
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from BODSDataExtractor.extractor import TimetableExtractor, VehicleMonitoringExtractor

# Data holders
static_stops = pd.DataFrame()
live_positions = pd.DataFrame()

def classify_delay(sec):
    if sec < -60:
        return 'Early'
    elif sec <= 299:
        return 'On time'
    else:
        return 'Late'

# Load timetable once
extractor = TimetableExtractor(api_key=os.getenv('BODS_API_KEY'), status='published', search='First Cymru', service_line_level=True, stop_level=True)
static_stops = extractor.stop_level_extract

# App factory
app = Flask(__name__)
scheduler = BackgroundScheduler()

# Job: fetch live every 30s
def fetch_live():
    global live_positions
    vm = VehicleMonitoringExtractor(api_key=os.getenv('BODS_API_KEY'), subscription='First Cymru', max_records=1000)
    live_positions = vm.vehicle_monitoring_extract

scheduler.add_job(fetch_live, 'interval', seconds=30)
scheduler.start()

@app.route('/api/vehicle_positions')
def vehicle_positions():
    return jsonify(live_positions.to_dict(orient='records'))

@app.route('/api/arrival_status')
def arrival_status():
    trip = request.args.get('trip_id')
    stop = request.args.get('stop_id')
    if not trip or not stop:
        return {'error': 'trip_id & stop_id needed'}, 400
    sched = static_stops[(static_stops['trip_id']==trip) & (static_stops['stop_id']==stop)]
    if sched.empty:
        return {'error': 'no schedule'}, 404
    dep = sched.iloc[0]['departure_time']
    h,m,s = map(int, dep.split(':'))
    scheduled = datetime.combine(date.today(), dt_time(h,m,s))
    obs = live_positions[live_positions['trip_id']==trip]
    if obs.empty:
        return {'error': 'no live data'}, 404
    observed = pd.to_datetime(obs.iloc[-1]['recorded_at_time'])
    delay = (observed - scheduled).total_seconds()
    return {'trip_id': trip, 'stop_id': stop, 'delay_sec': delay, 'status': classify_delay(delay)}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
