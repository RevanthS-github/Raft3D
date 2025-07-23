import os
import json
import threading
import time
import uuid
from flask import Flask, request, jsonify
import requests
import sys

app = Flask(__name__)

# ---- Node Configuration ----
NODE_ID = sys.argv[1] if len(sys.argv) > 1 else "node1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5001
PEER_PORTS = [5001, 5002, 5003]
if PORT in PEER_PORTS:
    PEER_PORTS.remove(PORT)

LEADER_PORT = None
IS_LEADER = False
leader_lock = threading.Lock()
data_lock = threading.Lock()

# ---- Data Structures ----
PRINTERS_FILE = f"printers_{NODE_ID}.json"
FILAMENTS_FILE = f"filaments_{NODE_ID}.json"
PRINT_JOBS_FILE = f"print_jobs_{NODE_ID}.json"

# Load data or initialize empty if files don't exist
if os.path.exists(PRINTERS_FILE):
    with open(PRINTERS_FILE, 'r') as f:
        printers = json.load(f)
else:
    printers = []

if os.path.exists(FILAMENTS_FILE):
    with open(FILAMENTS_FILE, 'r') as f:
        filaments = json.load(f)
else:
    filaments = []

if os.path.exists(PRINT_JOBS_FILE):
    with open(PRINT_JOBS_FILE, 'r') as f:
        print_jobs = json.load(f)
else:
    print_jobs = []

# ---- Helper Functions ----
def save_data():
    """Save all data to disk"""
    with open(PRINTERS_FILE, 'w') as f:
        json.dump(printers, f)
    with open(FILAMENTS_FILE, 'w') as f:
        json.dump(filaments, f)
    with open(PRINT_JOBS_FILE, 'w') as f:
        json.dump(print_jobs, f)

def discover_leader():
    """Leader election and heartbeat mechanism"""
    global LEADER_PORT, IS_LEADER
    while True:
        found = False
        for port in [PORT] + PEER_PORTS:
            try:
                res = requests.get(f"http://localhost:{port}/who_is_leader", timeout=1)
                data = res.json()
                if data['is_leader']:
                    with leader_lock:
                        LEADER_PORT = data['port']
                        IS_LEADER = (LEADER_PORT == PORT)
                    found = True
                    print(f"Node {NODE_ID}: Leader found at port {LEADER_PORT}")
                    break
            except:
                continue

        if not found:
            # This node becomes leader if no leader found
            with leader_lock:
                LEADER_PORT = PORT
                IS_LEADER = True
                print(f"Node {NODE_ID}: Becoming leader (port {PORT})")
        
        time.sleep(5)  # Check leadership every 5 seconds

def forward_to_leader(endpoint, method, data=None):
    """Forward requests to the leader node"""
    try:
        if method == "GET":
            res = requests.get(f"http://localhost:{LEADER_PORT}{endpoint}")
        elif method == "POST":
            res = requests.post(f"http://localhost:{LEADER_PORT}{endpoint}", json=data)
        return res.json(), res.status_code
    except Exception as e:
        return {"error": "Leader not reachable", "details": str(e)}, 500

def replicate_to_followers(data_type, operation, data):
    """Replicate data changes to follower nodes"""
    for peer_port in PEER_PORTS:
        try:
            requests.post(
                f"http://localhost:{peer_port}/replicate", 
                json={"data_type": data_type, "operation": operation, "data": data}
            )
        except:
            print(f"Failed to replicate to follower at port {peer_port}")

def validate_filament_type(filament_type):
    """Validate that filament type is one of the allowed values"""
    return filament_type in ["PLA", "PETG", "ABS", "TPU"]

def get_filament_by_id(filament_id):
    """Get a filament by its ID"""
    for filament in filaments:
        if filament["id"] == filament_id:
            return filament
    return None

def get_printer_by_id(printer_id):
    """Get a printer by its ID"""
    for printer in printers:
        if printer["id"] == printer_id:
            return printer
    return None

def get_print_job_by_id(job_id):
    """Get a print job by its ID"""
    for job in print_jobs:
        if job["id"] == job_id:
            return job
    return None

def calculate_filament_usage(filament_id):
    """Calculate how much filament is reserved by queued and running jobs"""
    usage = 0
    for job in print_jobs:
        if job["filament_id"] == filament_id and job["status"] in ["Queued", "Running"]:
            usage += job["print_weight_in_grams"]
    return usage

# ---- Flask Endpoints ----
@app.route('/who_is_leader', methods=['GET'])
def who_is_leader():
    """Endpoint to check if this node is the leader"""
    return jsonify({"node_id": NODE_ID, "is_leader": IS_LEADER, "port": PORT})

# ---- Printer Endpoints ----
@app.route('/api/v1/printers', methods=['POST'])
def create_printer():
    """Create a new printer"""
    data = request.get_json()
    if not data or 'company' not in data or 'model' not in data:
        return jsonify({"error": "Missing required printer data"}), 400

    with leader_lock:
        if not IS_LEADER:
            return forward_to_leader('/api/v1/printers', 'POST', data)

    with data_lock:
        # Generate a unique ID for the printer
        printer_id = str(uuid.uuid4())
        printer = {
            "id": printer_id,
            "company": data['company'],
            "model": data['model']
        }
        printers.append(printer)
        save_data()

    # Replicate to followers
    replicate_to_followers("printers", "create", printer)

    return jsonify({"message": "Printer created", "printer": printer}), 201

@app.route('/api/v1/printers', methods=['GET'])
def get_printers():
    """Retrieve all printers"""
    return jsonify({"printers": printers})

# ---- Filament Endpoints ----
@app.route('/api/v1/filaments', methods=['POST'])
def create_filament():
    """Create a new filament"""
    data = request.get_json()
    if not data or 'type' not in data or 'color' not in data or \
       'total_weight_in_grams' not in data or 'remaining_weight_in_grams' not in data:
        return jsonify({"error": "Missing required filament data"}), 400

    # Validate filament type
    if not validate_filament_type(data['type']):
        return jsonify({"error": "Invalid filament type. Must be one of: PLA, PETG, ABS, TPU"}), 400

    try:
        total_weight = int(data['total_weight_in_grams'])
        remaining_weight = int(data['remaining_weight_in_grams'])
        if total_weight <= 0 or remaining_weight <= 0 or remaining_weight > total_weight:
            return jsonify({"error": "Invalid weight values"}), 400
    except ValueError:
        return jsonify({"error": "Weight values must be integers"}), 400

    with leader_lock:
        if not IS_LEADER:
            return forward_to_leader('/api/v1/filaments', 'POST', data)

    with data_lock:
        # Generate a unique ID for the filament
        filament_id = str(uuid.uuid4())
        filament = {
            "id": filament_id,
            "type": data['type'],
            "color": data['color'],
            "total_weight_in_grams": total_weight,
            "remaining_weight_in_grams": remaining_weight
        }
        filaments.append(filament)
        save_data()

    # Replicate to followers
    replicate_to_followers("filaments", "create", filament)

    return jsonify({"message": "Filament created", "filament": filament}), 201

@app.route('/api/v1/filaments', methods=['GET'])
def get_filaments():
    """Retrieve all filaments"""
    return jsonify({"filaments": filaments})

# ---- Print Job Endpoints ----
@app.route('/api/v1/print_jobs', methods=['POST'])
def create_print_job():
    """Create a new print job"""
    data = request.get_json()
    if not data or 'printer_id' not in data or 'filament_id' not in data or \
       'filepath' not in data or 'print_weight_in_grams' not in data:
        return jsonify({"error": "Missing required print job data"}), 400

    # Validate printer and filament exist
    printer = get_printer_by_id(data['printer_id'])
    filament = get_filament_by_id(data['filament_id'])
    
    if not printer:
        return jsonify({"error": f"Printer with ID {data['printer_id']} not found"}), 404
    if not filament:
        return jsonify({"error": f"Filament with ID {data['filament_id']} not found"}), 404

    try:
        print_weight = int(data['print_weight_in_grams'])
        if print_weight <= 0:
            return jsonify({"error": "Print weight must be positive"}), 400
    except ValueError:
        return jsonify({"error": "Print weight must be an integer"}), 400

    # Check if there's enough filament available
    current_usage = calculate_filament_usage(data['filament_id'])
    if print_weight > (filament["remaining_weight_in_grams"] - current_usage):
        return jsonify({
            "error": "Not enough filament remaining",
            "remaining": filament["remaining_weight_in_grams"],
            "reserved": current_usage,
            "available": filament["remaining_weight_in_grams"] - current_usage,
            "requested": print_weight
        }), 400

    with leader_lock:
        if not IS_LEADER:
            return forward_to_leader('/api/v1/print_jobs', 'POST', data)

    with data_lock:
        # Generate a unique ID for the print job
        job_id = str(uuid.uuid4())
        print_job = {
            "id": job_id,
            "printer_id": data['printer_id'],
            "filament_id": data['filament_id'],
            "filepath": data['filepath'],
            "print_weight_in_grams": print_weight,
            "status": "Queued"  # Default status on creation
        }
        print_jobs.append(print_job)
        save_data()

    # Replicate to followers
    replicate_to_followers("print_jobs", "create", print_job)

    return jsonify({"message": "Print job created", "print_job": print_job}), 201

@app.route('/api/v1/print_jobs', methods=['GET'])
def get_print_jobs():
    """Retrieve all print jobs with optional status filter"""
    status_filter = request.args.get('status')
    
    if status_filter:
        filtered_jobs = [job for job in print_jobs if job["status"].lower() == status_filter.lower()]
        return jsonify({"print_jobs": filtered_jobs})
    else:
        return jsonify({"print_jobs": print_jobs})

@app.route('/api/v1/print_jobs/<job_id>/status', methods=['POST'])
def update_print_job_status(job_id):
    """Update the status of a print job"""
    data = request.get_json()
    if not data or 'status' not in data:
        return jsonify({"error": "Missing status data"}), 400

    new_status = data['status'].capitalize()
    
    # Validate new status value
    if new_status not in ["Running", "Done", "Canceled"]:
        return jsonify({"error": "Invalid status. Must be one of: Running, Done, Canceled"}), 400

    with leader_lock:
        if not IS_LEADER:
            return forward_to_leader(f'/api/v1/print_jobs/{job_id}/status', 'POST', data)

    with data_lock:
        job = get_print_job_by_id(job_id)
        if not job:
            return jsonify({"error": f"Print job with ID {job_id} not found"}), 404

        current_status = job["status"]
        
        # Validate status transition
        if new_status == "Running" and current_status != "Queued":
            return jsonify({"error": "Job can only go to Running state from Queued state"}), 400
        elif new_status == "Done" and current_status != "Running":
            return jsonify({"error": "Job can only go to Done state from Running state"}), 400
        elif new_status == "Canceled" and current_status not in ["Queued", "Running"]:
            return jsonify({"error": "Job can only go to Canceled state from Queued or Running state"}), 400

        # Update job status
        job["status"] = new_status
        
        # If job is marked as Done, update filament remaining weight
        if new_status == "Done":
            filament = get_filament_by_id(job["filament_id"])
            if filament:
                filament["remaining_weight_in_grams"] -= job["print_weight_in_grams"]
                # Make sure we don't go negative
                if filament["remaining_weight_in_grams"] < 0:
                    filament["remaining_weight_in_grams"] = 0
                    
        save_data()

    # Replicate to followers
    if new_status == "Done":
        # For Done status, we need to update both the job and filament
        filament = get_filament_by_id(job["filament_id"])
        replicate_to_followers("print_jobs", "update_status", {"job": job, "filament": filament})
    else:
        replicate_to_followers("print_jobs", "update_status", {"job": job})

    return jsonify({"message": f"Print job status updated to {new_status}", "print_job": job}), 200

# ---- Replication Endpoint ----
@app.route('/replicate', methods=['POST'])
def replicate():
    """Receive and apply replicated data from the leader"""
    replication_data = request.get_json()
    if not replication_data or 'data_type' not in replication_data or 'operation' not in replication_data or 'data' not in replication_data:
        return jsonify({"error": "Invalid replication data"}), 400

    data_type = replication_data['data_type']
    operation = replication_data['operation']
    data = replication_data['data']

    with data_lock:
        if data_type == "printers":
            if operation == "create":
                printers.append(data)
            
        elif data_type == "filaments":
            if operation == "create":
                filaments.append(data)
            
        elif data_type == "print_jobs":
            if operation == "create":
                print_jobs.append(data)
            
            elif operation == "update_status":
                job = data.get("job")
                filament = data.get("filament")
                
                # Update the job
                for i, existing_job in enumerate(print_jobs):
                    if existing_job["id"] == job["id"]:
                        print_jobs[i] = job
                        break
                
                # If Done status, update the filament too
                if filament:
                    for i, existing_filament in enumerate(filaments):
                        if existing_filament["id"] == filament["id"]:
                            filaments[i] = filament
                            break
        
        save_data()

    return jsonify({"message": "Replication successful"}), 200

# ---- Start Server ----
if __name__ == '__main__':
    print(f"Starting Raft3D node {NODE_ID} on port {PORT}")
    # Start leader discovery thread
    threading.Thread(target=discover_leader, daemon=True).start()
    # Start Flask server
    app.run(host='0.0.0.0', port=PORT, debug=False)