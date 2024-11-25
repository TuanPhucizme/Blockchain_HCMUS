import threading
import time
import random
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# Cấu hình cơ bản
node_id = None  # ID nút
state = "Follower"  # Trạng thái ban đầu
current_term = 0  # Nhiệm kỳ hiện tại
voted_for = None  # Nút đã bầu trong nhiệm kỳ hiện tại
log = []  # Nhật ký
commit_index = -1  # Chỉ số commit
leader_id = None  # ID leader hiện tại
nodes = []  # Danh sách nút trong cluster
election_timeout = random.uniform(1.0, 2.0)  # Timeout bầu cử (giây)
heartbeat_interval = 0.5  # Khoảng thời gian gửi heartbeat (giây)
timer = None  # Bộ đếm timeout

# Khởi tạo Flask server
@app.route("/append_entries", methods=["POST"])
def append_entries():
    global state, leader_id, current_term, timer
    data = request.json
    leader_term = data.get("term")
    if leader_term >= current_term:
        state = "Follower"
        leader_id = data.get("leader_id")
        current_term = leader_term
        reset_timer()
        return jsonify({"success": True})
    return jsonify({"success": False})

@app.route("/request_vote", methods=["POST"])
def request_vote():
    global voted_for, current_term
    data = request.json
    candidate_term = data.get("term")
    candidate_id = data.get("candidate_id")
    if candidate_term > current_term and (voted_for is None or voted_for == candidate_id):
        current_term = candidate_term
        voted_for = candidate_id
        return jsonify({"vote_granted": True})
    return jsonify({"vote_granted": False})

def reset_timer():
    global timer
    if timer:
        timer.cancel()
    timer = threading.Timer(election_timeout, start_election)
    timer.start()

def start_election():
    global state, current_term, voted_for
    state = "Candidate"
    current_term += 1
    voted_for = node_id
    votes = 1  # Bắt đầu với phiếu của chính mình
    for node in nodes:
        if node != node_id:
            try:
                response = requests.post(f"http://{node}/request_vote", json={
                    "term": current_term,
                    "candidate_id": node_id
                })
                if response.json().get("vote_granted"):
                    votes += 1
            except:
                pass
    if votes > len(nodes) // 2:
        become_leader()

def become_leader():
    global state
    state = "Leader"
    print(f"Node {node_id} trở thành Leader!")
    threading.Thread(target=send_heartbeat).start()

def send_heartbeat():
    global state
    while state == "Leader":
        for node in nodes:
            if node != node_id:
                try:
                    requests.post(f"http://{node}/append_entries", json={
                        "term": current_term,
                        "leader_id": node_id
                    })
                except:
                    pass
        time.sleep(heartbeat_interval)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Sử dụng: python raft_node.py <node_id> <node_list>")
        sys.exit(1)
    node_id = sys.argv[1]
    nodes = sys.argv[2].split(",")
    threading.Thread(target=reset_timer).start()
    app.run(host="0.0.0.0", port=int(node_id.split(":")[1]))
