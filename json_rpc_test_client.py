# python3 json_rpc_test_client.py

import requests
from jsonrpcclient import request, parse

def execute_remote_calls():
    url = "http://localhost:5000"

    print("[CLIENT] Transaction 1: Simple execution ping...")
    # Generate the standardized JSON-RPC 2.0 dictionary format string automatically
    ping_payload = request("ping")
    
    # Fire the payload over raw HTTP POST
    response = requests.post(url, json=ping_payload)
    
    # Parse the standardized result block out of the JSON envelope
    parsed_response = parse(response.json())
    print(f"         Server Reply: {parsed_response.result}\n")


    print("[CLIENT] Transaction 2: Passing parameters across the wire...")
    # Generate payload containing positional parameters
    sum_payload = request("compute_sum", params={"a": 85, "b": 15})
    
    response = requests.post(url, json=sum_payload)
    parsed_sum = parse(response.json())
    
    print(f"         Server Reply (Computed Sum): {parsed_sum.result}")

if __name__ == "__main__":
    execute_remote_calls()