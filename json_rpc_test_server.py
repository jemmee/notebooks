# pip install jsonrpcserver jsonrpcclient werkzeug requests
#
# python3 json_rpc_test_server.py

from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple
from jsonrpcserver import method, dispatch

# 1. Define and register remote functions using the @method decorator
@method
def ping():
    return "pong"

@method
def compute_sum(a, b):
    print(f"[SERVER] Executing remote procedure: compute_sum({a}, {b})")
    return a + b

# 2. Set up a standard raw HTTP application gateway receiver
@Request.application
def application(request):
    # JSON-RPC requires communication exclusively over HTTP POST requests
    if request.method == "POST":
        request_data = request.get_data(as_text=True)
        
        # Pass the raw text string to the dispatch engine to parse, 
        # execute the targeted function, and structure the proper JSON response.
        rpc_response = dispatch(request_data)
        
        return Response(str(rpc_response), mimetype="application/json")
    
    return Response("JSON-RPC server expects HTTP POST transactions.", status=405)

if __name__ == "__main__":
    print("[SERVER] JSON-RPC Engine listening on http://localhost:5000 ...")
    run_simple("localhost:5000", application)