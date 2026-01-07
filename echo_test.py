# python3 -m pip install flask
#
# python3 -m flask --app echo_test.py run --port 5001
#
# curl -X POST http://localhost:5001/hello-world -d "Secret Message"

from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def echo(path):
    # This captures everything sent to the server
    return jsonify({
        "message": "I received your request!",
        "method": request.method,
        "path_requested": f"/{path}",
        "headers": dict(request.headers),
        "query_params": request.args,
        "body": request.get_data(as_text=True)
    })

if __name__ == '__main__':
    # app.run(port=5001)
    app.run(host='0.0.0.0', port=5001, debug=True)