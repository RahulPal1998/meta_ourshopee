import os
from flask import Flask

app = Flask(_name_)

@app.route('/', methods=['GET', 'POST'])
def hello():
    return 'ETL Service is running!', 200

@app.route('/run-etl', methods=['POST'])
def run_etl():
    try:
        # Your ETL code here
        return {'status': 'success'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

if _name_ == "_main_":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
