
from flask import Flask, request
import os

app = Flask(__name__)

@app.route('/dns-lookup')
def dns_lookup():
    domain_name = request.args.get('domain')

    # VULNERABLE: User input is passed to a system command
    command = "nslookup " + domain_name
    result = os.popen(command).read()

    return f"<pre>{result}</pre>"

if __name__ == '__main__':
    app.run()