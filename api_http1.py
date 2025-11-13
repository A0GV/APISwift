from flask import Flask, jsonify, make_response, request, send_file
import json
import sys
import mysqlfunc as MSSql

# Connect to mssql dB from start
mssql_params = {}
mssql_params['DB_HOST'] = '100.80.80.7'
mssql_params['DB_NAME'] = 'nova'
mssql_params['DB_USER'] = 'SA'
mssql_params['DB_PASSWORD'] = 'Shakira123.'

try:
    MSSql.cnx = MSSql.mssql_connect(mssql_params)
except Exception as e:
    print("Cannot connect to mssql server!: {}".format(e))
    sys.exit()

app = Flask(__name__)

@app.route("/hello")
def hello():
    return "Shakira rocks!\n"

@app.route("/user")
def user():
    username = request.args.get('username', None)
    #print(username)
    d_user = MSSql.read_user_data('users', username)
    return make_response(jsonify(d_user))

@app.route("/crud/create", methods=['POST'])
def crud_create():
    d = request.json
    idUser = MSSql.sql_insert_row_into('users', d)
    return make_response(jsonify(idUser))

@app.route("/crud/read", methods=['GET'])
def crud_read():
    username = request.args.get('username', None)
    d_user = MSSql.sql_read_where('users', {'username': username})
    return make_response(jsonify(d_user))

@app.route("/crud/update", methods=['PUT'])
def crud_update():
    d = request.json
    d_field = {'password': d['password']}
    d_where = {'username': d['username']}
    MSSql.sql_update_where('users', d_field, d_where)
    return make_response(jsonify('ok'))
#apdjaljfnas
@app.route("/crud/delete", methods=['DELETE'])
def crud_delete():
    d = request.json
    d_where = {'username': d['username']}
    MSSql.sql_delete_where('users', d_where)
    return make_response(jsonify('ok'))

if __name__ == '__main__':
    print ("Running API...")
    app.run(host='0.0.0.0', port=10204, debug=True)
