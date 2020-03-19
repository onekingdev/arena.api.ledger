from flask import Flask, request, jsonify
import json
from amazon.ion.simpleion import dumps, loads
from qldb_sesssion import session
from middlewate import Middleware

app = Flask(__name__)
app.wsgi_app = Middleware(app.wsgi_app)


@app.route('/tables')
def tables():
    tables = []

    try:
        qldb_session = session()
        for table in qldb_session.list_tables():
            tables.append(table)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    return json.dumps(tables)


@app.route('/table/<name>/documents', methods=["GET"])
def table_documents(name):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    query = "SELECT * FROM {}"
    query = query.format(name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(query)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    result = []

    for row in cursor:
        result.append(dumps(row, binary=False, indent='  ', omit_version_marker=True))

    return jsonify(documents=result)


@app.route('/table/<name>/document', methods=["POST"])
def insert_data(name):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    data = request.get_json()

    if data is None:
        return jsonify(error=400, exception='Insert data is required')

    statement = 'INSERT INTO {} ?'.format(name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(statement, loads(dumps(data)))
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    ret_val = list(map(lambda x: x.get('documentId'), cursor))


    if len(ret_val) == 0:
        return jsonify(error=404, exception='Document id not found')

    return jsonify(id=ret_val[0])


@app.route('/table/<name>/document/<id>', methods=["GET"])
def get_document(name, id):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    if len(id) == 0:
        return jsonify(error=400, exception='Document id is required')

    query = "SELECT rid, r.* FROM {} AS r BY rid WHERE rid = ?"
    query = query.format(name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(query, id)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))


    result = []

    for row in cursor:
        result.append(dumps(row, binary=False, omit_version_marker=True))

    return json.dumps(result)


@app.route('/table/<name>/document/<id>', methods=["PUT"])
def update_document(name, id):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    if len(id) == 0:
        return jsonify(error=400, exception='Document id is required')

    data = request.get_json()

    if data is None:
        return jsonify(error=400, exception='Update data is required')

    query = "UPDATE {} AS p BY rid SET p = {} WHERE rid = ?".format(name, data)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(query, id)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    result = []

    for row in cursor:
        result.append(dumps(row, binary=False, omit_version_marker=True))

    return json.dumps(result)


@app.route('/table/<name>', methods=["POST"])
def create_table(name):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    indexes = request.get_json()

    try:
        qldb_session = session()
        statement = 'CREATE TABLE {}'.format(name)
        qldb_session.execute_statement(statement)

        if indexes is not None:
            for index in indexes:
                statement = 'CREATE INDEX on {} ({})'.format(name, index)
                qldb_session.execute_statement(statement)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    return ''


@app.route('/table/<name>', methods=["DELETE"])
def delete_table(name):
    if len(name) == 0:
        return jsonify(error=400, exception='Table name is required')

    statement = 'DROP TABLE {}'.format(name)

    try:
        qldb_session = session()
        qldb_session.execute_statement(statement)
    except Exception as e:
        return jsonify(error=400, exception='QLDB error: ' + str(e))

    return ''


if __name__ == '__main__':
    app.run(port=5001)
