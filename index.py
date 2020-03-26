from flask import Flask, request, jsonify, abort, Response
import json
from amazon.ion.simpleion import dumps, loads, IonType
from qldb_sesssion import session
from middlewate import Middleware
import re

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
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    return json.dumps(tables)


@app.route('/table/<name>/documents', methods=["GET"])
def table_documents(name):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

    query = "SELECT * FROM {}"
    query = query.format(name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(query)
    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    result = []

    for row in cursor:
        result.append(parse_ion(loads(dumps(row, binary=False, omit_version_marker=True))))

    return jsonify(result)


@app.route('/table/<name>/document', methods=["POST"])
def insert_data(name):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

    data = request.get_json()

    if data is None:
        return Response(u'Insert data is required', mimetype='text/plain', status=400)

    statement = 'INSERT INTO {} ?'.format(name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(statement, loads(dumps(data)))
        ret_val = list(map(lambda x: x.get('documentId'), cursor))

        if len(ret_val) == 0:
            return jsonify(error=404, exception='Document id not found')

        id = ret_val[0]
        query = "SELECT id, r.blockAddress, r.hash, r.metadata FROM _ql_committed_{} AS r BY id WHERE id = ?"

        query = query.format(name)
        last_element = qldb_session.execute_statement(query, id)

        result = []

        for row in last_element:
            result.append(parse_ion(loads(dumps(row, binary=False, omit_version_marker=True))))
        print(result)
        if len(result) == 1:
            result = result[0]

    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    return jsonify(result)


@app.route('/table/<name>/document/<id>', methods=["GET"])
def get_document(name, id):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

    if len(id) == 0:
        return Response(u'Document id is required', mimetype='text/plain', status=400)

    query = "SELECT rid, r.*, m.blockAddress, m.hash, m.metadata FROM {} AS r BY rid " \
            "INNER JOIN _ql_committed_{} AS m BY mid ON rid = mid " \
            "WHERE rid = ?"
    query = query.format(name, name)

    try:
        qldb_session = session()
        cursor = qldb_session.execute_statement(query, id)
    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    result = []

    for row in cursor:
        result.append(parse_ion(loads(dumps(row, binary=False, omit_version_marker=True))))

    if len(result) == 1:
        result = result[0]

    history_query = "SELECT * FROM history( {} ) AS h WHERE h.metadata.id = ?"
    history_query = history_query.format(name)

    try:
        history_cursor = qldb_session.execute_statement(history_query, id)
    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    history = []

    for row in history_cursor:
        history.append(parse_ion(loads(dumps(row, binary=False, omit_version_marker=True))))

    response = dict()
    response['document'] = result
    response['history'] = history

    return json.dumps(response)


@app.route('/table/<name>/document/<id>', methods=["PUT"])
def update_document(name, id):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

    if len(id) == 0:
        return Response(u'Document id is required', mimetype='text/plain', status=400)

    data = request.get_json()

    if data is None:
        return Response(u'Update data is required', mimetype='text/plain', status=400)

    select_query = "SELECT r.* FROM {} AS r BY rid WHERE rid = ?".format(name)

    try:
        qldb_session = session()
        selected = qldb_session.execute_statement(select_query, id)

        result = []

        for row in selected:
            parsed_selected = loads(dumps(row, binary=False, omit_version_marker=True))
            result.append(parsed_selected)

        if len(result) == 1:
            result = result[0]

            processed_result = dict()

            for res in result:
                processed_result[res] = str(result[res])

            for field in data:
                processed_result[field] = data[field]

            data = processed_result

        data = loads(dumps(data))

        query = "UPDATE {} AS p BY rid SET p = ? WHERE rid = ?".format(name)
        cursor = qldb_session.execute_statement(query, data, id)
    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    result = []

    for row in cursor:
        result.append(parse_ion(loads(dumps(row, binary=False, omit_version_marker=True))))

    return json.dumps(result)


@app.route('/table/<name>', methods=["POST"])
def create_table(name):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

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
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    return ''


@app.route('/table/<name>', methods=["DELETE"])
def delete_table(name):
    if len(name) == 0:
        return Response(u'Table name is required', mimetype='text/plain', status=400)

    statement = 'DROP TABLE {}'.format(name)

    try:
        qldb_session = session()
        qldb_session.execute_statement(statement)
    except Exception as e:
        return Response(u'QLDB error: ' + str(e), mimetype='text/plain', status=400)

    return ''


def parse_ion(ion_object):
    parsed = dict()

    for field in ion_object:
        if ion_object[field].__dict__['ion_type'] == IonType.STRUCT:
            parsed[field] = parse_ion(ion_object[field])
        elif ion_object[field].__dict__['ion_type'] == IonType.BLOB:
            hash_str = re.search('{{(.*)}}', dumps(ion_object[field], binary=False))

            if hash_str:
                parsed[field] = hash_str.group(1)
        else:
            parsed[field] = str(ion_object[field])

    return parsed


if __name__ == '__main__':
    app.run(port=5001)
