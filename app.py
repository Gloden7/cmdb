from aiohttp import web
from aiohttp.web import json_response
from cmdb import add_schema, add_field, add_entity
from cmdb import update_schema, update_field, update_entity, iter_schema, unique_fields
from cmdb import delete_entity, delete_field, delete_schema
from cmdb import list_schema, list_field, list_record, list_value, list_relation_record
from cmdb import iter_record, iter_value, iter_relation_record
from cmdb.tools import RET, getmsg, get_logger, types
from cmdb.exceptions import CMDBError
import settings
import json
import xlrd
import xlwt


logger = get_logger("app")


def jsonify(**kwargs):
    return json_response(data=kwargs)


async def table(request: web.Request):
    try:
        page = int(request.query.get('page', 1))
    except ValueError:
        page = 1
    try:
        size = int(request.query.get('size', 20))
    except ValueError:
        size = 20
    try:
        query = json.loads(request.query.get('query'))
    except TypeError:
        query = {}

    try:
        schemas, pagination = list_schema(page, size, query)

        schemas = [{
            "id": schema.id,
            "name": schema.name,
            "desc": schema.desc,
            "createtime": schema.createtime.timestamp(),
            "updatetime": schema.updatetime.timestamp(),
        } for schema in schemas]

        data = dict(data=schemas, pagination=pagination)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=data)


async def column(request: web.Request):
    id_ = request.query.get("schema_id")
    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))

    try:
        fields = list_field(id_)

        fields = [{
            "id": field.id,
            "name": field.name,
            "desc": field.desc,
            "createtime": field.createtime.timestamp(),
            "updatetime": field.updatetime.timestamp(),
            "meta": json.loads(field.meta),
            "ref": field.ref,
            "schema_id": field.schema_id
        } for field in fields]

    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=fields)


async def all_column(request: web.Request):
    id_ = request.query.get("schema_id")
    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))

    try:
        fields = list_field(id_)
        fields = [{
            "key": field.id,
            "title": field.name,
            "ref": field.ref,
            "meta": json.loads(field.meta),
            "description": field.desc,
        } for field in fields]
        for field in fields:
            ref = field.get("ref")
            if ref:
                fields.extend([{
                    "key": f'{field["key"]}-{f.id}',
                    "title": f'{field["title"]}{f.name}',
                    "meta": json.loads(f.meta),
                    "description": f.desc
                } for f in list_field(field_id=ref) if f.id != ref])

    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=fields)


async def row(request: web.Request):
    try:
        page = int(request.query.get('page', 1))
    except ValueError:
        page = 1
    try:
        size = int(request.query.get('size', 20))
    except ValueError:
        size = 20
    try:
        query = json.loads(request.query.get('query'))
    except TypeError:
        query = {}
    schema_id = request.query.get("schema_id")
    query_fields = request.query.getall("fields", [])

    if not schema_id:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))

    try:
        records, pagination = \
            list_record(schema_id=schema_id, query_fields=query_fields, page=page, size=size, query=query)
        data = dict(data=records, pagination=pagination)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=data)


async def relation_row(request: web.Request):
    try:
        page = int(request.query.get('page', 1))
    except ValueError:
        page = 1
    try:
        size = int(request.query.get('size', 20))
    except ValueError:
        size = 20
    try:
        query = json.loads(request.query.get('query'))
    except TypeError:
        query = {}
    schema_id = request.query.get("schema_id")
    query_fields = request.query.getall("fields", [])

    if not schema_id:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    records, pagination = \
        list_relation_record(schema_id=schema_id, query_fields=query_fields, page=page, size=size, query=query)
    data = dict(data=records, pagination=pagination)
    try:
        records, pagination = \
            list_relation_record(schema_id=schema_id, query_fields=query_fields, page=page, size=size, query=query)
        data = dict(data=records, pagination=pagination)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=data)


async def post_table(request: web.Request):
    payload = await request.json()
    name = payload.get("name")
    desc = payload.get("desc")
    if not name:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        add_schema(name=name, desc=desc)
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    
    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def post_column(request: web.Request):
    """
    :param request: name, desc, type, schema_id, meta: dict
    :return:
    """
    payload = await request.json()
    name = payload.get("name")
    desc = payload.get("desc")
    type_ = payload.get("type")
    schema_id = payload.get("schema_id")
    meta = payload.get("meta", {})
    if not all((name, schema_id)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        add_field(name=name, desc=desc, schema_id=schema_id, type=type_, meta_=meta)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))
        

async def post_row(request: web.Request):
    payload = await request.json()
    schema_id = payload.get("schema_id")
    values = payload.get("values", {})
    if not schema_id:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        add_entity(schema_id=schema_id, values=values)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def post_many_row(request: web.Request):
    payload = await request.json()
    schema_id = payload.get("schema_id")
    count = payload.get("count")
    ref = payload.get("ref")
    values = payload.get("values", {})

    if not schema_id:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    if not any((ref, count)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        if ref:
            id_ = ref.get("id")
            name = ref.get("name")
            for val in iter_value(id_):
                values.update({name: val.value})
                add_entity(schema_id=schema_id, values=values)
        else:
            count = int(count)
            for i in range(count):
                add_entity(schema_id=schema_id, values=values)
    except ValueError:
        return jsonify(errno=RET.PARAMERR, errmsg="参数错误")
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def delete_table(request: web.Request):
    payload = await request.json()
    id_ = payload.get("id")

    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        delete_schema(id_)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def delete_column(request: web.Request):
    payload = await request.json()
    id_ = payload.get("id")

    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        delete_field(id_)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def delete_row(request: web.Request):
    payload = await request.json()
    id_ = payload.get("id")

    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        delete_entity(id_)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def put_table(request: web.Request):
    payload = await request.json()
    id_ = payload.get("id")
    name = payload.get("name")
    desc = payload.get("desc")
    if not all((id_, name)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        update_schema(id_=id_, name=name, desc=desc)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def put_column(request: web.Request):
    """
    :param request: name, desc, type, schema_id, meta: dict
    :return:
    """
    payload = await request.json()
    name = payload.get("name")
    desc = payload.get("desc")
    type_ = payload.get("type")
    id_ = payload.get("id")
    meta = payload.get("meta", {})
    if not all((name, id_)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        update_field(id_=id_, name=name, desc=desc, type=type_, meta_=meta)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def put_row(request: web.Request):
    payload = await request.json()
    id_ = payload.get("id")
    values = payload.get("values", {})
    if not id_:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        update_entity(id_=id_, **values)
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


async def all_type(request: web.Request):
    try:
        data = {type_: types[type_].get_meta() for type_ in types if not type_.startswith("cmdb")}
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=data)


async def relations(request: web.Request):
    try:
        schemas = iter_schema()
        data = []
        for schema in schemas:
            fields = unique_fields(schema.id)
            data.append({
                "value": schema.id,
                "label": schema.name,
                "children": fields,
                "disabled": False if fields else True
            })
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=data)


async def value(request: web.Request) -> web.Response:

    ref = request.query.get('ref')
    if not ref:
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        page = int(request.query.get('page', 1))
    except ValueError:
        page = 1
    try:
        size = int(request.query.get('size', 20))
    except ValueError:
        size = 20
    try:
        values, pagination = list_value(id_=ref, page=page, size=size)
        data = [v.value for v in values]
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK), data=dict(data=data, pagination=pagination))


def _iter_row(io_):
    book = xlrd.open_workbook(file_contents=io_.read())
    sheet = book.sheet_by_index(0)
    nrows = sheet.nrows
    for row in range(nrows):
        yield sheet.row_values(row)


async def upload(request: web.Request):
    payload = await request.post()
    file = payload.get("file")
    schema_id = payload.get('schema_id')
    if not all((file, schema_id)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        data = _iter_row(file.file)
        header = next(data)
        fields = [field.name for field in list_field(schema_id)]
        assert set(header) & set(fields)
    except Exception as e:
        return jsonify(errno=RET.UPERR, errmsg=getmsg(RET.UPERR))

    try:
        for row_values in data:
            add_entity(schema_id, dict(zip(header, row_values)))
    except ValueError:
        return jsonify(errno=RET.VERR, errmsg=getmsg(RET.VERR))
    except CMDBError as e:
        return jsonify(errno=e.no, errmsg=getmsg(e.no))
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))

    return jsonify(errno=RET.OK, errmsg=getmsg(RET.OK))


def _generate_excel_io(schema_id, query, query_fields):
    profile = xlwt.Workbook()
    sheet = profile.add_sheet("Sheet")
    records = iter_record(schema_id=schema_id, query=query, query_fields=query_fields)
    for index, row in enumerate(records):
        for col, value in enumerate(row):
            sheet.write(index, col, value)

    from io import BytesIO
    io_ = BytesIO()
    profile.save(io_)
    try:
        return io_.getvalue()
    finally:
        io_.close()


async def download(request: web.Request):
    payload = await request.json()
    schema_id = payload.get("id")
    name = payload.get("name")
    if not all((schema_id, name)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        query = payload.get('query')
    except ValueError:
        query = {}
    try:
        query_fields = payload.get("fields", [])
        io_ = _generate_excel_io(schema_id, query, query_fields)
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    return web.Response(
        content_type='application/octet-stream',
        headers={'Content-Disposition': f'attachment;filename={name}'},
        body=io_)


def _generate_relation_excel_io(schema_id, query, query_fields):
    profile = xlwt.Workbook()
    sheet = profile.add_sheet("Sheet")
    records = iter_relation_record(schema_id=schema_id, query=query, query_fields=query_fields)
    for index, row in enumerate(records):
        for col, value in enumerate(row):
            sheet.write(index, col, value)

    from io import BytesIO
    io_ = BytesIO()
    profile.save(io_)
    try:
        return io_.getvalue()
    finally:
        io_.close()


async def relation_download(request: web.Request):
    payload = await request.json()
    schema_id = payload.get("id")
    name = payload.get("name")
    if not all((schema_id, name)):
        return jsonify(errno=RET.PARAMERR, errmsg=getmsg(RET.PARAMERR))
    try:
        query = payload.get('query')
    except ValueError:
        query = {}
    try:
        query_fields = payload.get("fields", [])
        io_ = _generate_relation_excel_io(schema_id, query, query_fields)
    except Exception as e:
        logger.error(e)
        return jsonify(errno=RET.UNKNOWN, errmsg=getmsg(RET.UNKNOWN))
    return web.Response(
        content_type='application/octet-stream',
        headers={'Content-Disposition': f'attachment;filename={name}'},
        body=io_)


async def currentUser(request: web.Request):
    data = {
        'name': 'Serati Ma',
        'avatar': 'https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png',
        'userid': '00000001',
        'email': 'antdesign@alipay.com',
        'signature': '海纳百川，有容乃大',
        'title': '交互专家',
        'group': '蚂蚁金服－某某某事业群－某某平台部－某某技术部－UED',
        'tags': [
            {
                'key': '0',
                'label': '很有想法的',
            },
            {
                'key': '1',
                'label': '专注设计',
            },
            {
                'key': '2',
                'label': '辣~',
            },
            {
                'key': '3',
                'label': '大长腿',
            },
            {
                'key': '4',
                'label': '川妹子',
            },
            {
                'key': '5',
                'label': '海纳百川',
            },
        ],
        'notifyCount': 12,
        'unreadCount': 11,
        'country': 'China',
        'geographic': {
            'province': {
                'label': '浙江省',
                'key': '330000',
            },
            'city': {
                'label': '杭州市',
                'key': '330100',
            },
        },
        'address': '西湖区工专路 77 号',
        'phone': '0752-268888888',
    }
    return json_response(data=data)


async def login_account(request: web.Request):
    payload = await request.json()
    userName = payload.get('userName')
    password = payload.get('password')
    type = payload.get('type')

    if password == 'ant.design' and userName == 'admin':
      return web.json_response(data={
        'status': 'ok',
        'type': type,
        'currentAuthority': 'admin',
      })

    if password == 'ant.design' and userName == 'user':
      return web.json_response({
        'status': 'ok',
        'type': type,
        'currentAuthority': 'user',
      })

    return web.json_response(data={
      'status': 'error',
      'type': type,
      'currentAuthority': 'guest',
    })


app = web.Application()
app.router.add_get("/table", table)
app.router.add_post("/table", post_table)
app.router.add_delete("/table", delete_table)
app.router.add_put("/table", put_table)
app.router.add_get("/column", column)
app.router.add_get("/all_column", all_column)
app.router.add_post("/column", post_column)
app.router.add_delete("/column", delete_column)
app.router.add_put("/column", put_column)
app.router.add_get("/row", row)
app.router.add_get("/relation_row", relation_row)
app.router.add_post("/row", post_row)
app.router.add_post("/many_row", post_many_row)
app.router.add_delete("/row", delete_row)
app.router.add_put("/row", put_row)
app.router.add_get("/types", all_type)
app.router.add_get("/relations", relations)
app.router.add_get("/value", value)
app.router.add_route("*", "/upload", upload)
app.router.add_post("/download", download)
app.router.add_post("/relation_download", relation_download)
app.router.add_get("/currentUser", currentUser)
app.router.add_post("/login/account", login_account)
web.run_app(app, host=settings.HOST, port=settings.PORT)
