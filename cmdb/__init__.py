from cmdb.models import session, Schema, Field, Entity, Value
from cmdb.tools import get_logger, FieldMeta, pagination, itemiter
from cmdb.exceptions import *
from datetime import datetime
import uuid

logger = get_logger("cmdb", is_print=False)


def add_schema(name: str, desc: str = None):
    try:
        schema = Schema(name=name, desc=desc)
        session.add(schema)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e


def delete_schema(id_: int):
    schema = session.query(Schema).get(id_)
    if schema is None:
        raise ValueError(f"Schema with ID {id_} does not exist")
    try:
        fields = list_field(schema_id=id_)
        for field in fields:
            delete_field(field.id)
        entities = iter_entity(id_)
        for entity in entities:
            delete_entity(entity.id)
        schema.is_delete = True
        session.add(schema)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e


def update_schema(id_: int, name: str, desc: str=None):
    schema = session.query(Schema).get(id_)
    if schema is None:
        raise ValueError(f"Schema with ID {id_} does not exist")
    try:
        if name:
            schema.name = name
        if desc:
            schema.desc = desc

        session.add(schema)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e


def list_schema(page: int = 1, size: int = 20, query: {} = None):
    cond = Schema.is_delete == False
    if query:
        name = query.get("name")
        if name:
            cond &= Schema.name.like(f'%{name}%')
        desc = query.get("desc")
        if desc:
            cond &= Schema.desc.like(f'%{desc}%')
        createtime = query.get("createtime")
        if createtime:
            created = datetime.strptime(createtime, "%Y-%m-%d %H:%M:%S")
            cond &= Schema.createtime > created

    query = session.query(Schema).filter(cond)
    return pagination(size=size, page=page, query=query)


def iter_schema():
    query = session.query(Schema).filter((Schema.is_delete == False))
    return itemiter(query=query)


def unique_fields(schema_id):
    fields = list_field(schema_id=schema_id)
    data = []
    for field in fields:
        meta = FieldMeta().loads(field.meta)
        if meta.unique:
            data.append({
                "value": field.id,
                "label": field.name
            })
    return data


def _add_field(name: str, schema_id: int, meta: dict, desc: str = None, ref: id = None):
    try:
        field = Field()
        field.name = name
        field.desc = desc
        field.schema_id = schema_id
        field.meta = meta
        field.ref = ref
        session.add(field)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e
    return field


def add_field(name: str, schema_id: int, desc: str = None, type: str = "String", meta_: {} = None):
    """
    :param name:
    :param schema_id:
    :param desc:
    :param type:
    :param kwargs: nullable, unique, multiple, relation, min, max, len,
    :return:
    """
    meta = FieldMeta(type=type)
    meta.get_meta(**meta_)

    schema = session.query(Schema).get(schema_id)
    if schema is None:
        raise ValueError("The table to which the field belongs does not exist")

    ref_id = None

    if meta.relation and meta.relation.target:

        ref_field = session.query(Field)\
            .filter((Field.is_delete==False)&(Field.id==meta.relation.target)).first()
        if ref_field is None:  # 关联字段不存在
            raise CMDBFieldError(1101, "Associated field does not exist")
        ref_meta = FieldMeta().loads(ref_field.meta)
        if not ref_meta.unique:
            raise CMDBFieldError(1102, "Non unique field cannot be a foreign key")
        if not meta.equal(ref_meta, nullable=False):  # 关联字段元属性不合法
            raise CMDBFieldError(1103, "The meta property of the associated field does not match")
        ref_id = ref_field.id

    has_entity = session.query(Entity).filter((Entity.is_delete==False)&(Entity.schema_id==schema_id)).first()
    if not has_entity:
        _add_field(name=name, schema_id=schema_id, desc=desc, meta=meta.dumps(), ref=ref_id)
    else:
        if meta.unique:
            raise CMDBFieldError(1104, "Cannot set unique index because table is not empty")
        if not meta.nullable:
            if not meta.default:
                raise CMDBFieldError(1105, "Cannot add field because field does have default value")

        field = _add_field(name=name, schema_id=schema_id, desc=desc, meta=meta.dumps(), ref=ref_id)
        try:
            entities = iter_entity(schema_id)
            for entity in entities:
                _add_value(meta=meta, value=meta.default, entity=entity, field=field)
            session.commit()
        except Exception as e:
            logger.error(e)
            session.rollback()
            field.is_delete=True
            session.commit()
            raise e


def delete_field(id_: int):
    """
    删除字段，删除字段前查询其管理的对象
    :param id_:
    :return: None
    """

    field = session.query(Field).filter((Field.is_delete==False)&(Field.id==id_)).first()
    if field is None:
        raise ValueError(f"Field with ID {id_} does not exist")

    ref_field = session.query(Field).filter((Field.is_delete==False)&(Field.ref==id_)).first()
    if ref_field:
        raise CMDBFieldError(1106, f'Cannot delete field {field.name} because there are dependencies')

    field.is_delete = True
    query = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==field.id))
    values = itemiter(query)
    for value in values:
        _delete_value(value)
    fields = session.query(Field)\
        .filter((Field.is_delete==False)&(Field.schema_id==field.schema_id)).all()
    if not fields:
        entities = iter_entity(schema_id=field.schema_id)
        for entity in entities:
            entity.is_delete = False
    try:
        session.add(field)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e


def update_field(id_: int, name: str = None, desc: str = None, type: str = None, meta_: {} = None):

    field = session.query(Field).get(id_)
    if field is None:
        raise ValueError(f"Field with ID {id_} does not exist")
    src_meta = FieldMeta().loads(field.meta)
    if type or meta_:
        if not type:
            type = src_meta.type
        meta = FieldMeta(type=type)
        meta.get_meta(**meta_)
    else:
        meta = src_meta
    if name:
        field.name = name
    if desc:
        field.desc = desc
    if type:
        field.type = type

    if src_meta != meta:
        if meta.unique != src_meta.unique:
            if src_meta.unique:
                s = session.execute("""
                select count(v.id) as c
                from `value` as v
                where v.is_delete=0 and v.field_id=:field_id
                group by v.value
                having count(c)>1;
                """,{"field_id": field.id}).first()
                if s:
                    raise CMDBFieldError\
                            (1107, "Unique constraint of field cannot be modified, because the value is not unique")
            else:
                s = session.query(Field)\
                    .filter((field.is_delete==False)&(Field.ref==field.id)).first()
                if s:
                    raise CMDBFieldError\
                        (1111, "Unique constraint of field cannot be modified, because has association field")
        if meta.multiple != src_meta.multiple and not meta.multiple:
            s = session.execute("""
            select count(v.id)
            from `value` as v
            where v.is_delete=0 and v.field_id=:field_id
            group by v.entity_id
            having count(v.id)>1;
            """, {'field_id': field.id}).first()
            if s:
                raise CMDBFieldError \
                    (1108, "Multi value constraint of field cannot be modified, because the value has multiple values")
        if meta.relation != src_meta.relation and meta.relation:
            target = session.query(Field).filter((Field.is_delete==False)&(Field.id==meta.relation.target)).first()
            if target is None:
                raise CMDBFieldError(1109, "The associated target field does not exist")
            ref_meta = FieldMeta().loads(target.meta)
            if ref_meta.type != src_meta.type:
                raise CMDBFieldError(1110, "Association target field type error")
            if not ref_meta.unique:
                raise CMDBFieldError(1102, "Non unique field cannot be a foreign key")
            has_conflict = session.execute("""
            select count(source.id)
            from `value` as source
            left join `value` as target on source.value=target.value and target.field_id=2 and target.is_delete=0
            where target.value is null and source.value is not null and source.field_id=3 and source.is_delete=0;
            """).first()
            if has_conflict[0]:
                raise CMDBFieldError(1112, "Association target field has conflict")
            field.ref = meta.relation.target
        if not meta.equal(src_meta):
            query = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==field.id))
            values = itemiter(query)
            try:
                for value in values:
                    meta.inspect(value.value)
            except Exception as e:
                # logger.error(e)
                raise CMDBFieldError(1113, "Cannot update field meta because there is a value mismatch")
        field.meta = meta.dumps()
    try:
        session.add(field)
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise e


def list_field(schema_id: object = None, field_id: object = None):
    if field_id:
        field = session.query(Field).filter((Field.is_delete==False)&(Field.id==field_id)).first()
        if field is None:
            raise ValueError(1301, f"Field with ID {field_id} does not exist")
        schema_id = field.schema_id
    query = session.query(Field).filter((Field.is_delete==False)&(Field.schema_id==schema_id))
    return query.all()


def _add_value(meta: dict, value: str, field: Field, entity: Entity):
    try:
        meta.inspect(value)
    except Exception as e:
        raise CMDBValueError(1302, "Invalid value")
    if meta.unique:
        v = session.query(Value).filter(
            (Value.is_delete == False) & (Value.field_id == field.id) & (Value.value == value)).first()
        if v:
            raise CMDBValueError(1303, "Invalid value because value is not unique")
    if value and field.ref:
        has_ = session.query(Value)\
            .filter((Value.is_delete==False) & (Value.field_id==field.ref)&(Value.value==value)).first()
        if not has_:
            raise CMDBValueError(1304, "Invalid value because association value does not exits")

    v = Value(value=value, entity_id=entity.id, field_id=field.id)
    session.add(v)


def add_entity(schema_id: int = None, values: dict = None):
    schema = session.query(Schema).filter((Schema.is_delete==False)&(Schema.id==schema_id)).first()
    if schema is None:
        raise ValueError("The table to which the entity belongs does not exist")
    fields = list_field(schema_id)

    entity = Entity()
    try:
        entity.key = uuid.uuid4().hex
        entity.schema_id = schema_id
        session.add(entity)
        session.commit()

        for field in fields:
            value = values.get(field.name)
            meta = FieldMeta().loads(field.meta)

            if meta.multiple and isinstance(value, list):
                for val in value:
                    _add_value(meta=meta, value=val, field=field, entity=entity)
            else:
                _add_value(meta=meta, value=value, field=field, entity=entity)

        session.commit()
    except Exception as e:
        if type(e) != CMDBEntityError:
            logger.error(e)
        session.rollback()
        entity.is_delete = True
        session.commit()
        raise e


def update_value(meta: dict, val: str, value: Value = None, field: Field = None, id_: int = None):
    if value is None and id_:
        value = session.query(Value).filter((Value.is_delete==False)&(Value.id==id_)).first()
        if value is None:
            raise CMDBValueError(1301, f"Value with ID {id_} does not exist")

        field = session.query(Field).filter((Field.is_delete==False)&(Field.id==Value.field_id))

    try:
        meta.inspect(val)
    except Exception as e:
        raise CMDBValueError(1302, "Invalid value")

    if value and field.ref:
        has_ = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==field.ref)&(Value.value==val)).first()
        if not has_:
            raise CMDBValueError(1304, "Invalid value because association value does not exits")

    if meta.unique:
        v = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==field.id)&(Value.value==val)).first()
        if v:
            raise CMDBValueError(1303, "Invalid value because value is not unique")

        ref_fields = session.query(Field).filter((Field.is_delete == False) & (Field.ref == field.id)).all()

        if ref_fields:
            for ref_field in ref_fields:
                meta = FieldMeta().loads(ref_field.meta)
                if meta.relation.update_cascade == 'update':
                    query = session.query(Value)\
                        .filter((Value.is_delete==False)&(Value.field_id==ref_field.id)&(Value.value==v.value))
                    ref_values = itemiter(query)
                    for ref_value in ref_values:
                        update_value(meta=meta, val=val, value=ref_value, field=field)
                else:
                    raise CMDBValueError(1305, "Cannot be updated because the value is used in other associated fields")

    value.value=val


def _delete_value(value: Value):

    ref_fields = session.query(Field).filter((Field.is_delete==False)&(Field.ref==value.field_id)).all()
    if ref_fields:  # 级联处理
        for ref_field in ref_fields:
            meta = FieldMeta().loads(ref_field.meta)
            if meta.relation.cascade == 'set_null':  # 级联值设置为null
                ref_values = session.query(Value)\
                    .filter((Value.is_delete==False)&(Value.field_id==ref_field.id)&(Value.value==value.value)).all()
                for ref_value in ref_values:
                    update_value(meta=meta, val=None, value=ref_value, field=ref_field)
            elif meta.relation.cascade == 'delete':  # 级联删除
                ref_values = session.query(Value) \
                    .filter((Value.is_delete == False) & (Value.field_id == ref_field.id) & (Value.value == value.value)).all()
                for ref_value in ref_values:
                    delete_entity(ref_value.entity)
            else:  # 没有级联值
                raise CMDBValueError(1306, "Cannot be deleted because the value is used in other associated fields")
    value.is_delete = True


def delete_entity(id_: int):
    entity = session.query(Entity).filter((Entity.is_delete==False)&(Entity.id==id_)).first()
    if entity is None:
        raise ValueError(f"Entity with ID {id_} does not exist")

    values = session.query(Value).filter((Value.is_delete==False)&(Value.entity_id==id_)).all()

    try:
        entity.is_delete = True
        for value in values:
            _delete_value(value)

        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(e)
        raise e


def _update_value(meta: dict, value, field, entity):
    try:
        meta.inspect(value)
    except Exception as e:
        raise CMDBValueError(1302, "Invalid value")
    if meta.unique:
        v = session.query(Value).filter(
            (Value.is_delete == False) & (Value.field_id == field.id) & (Value.value == value)).first()
        if v:
            raise CMDBValueError(1303, "Invalid value because value is not unique")

        # 级联更新
        ref_fields = session.query(Field).filter((Field.is_delete == False) & (Field.ref == field.id)).all()

        if ref_fields:
            for ref_field in ref_fields:
                meta = FieldMeta().loads(ref_field.meta)
                if meta.relation.update_cascade == 'update':
                    ref_values = session.query(Value) \
                        .filter(
                        (Value.is_delete == False) & (Value.field_id == ref_field.id) & (Value.value == v.value)).all()
                    for ref_value in ref_values:
                        update_value(meta=meta, val=value.value, value=ref_value, field=field)
                else:
                    raise CMDBValueError(1305, "Cannot be updated because the value is used in other associated fields")

    if value and field.ref:
        has_ = session.query(Value).filter(
            (Value.is_delete == False) & (Value.field_id == field.ref) & (Value.value == value)).first()
        if not has_:
            raise CMDBValueError(1304, "Invalid value because association value does not exits")

    v = session.query(Value) \
        .filter((Value.is_delete == False) & (Value.entity_id == entity.id) & (Value.field_id == field.id)).first()

    if v is None:
        raise CMDBValueError(1301, "Value does not exist")

    v.value = value


def _update_multiple_value(meta: dict, values: list, field, entity):

    vs = session.query(Value) \
        .filter((Value.is_delete == False) & (Value.entity_id == entity.id) & (Value.field_id == field.id)).all()

    if not vs:
        raise CMDBValueError(1301, "Values does not exist")

    values_len = len(values)
    vs_len = len(vs)

    if values_len == vs_len:  # 情况1：修改值的数量和原有值数量一样多
        for i in range(vs_len):
            v = vs[i]
            update_value(meta=meta, val=values[i], value=v, field=field)

    elif values_len > vs_len:  # 情况2： 修改值的数量比原有值数量多
        for i in range(vs_len):
            v = vs[i]
            update_value(meta=meta, val=values[i], value=v, field=field)
        for i in range(vs_len, values_len):
            _add_value(meta=meta, value=values[i], entity=entity, field=field)
    else:  # 情况3：修改值数量比原有值数量少
        for i in range(values_len):
            v = vs[i]
            update_value(meta=meta, val=values[i], value=v, field=field)
        for i in range(values_len, vs_len):
            v = vs[i]
            _delete_value(v)


def update_entity(id_: int, **kwargs):
    entity = session.query(Entity).filter((Entity.is_delete==False)&(Entity.id==id_)).first()
    if entity is None:
        raise ValueError(f"Entity with ID {id_} does not exist")
    fields = session.query(Field).filter((Field.is_delete==False) & (Field.schema_id==entity.schema_id)).all()

    try:
        for field in fields:
            value = kwargs.get(field.name)
            if not value:
                continue
            meta = FieldMeta().loads(field.meta)

            if meta.multiple and isinstance(value, list):
                _update_multiple_value(meta=meta, values=value, field=field, entity=entity)
            else:
                _update_value(meta=meta, value=value, field=field, entity=entity)
        session.commit()
    except Exception as e:
        if type(e) != CMDBEntityError:
            logger.error(e)
        session.rollback()
        raise e


def iter_entity(schema_id: int, query=None, fields=None):
    cond = (Entity.is_delete==False)&(Entity.schema_id==schema_id)
    if query and fields:
        for field in fields:
            cond_val = query.get(field.name)
            if cond_val:
                cond &= (Value.field_id == field.id) & (Value.value.like(f'%{cond_val}%'))
        query = session.query(Entity) \
            .join(Value, (Value.entity_id == Entity.id) & (Entity.schema_id == schema_id)).filter(cond)
    else:
        query = session.query(Entity).filter(cond)
    return itemiter(query)


def list_entity(schema_id: int, page: int, size: int, query: dict = None, fields: list = None):
    cond = (Entity.is_delete==False) & (Entity.schema_id==schema_id)
    if query and fields:
        for field in fields:
            cond_val = query.get(field.name)
            if cond_val:
                cond &= (Value.field_id==field.id)&(Value.value.like(f'%{cond_val}%'))
        query = session.query(Entity)\
            .join(Value, (Value.entity_id == Entity.id) & (Entity.schema_id == schema_id)).filter(cond)
    else:
        query = session.query(Entity).filter(cond)
    return pagination(size=size, page=page, query=query)


def get_entity(fields, id_, name, val) -> {}:
    value = session.query(Value) \
        .filter((Value.is_delete==False)&(Value.value==val)&(Value.field_id==id_)).first()
    entity = session.query(Entity) \
        .filter((Entity.is_delete==False)&(Entity.id==value.entity_id)).first()
    record = {}

    for field in fields:
        if field.id != id_:
            if entity:
                meta = FieldMeta().loads(field.meta)
                values = session.query(Value) \
                    .filter((Value.is_delete==False)&(Value.entity_id==entity.id)&(Value.field_id==field.id))
                if meta.multiple:
                    value = [val.value for val in values.all()]
                else:
                    value = values.first().value
            else:
                value = ""
            record.update({f"{name}{field.name}": value})
     
    return record


def _format_record(entities: list, fields: list) -> list:
    records = []
    for entity in entities:
        record = entity.todict()
        for field in fields:
            meta = FieldMeta().loads(field.meta)
            values = session.query(Value) \
                .filter((Value.is_delete == False) & (Value.entity_id == entity.id) & (Value.field_id == field.id))
            if meta.multiple:
                value = [val.todict() for val in values.all()]
            else:
                value = values.first().todict()
            record.update({field.name: value})
        records.append(record)
    return records


def list_record(schema_id: int, query_fields: list = None, page: int = None, size: int = None, query: dict = None) -> tuple:
    fields = list_field(schema_id)
    entities, pagination = list_entity(schema_id, page, size, query=query, fields=fields)
    if not entities or not fields:
        return [], pagination
    if query_fields:
        fields = tuple(filter(lambda x: x.name in query_fields, fields))

    records = _format_record(entities, fields)
    return records, pagination


def list_relation_record(schema_id: int, query_fields: list = None, page: int = None, size: int = None, query: dict = None) -> tuple:
    fields = list_field(schema_id=schema_id)
    entities, pagination = list_entity(schema_id, page, size, query=query, fields=fields)
    if not entities or not fields:
        return [], pagination
    if query_fields:
        fields = tuple(filter(lambda x: x.name in query_fields, fields))

    records = []
    ref_fields = {}
    for ref_field in filter(lambda x: x.ref, fields):
        temp = session.query(Field).filter((Field.is_delete==False)&(Field.id==ref_field.ref)).first()
        _fields = list_field(schema_id=temp.schema_id)
        ref_fields[ref_field.id] = _fields

    for entity in entities:
        record = entity.todict()
        ref_values = {}
        for field in fields:
            meta = FieldMeta().loads(field.meta)
            values = session.query(Value) \
                .filter((Value.is_delete==False) & (Value.entity_id==entity.id)&(Value.field_id==field.id))
            if meta.multiple:
                values = values.all()
                if field.ref:
                    ref_values[field.id] = [field.ref, field.name, val[0].value]
                value = [val.value for val in values]
            else:
                value = values.first()
                if field.ref:
                    ref_values[field.id] = [field.ref, field.name, value.value]
                value = value.value
            record.update({field.name: value})
        for id_, value in ref_values.items():
            ref, name, val = value
            _fields = ref_fields.get(id_)
            record.update(get_entity(_fields, ref, name, val))
        records.append(record)
    return records, pagination


def iter_record(schema_id: int, query: dict = None, query_fields: list = None):
    fields = list_field(schema_id)
    entities = iter_entity(schema_id, query=query, fields=fields)
    if query_fields:
        fields = tuple(filter(lambda x: x.name in query_fields, fields))
    yield [field.name for field in fields]
    for entity in entities:
        row = []
        for field in fields:
            meta = FieldMeta().loads(field.meta)
            values = session.query(Value) \
                .filter((Value.is_delete==False)&(Value.entity_id==entity.id)&(Value.field_id==field.id))
            if meta.multiple:
                value = [val.value for val in values.all()]
            else:
                value = values.first().value
            row.append(value)
        yield row


def get_relation_entity(fields, id_, name, val, query_fields) -> {}:
    value = session.query(Value) \
        .filter((Value.is_delete==False)&(Value.value==val)&(Value.field_id==id_)).first()
    if value:
        entity = session.query(Entity) \
            .filter((Entity.is_delete==False)&(Entity.id==value.entity_id)).first()
    else:
        entity = None
    record = []

    for field in fields:
        if field.id != id_ and (name+field.name) in query_fields:
            if entity:
                meta = FieldMeta().loads(field.meta)
                values = session.query(Value) \
                    .filter((Value.is_delete==False)&(Value.entity_id==entity.id)&(Value.field_id==field.id))
                if meta.multiple:
                    value = ",".join([val.value for val in values.all()])
                else:
                    value = values.first().value
            else:
                value = ""
            record.append(value)
    return record


def iter_relation_record(schema_id: int, query: dict = None, query_fields: list = None):
    fields = list_field(schema_id)
    entities = iter_entity(schema_id, query=query, fields=fields)

    ref_fields = {}
    for ref_field in filter(lambda x: x.ref, fields):
        temp = session.query(Field).filter((Field.is_delete==False)&(Field.id==ref_field.ref)).first()
        _fields = list_field(schema_id=temp.schema_id)
        ref_fields[ref_field.id] = (ref_field.name, ref_field.ref, _fields)
    if query_fields:
        fields = tuple(filter(lambda x: x.name in query_fields, fields))
        yield query_fields
    else:
        yield [field.name for field in fields]
    for entity in entities:
        row = []
        ref_values = {}
        for field in fields:
            meta = FieldMeta().loads(field.meta)
            values = session.query(Value) \
                .filter((Value.is_delete==False)&(Value.entity_id==entity.id)&(Value.field_id==field.id))
            if meta.multiple:
                value = [val.value for val in values.all()]
                if field.ref:
                    ref_values[field.id] = value[0]
                value = ",".join(value)
            else:
                value = values.first().value
                if field.ref:
                    ref_values[field.id] = value
            row.append(value)

        for id_, value in ref_fields.items():
            name, ref, fields = value
            val = ref_values.get(id_)
            row.extend(get_relation_entity(fields, ref, name, val, query_fields))

        yield row


def list_value(id_: int, page: int, size: int):
    query = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==id_))
    return pagination(size=size, page=page, query=query)


def iter_value(id_: int):
    query = session.query(Value).filter((Value.is_delete==False)&(Value.field_id==id_))
    return itemiter(query)
