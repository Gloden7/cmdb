import logging
import os
import json
import math
from logging import Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from settings import LOG_PATH
from cmdb.types_ import types


def get_logger(name="root", level=logging.ERROR, is_print=False):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    fmt = Formatter("%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s")

    log_path = os.path.join(LOG_PATH, name)

    if not os.path.exists(log_path):
        os.makedirs(log_path)

    fh = RotatingFileHandler(filename=f'{log_path}/err', maxBytes=1024*1024*100, backupCount=10)
    fh.setFormatter(fmt)
    fh.setLevel(level)
    logger.addHandler(fh)

    if is_print:
        sh = StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    return logger


class FieldMeta(dict):
    def __setattr__(self, key, value):
        raise NotImplementedError("Meta object cannot set!")

    def __getattr__(self, item):
        return self.get(item)

    def inspect(self, value):
        types[self.type].serialize(value=value, metadata=self)

    def get_meta(self, **kwargs):
        type_ = types.get(self.type)
        if type_ is None:
            raise TypeError('Illegal field type')
        meta = type_.get_meta(**kwargs)
        self.loads(meta)

    def equal(self, other, nullable=True):
        if nullable:
            return self.type == other.type and self.min == other.min and \
               self.max == other.max and self.len == other.len 
        return self.type == other.type and self.min == other.min and self.max == other.max and \
         self.len == other.len and self.nullable == other.nullable

    def loads(self, d):
        if isinstance(d, str):
            d = json.loads(d)
        for k, v in d.items():
            if isinstance(v, dict):
                self[k] = FieldMeta(v)
            else:
                self[k] = v
        return self

    def dumps(self):
        return json.dumps(self)


def pagination(size: int, page: int, query):
    page = page if page > 0 else 1
    size = size if 0 < size < 101 else 20
    count = query.count()
    pages = math.ceil(count/size)
    results = query.limit(size).offset((page-1)*size).all()
    return results, dict(page=page, size=size, count=count, pages=pages)


def itemiter(query, dispatch=100):
    page = 1
    while True:
        results, _ = pagination(dispatch, page, query)
        if not results:
            break
        yield from results
        page += 1


class RET:
    OK = 0
    VERR = 1
    UNKNOWN = 2
    PARAMERR = 3
    UPERR = 4
    DBERR = 1000
    DBFIELDERR = 1100
    DBENTITYERR = 1200
    DBVALUEERR = 1300


RETMSG = {
    0: '请求成功',
    1: '数据不存在',
    2: '未知异常',
    3: '参数不完整',
    4: '文件格式错误',
    1101: '关联字段不存在',
    1102: '非唯一字段不能作为外键',
    1103: '关联字段元属性不匹配',
    1104: '不能添加唯一索引，表不为空',
    1105: '不能添加字段，没有默认值',
    1106: '不能删除字段，存在依赖',
    1107: '不能设置唯一索引，值不为唯一',
    1108: '不能设置为非多值，存在多值',
    1109: '关联字段不存在',
    1110: '关联字段类型不匹配',
    1111: '不能设置非唯一索引，有关联字段',
    1112: '关联字段存在冲突',
    1113: '不能更新字段，值不匹配',
    # 1201: '无效值',
    # 1202: '无效值，值不唯一',
    # 1203: '无效值，无对应关联值',
    1301: '值不存在',
    1302: '无效值',
    1303: '无效值，值不唯一',
    1304: '无效值，无对应关联值',
    1305: '不能修改值，因为在其他关联表中使用',
    1306: '不能删除值，因为在其他关联表中使用',
}

getmsg = lambda x: RETMSG.get(x, "未知异常")


if __name__ == '__main__':
    logger = get_logger(__name__, logging.INFO, True)
    # print(logger.level)
    a = dict(name="luojing", age=18, relation=dict(brother=dict(name="zihang")), min=10)
    b = dict(name="luojing", age=23, relation=dict(brother=dict(name="zihang"), sister={}), min=10)

    m = FieldMeta(type="Int")

    m.get_meta()
    print(m)
    m.loads({"type": "String"})
    m.get_meta()

    print(m)
    print(bool(m.relation))
