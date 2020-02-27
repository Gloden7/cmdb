import ipaddress
from datetime import datetime


class BaseType:
    def __init__(self):
        raise NotImplementedError("BaseType class Cannot instantiate")

    @classmethod
    def serialize(cls, value, metadata=None):
        raise NotImplementedError("BaseType not implement method serialize")

    @classmethod
    def unserialize(cls, value):
        raise NotImplementedError("BaseType not implement method unserialize")

    @classmethod
    def get_meta(cls, **kwargs):
        nullable = kwargs.get("nullable", True)
        unique = kwargs.get("unique", False)
        multiple = kwargs.get("multiple", False)
        relation = kwargs.get("relation", {})
        default = kwargs.get("default")
        return dict(nullable=nullable, unique=unique, multiple=multiple, relation=relation, default=default)
        # raise NotImplementedError("BaseType not implement method get_meta")


class String(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        len_ = metadata.get("len")

        if len_:
            assert len(value) <= len_, "The length of the value exceeds the limit"
        return value

    @classmethod
    def unserialize(cls, value):
        return value

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        len_ = kwargs.get("len", None)
        meta.update(dict(len=len_, type="String"))
        return meta


class Int(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        if value:
            value = int(value)
            min_ = metadata.get("min")
            if min_:
                assert value > min_, f"Value cannot be less than {min_}"
            max_ = metadata.get("max")
            if max_:
                assert value < max_, f"Value cannot be greater than {max_}"
        return value

    @classmethod
    def unserialize(cls, value):
        return int(value)

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        min_ = kwargs.get("min")
        max_ = kwargs.get("max")
        meta.update(dict(min=min_, max=max_, type="Int"))
        return meta


class Float(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        if value:
            value = float(value)
            min_ = metadata.get("min")
            if min_:
                assert value > min_, f"Value cannot be less than {min_}"
            max_ = metadata.get("max")
            if max_:
                assert value < max_, f"Value cannot be greater than {max_}"
        return value

    @classmethod
    def unserialize(cls, value):
        return int(value)

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        min_ = kwargs.get("min")
        max_ = kwargs.get("max")
        meta.update(dict(min=min_, max=max_, type="Float"))
        return meta


class Date(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        if value:
            datetime.strptime(value, "%Y-%m-%d")
        return value

    @classmethod
    def unserialize(cls, value):
        return value

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        meta.update(type="Date")
        return meta


class DateTime(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        if value:
            datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return value

    @classmethod
    def unserialize(cls, value):
        return value

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        meta.update(type="DateTime")
        return meta


class Ip(BaseType):
    @classmethod
    def serialize(cls, value, metadata=None):
        nullable = metadata.get("nullable", True)
        if not nullable:
            assert value, "Value is cannot be empty"
        if value:
            assert ipaddress.ip_address(value), "Value is not legal IP"
        return value

    @classmethod
    def unserialize(cls, value):
        return value

    @classmethod
    def get_meta(cls, **kwargs):
        meta = super().get_meta(**kwargs)
        meta.update(type="Ip")
        return meta


types = {}


def inject():
    for key, value in globals().items():
        if not isinstance(value, type):
            continue
        if issubclass(value, BaseType) and key != 'BaseType':
            types[key] = value
            types[f"cmdb.typs.{key}"] = value


inject()
