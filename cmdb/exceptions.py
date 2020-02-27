class CMDBError(Exception):
    """CMDB Base Error"""

    def __init__(self, no, msg):
        self.no = no
        self.msg = msg

    def __str__(self):
        return self.msg



class CMDBSchemaError(CMDBError):
    """ this is cmdb's Exception for check the schema """

    def __init__(self, no, msg):  # real signature unknown
        self.no = no
        self.msg = msg

    def __str__(self):
        return self.msg


class CMDBFieldError(CMDBError):
    """ this is cmdb's Exception for check the field """

    def __init__(self, no, msg):  # real signature unknown
        self.no = no
        self.msg = msg

    def __str__(self):
        return self.msg


class CMDBEntityError(CMDBError):
    """ this is cmdb's Exception for check the entity """

    def __init__(self, no, msg):  # real signature unknown
        self.no = no
        self.msg = msg

    def __str__(self):
        return self.msg


class CMDBValueError(CMDBError):
    """ this is cmdb's Exception for check the value """

    def __init__(self, no, msg):  # real signature unknown
        self.no = no
        self.msg = msg

    def __str__(self):
        return self.msg


if __name__ == '__main__':
    try:
        raise CMDBFieldError(11, "cmdbField")
    except CMDBError as e:
        print(e)
        print(e.no)