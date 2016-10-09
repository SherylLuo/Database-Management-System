import json, re
from collections import namedtuple
from struct import Struct

class Types:
  """
  Utility functions for database types.

  The 'types' dictionary defines a mapping from user-facing type
  primitives to their representation in the Python 'struct' module,
  and a boolean indicating whether the type requires a repeat count suffix.

  The list of supported types in the database is given by the keys
  of the 'types' dictionary.
  """
  types = {
      'byte'    : ('B', False, 0),
      'short'   : ('h', False, 0),
      'int'     : ('i', False, 0),
      'float'   : ('f', False, 0.0),
      'double'  : ('d', False, 0.0),
      'char'    : ('s', True, chr(0)),
      'text'    : ('s', True, chr(0))
    }

  @classmethod
  def parseType(cls, typeDesc):
    typeMatcher = re.compile("(?P<typeStr>\w+)(\((?P<size>\d+)\))?(?P<rest>.*)")
    match = typeMatcher.match(typeDesc)
    if match:
      return match.groupdict()

  @classmethod
  def formatType(cls, typeDesc):
    """
    Converts a type description string into a C-struct format.

    >>> Types.formatType('int')
    'i'

    Character sequences require a fixed-length declaration.

    >>> Types.formatType('char(100)')
    '100s'

    Invalid type description examples.

    >>> Types.formatType('int(100)') == None
    True
    >>> Types.formatType('char') == None
    True
    >>> Types.formatType('char(100') == None
    True
    >>> Types.formatType('char100)') == None
    True
    >>> Types.formatType('char(100)asdsa') == None
    True
    """
    format = None
    matches = Types.parseType(typeDesc)
    if matches:
      typeStr = matches.get("typeStr", None)
      size    = matches.get("size", None)
      rest    = matches.get("rest", None)
      if not rest:
        (format, requiresSize, _) = Types.types.get(typeStr, (None, None, None))
        if requiresSize:
          format = size+format if size else None
        else:
          format = format if not size else None
    
    return format


  @classmethod
  def defaultValue(cls, typeDesc):
    """
    Returns a default value for the given type.

    >>> Types.defaultValue('int') == 0
    True
    >>> Types.defaultValue('int(100)') == None
    True
    >>> Types.defaultValue('float') == 0.0
    True
    >>> Types.defaultValue('double') == 0.0
    True
    >>> Types.defaultValue('char(100)') == (chr(0) * 100)
    True
    """
    default = None
    matches = Types.parseType(typeDesc)
    if matches:
      typeStr = matches.get("typeStr", None)
      size    = matches.get("size", None)
      rest    = matches.get("rest", None)
      if not rest:
        (_, requiresSize, val) = Types.types.get(typeStr, (None, None, None))
        if requiresSize:
          default = val * int(size) if size else None
        else:
          default = val if not size else None

    return default


  @classmethod
  def formatValue(cls, value, typeDesc, forSerialization=True):
    """
    Performs any type conversion necessary to process the given
    value as the given type during serialization.

    For now, this converts character sequences from Python strings
    into bytes for Python's struct module.
    """
    prefixes = ['char', 'text']
    if list(filter(typeDesc.startswith, prefixes)):
      if forSerialization:
        return value.encode() if isinstance(value, str) else value
      else:
        return value.decode() if isinstance(value, bytes) else value
    else:
      return value


class DBSchema:
  """
  A database schema class to represent the type of a relation.
  
  Schema definitions require a name, and a list of attribute-type pairs.

  This schema class maintains the above information, as well as Python
  'namedtuple' and 'struct' instances to provide an in-memory object and
  binary serialization/deserialization facilities.

  That is, a Python object corresponding to an instance of the schema can
  easily be created using our 'instantiate' method.

  >>> schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])
  
  >>> e1 = schema.instantiate(1, '1990-01-01', 100000)
  >>> e1
  employee(id=1, dob='1990-01-01', salary=100000)

  Also, we can serialize/deserialize the created instances with the 'pack'
  and 'unpack' methods.

  (Note the examples below escape the backslash character to ensure doctests
  run correctly. These escapes should be removed when copy-pasting into the Python REPL.)

  >>> schema.pack(e1)
  b'\\x01\\x00\\x00\\x001990-01-01\\x00\\x00\\xa0\\x86\\x01\\x00'
  >>> schema.unpack(b'\\x01\\x00\\x00\\x001990-01-01\\x00\\x00\\xa0\\x86\\x01\\x00')
  employee(id=1, dob='1990-01-01', salary=100000)

  >>> e2 = schema.unpack(schema.pack(e1))
  >>> e2 == e1
  True

  Finally, the schema description itself can be serialized with the packSchema/unpackSchema
  methods. One example use-case is in our self-describing storage files, where the files
  include the schema of their data records as part of the file header.
  >>> schemaDesc = schema.packSchema()
  >>> schema2 = DBSchema.unpackSchema(schemaDesc)
  >>> schema.name == schema2.name and schema.schema() == schema2.schema()
  True

  # Test default tuple generation
  >>> d = schema.default()
  >>> d.id == 0 and d.dob == (chr(0) * 10) and d.salary == 0
  True
  """
  
  def __init__(self, name, fieldsAndTypes):
    self.name = name
    if self.name and fieldsAndTypes:
      self.fields  = [x[0] for x in fieldsAndTypes]
      self.types   = [x[1] for x in fieldsAndTypes]
      self.clazz   = namedtuple(self.name, self.fields)
      self.binrepr = Struct(''.join([Types.formatType(x) for x in self.types]))
      self.size    = self.binrepr.size
    else:
      raise ValueError("Invalid attributes when constructing a schema")

  def schema(self):
    if self.fields and self.types:
      return list(zip(self.fields, self.types))

  def default(self):
    if self.clazz:
      return self.clazz(*map(Types.defaultValue, self.types))

  def instantiate(self, *args):
    if self.clazz:
      return self.clazz(*args)

  def pack(self, instance):
    if self.binrepr:
      values = [Types.formatValue(instance[i], self.types[i])
                  for i in range(len(instance))]
      return self.binrepr.pack(*values)

  def unpack(self, buffer):
    if self.clazz and self.binrepr:
      values = [Types.formatValue(v, self.types[i], False)
                  for i, v in enumerate(self.binrepr.unpack(buffer))]
      return self.clazz._make(values)

  def packSchema(self):
    if self.name and self.fields and self.types:
      return json.dumps((self.name, self.schema())).encode()

  @classmethod
  def unpackSchema(cls, buffer):
    args = json.loads(buffer.decode())
    if len(args) == 2:
      return cls(args[0], args[1])

if __name__ == "__main__":
    import doctest
    doctest.testmod()