import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader:
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """
  prefix = "cHH"
  prefixFmt = struct.Struct(prefix)
  size = prefixFmt.size

  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')
    if buffer:
      self.tupleSize       = kwargs.get("tupleSize", None)
      self.pageCapacity    = kwargs.get("pageCapacity", len(buffer))
      self.numSlots 	   = kwargs.get("numSlots", self.maxTuples())
      self.slot 	   = [0] * self.slotLenInBytes()
      self.nextSlot 	   = 0
      self.binrerp	   = Struct(SlottedPageHeader.prefix + "HH" + str(self.slotLenInBytes()) + "B")
      self.rerpSize	   = self.binrerp.size
      #self.slotBuffer	   = buffer[SlottedPageHeader.size: self.rerpSize]
      buffer[0: self.rerpSize] = self.pack()
    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def maxTuples(self):
    return math.floor((8 * self.pageCapacity - 8 * SlottedPageHeader.size) / (8 * self.tupleSize + 1))

  def slotLenInBytes(self):
    return math.ceil(self.maxTuples() / 8)

  def __eq__(self, other):
    return (    self.flags == other.flags
            and self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity
            and self.numSlots == other.numSlots
	    and self.slot == other.slot)

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.numSlots, self.slot[:]))

  def headerSize(self):
    return self.rerpSize

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    self.num = 0
    for i in range(0, self.numSlots):
      if self.getSlot(i):
        self.num += 1
    return self.num

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.usedSpace() - self.headerSize()

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    return self.numTuples() * self.tupleSize


  # Slot operations.
  def offsetOfSlot(self, slotIndex):
    if self.hasSlot(slotIndex):
      return slotIndex * self.tupleSize + self.headerSize()

  def hasSlot(self, slotIndex):
    return slotIndex < self.numSlots and slotIndex >= 0

  def getSlot(self, slotIndex):
    #return bool(ord((struct.unpack('c',self.slot[int(slotIndex / 8)])[0])) & (0b1 << (slotIndex % 8)))
    BITMASK = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    if self.hasSlot(slotIndex):
      return bool(self.slot[math.floor(slotIndex / 8)] & BITMASK[slotIndex % 8])

  def setSlot(self, slotIndex, slot):
    BITMASK = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    if self.hasSlot(slotIndex):
      if slot:
        #self.temp = struct.unpack('B',self.slot[int(slotIndex / 8)])[0]
        self.slot[math.floor(slotIndex / 8)] |= BITMASK[slotIndex % 8]
        #self.slot[int(slotIndex / 8)] = struct.pack('B', self.temp)
      else:
        #self.temp = struct.unpack('B',self.slot[int(slotIndex / 8)])[0]
        self.slot[math.floor(slotIndex / 8)] &= ~BITMASK[slotIndex % 8] 
        #self.slot[int(slotIndex / 8)] = struct.pack('B', chr(self.temp))

  def resetSlot(self, slotIndex):
    if self.hasSlot(slotIndex):
      return self.setSlot(slotIndex, False)

  def freeSlots(self):
    self.freeSlot = []
    for i in range(0, self.numSlots):
      if not self.getSlot(i):
        self.freeSlot.append(i)
    return self.freeSlot

  def usedSlots(self):
    self.usedSlot = []
    for i in range(0, self.numSlots):
      if self.getSlot(i):
        self.usedSlot.append(i)
    return self.usedSlot

  # Tuple allocation operations.
  
  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return self.numTuples() < self.numSlots

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if not self.hasFreeTuple():
      return None

    i = 1
    self.current = self.numTuples()

    def findSpace(self, i):
      while self.getSlot(i % self.numSlots - 1):
        i *= 2
      self.nextSlot = (i % self.numSlots) - 1
      return self.nextSlot

    if self.current < self.numSlots and not self.getSlot(self.current):
      self.setSlot(self.current, True)
      self.nextSlot = self.current
      return self.current
    return self.findSpace(i)

  def nextTupleRange(self):
    if self.nextFreeTuple():
      return {'tupleIndex': self.nextSlot, 'start': self.nextSlot * self.tupleSize + self.headerSize(),
		    'end': (self.nextSlot + 1) * self.tupleSize + self.headerSize()}

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    return self.binrerp.pack(self.flags, self.tupleSize, self.pageCapacity, 
		    self.numSlots, self.nextSlot, *self.slot[:])

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def binrepr(cls, buffer):
    reStruct     = Struct("H")
    numSlots     = reStruct.unpack_from(buffer, offset = SlottedPageHeader.size)[0]
    slotLenInBytes = math.ceil(numSlots / 8)
    if numSlots > 0:
      return Struct(SlottedPageHeader.prefix + "HH" + str(slotLenInBytes) + "B")
    else:
      raise ValueError("Invalid number of slots in slotted page header")

  @classmethod
  def unpack(cls, buffer):
    fmtrerp = cls.binrepr(buffer) 
    value = fmtrerp.unpack_from(buffer)
    return cls(buffer = buffer, flags = value[0], tupleSize = value[1], 
		    pageCapacity = value[2], numSlot = value[3], nextSlot = value[4],
		    slot = list(value[5:]))
    #return cls(buffer = buffer, flags, tupleSize, pageCapacity, *tupleFlag = value)



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.
  def __init__(self, **kwargs):
    '''
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")
      
      raise NotImplementedError

    else:
      raise ValueError("No backing buffer provided to page constructor.")
    '''
    Page.__init__(self, **kwargs)


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    self.iterTupleIdx = 0
    return self

  def __next__(self):
    while self.iterTupleIdx < self.header.numSlots and not self.header.getSlot(self.iterTupleIdx):
      self.iterTupleIdx += 1
    if self.iterTupleIdx < self.header.numSlots and self.header.getSlot(self.iterTupleIdx):
      self.iterTupleIdx += 1
      return self.getTuple(TupleId(self.pageId, self.iterTupleIdx - 1))
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    if self.header.hasSlot(tupleId.tupleIndex) and self.header.getSlot(tupleId.tupleIndex):
      return self.getbuffer()[tupleId.tupleIndex * self.header.tupleSize + self.header.headerSize(): (
	    tupleId.tupleIndex + 1) * self.header.tupleSize + self.header.headerSize()]

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    return Page.putTuple(self, tupleId, tupleData)

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    self.index = self.header.nextFreeTuple()
    if self.index:
      self.getbuffer()[self.index * self.header.tupleSize + self.header.headerSize(): (self.index + 1)
		      * self.header.tupleSize + self.header.headerSize()] = tupleData
      return TupleId(self.pageId, self.index)
      self.header.setDirty(True)

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    return Page.clearTuple(self, tupleId)

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    if self.header.hasSlot(tupleId.tupleIndex):
      self.header.setSlot(tupleId.tupleIndex, False)
      self.header.setDirty(True)

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    return Page.pack(self)

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    header = cls.headerClass.unpack(BytesIO(buffer).getbuffer())
    return cls(pageId = pageId, buffer = buffer, header = header)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
