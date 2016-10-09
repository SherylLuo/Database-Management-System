import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)
    self.freeList         = list(range(0, self.poolSize, self.pageSize))
    self.freeListLen  = len(self.freeList)
    self.pageMap      = OrderedDict()

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return self.freeListLen

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return pageId in self.pageMap
  
  def getPage(self, pageId):
    if self.fileMgr:
      if self.hasPage(pageId):
        self.pageMap.move_to_end(pageId)
        return self.pageMap[pageId][1]
      else:
        if not self.freeSpace():
          self.evictPage()

        self.freeListLen -= 1
        offset = self.freeList.pop(0)
        pageBuffer = self.pool.getbuffer()[offset: offset + self.pageSize]
        page = self.fileMgr.readPage(pageId, pageBuffer)

        self.pageMap[pageId] = (offset, page)
        self.pageMap.move_to_end(pageId)
        return page

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if self.hasPage(pageId):
      self.freeList.append(self.pageMap[pageId][0])
      self.freeListLen += 1
      del self.pageMap[pageId]

  def flushPage(self, pageId):
    if self.hasPage(pageId):
      page = self.getPage(pageId)
      self.discardPage(pageId)

      if page.isDirty():
        self.fileMgr.writePage(page)

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    if self.pageMap:
      pageIdToEvict = self.pageMap.items()[0][0]
      self.flushPage(pageIdToEvict)

  # Flushes all dirty pages
  def clear(self):
    for (pageId, (offset, page)) in self.pageMap.items():
      if page.isDirty():
        self.flush(pageId)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
