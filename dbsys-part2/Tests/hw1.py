from Storage.Page import Page
from Storage.SlottedPage import SlottedPage
from Storage.File import StorageFile
from Storage.FileManager import FileManager
from Storage.BufferPool import BufferPool
from Catalog.Identifiers import FileId, PageId, TupleId
from Catalog.Schema import DBSchema

import sys
import unittest

# Change this to 'pageClass = SlottedPage' to test the SlottedPage class.
pageClass = Page

class Hw1PublicTests(unittest.TestCase):
  ###########################################################
  # Page Class Tests
  ###########################################################
  # Utils:
  def makeSchema(self):
    return DBSchema('employee', [('id', 'int'), ('age', 'int')])

  def makeEmployee(self, n):
    schema = self.makeSchema()
    return schema.instantiate(n, 25 + n)

  def makeEmptyPage(self):
    schema = self.makeSchema()
    pId = PageId(FileId(1), 100)
    return pageClass(pageId=pId, buffer=bytes(4096), schema=schema)

  # Tests
  def testPageInsertTuple(self):
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    self.assertIsNotNone(tId)

  def testPagePutGetTuple(self):
    # Insert a Tuple
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    self.assertIsNotNone(tId, 'Insert Tuple Returned None!')

    # Get It Back
    e2 = p.getTuple(tId)
    self.assertIsNotNone(e2, 'Get Tuple Returned None!')
    self.assertEqual(e1, schema.unpack(e2), 'Get Tuple Returned an Invalid Tuple!')

    # Update it in place
    e3 = self.makeEmployee(2)
    p.putTuple(tId, schema.pack(e3))

    # Check that the update took effect
    e4 = p.getTuple(tId)
    self.assertEqual(e3, schema.unpack(e4))

  def testPageDeleteTuple(self):
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    p.deleteTuple(tId)
    self.assertIsNone(p.getTuple(tId), 'Deleted tuple is still present in the page!')

  # Stress Tests:
  def testPageInsertMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    # Insert 1000 tuples, making sure no errors occur
    for i in range(1000):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))

  def testPageGetMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    tids = []
    # Insert 500 tuples, then 'get' them back.
    for i in range(500):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))
      tids.append(tId)
    for tId in tids:
      if tId is not None:
        e2 = p.getTuple(tId)
        self.assertIsNotNone(e2, 'Get Tuple Returned None!')

  def testPageDeleteMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    tids = []
    # Insert 500 tuples, then 'delete' them.
    for i in range(500):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))
      tids.append(tId)
    for tId in tids:
      if tId is not None:
        p.deleteTuple(tId)

  ###########################################################
  # File Class Tests
  ###########################################################
  # Utils:
  def makeDB(self):
    schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
    bp = BufferPool()
    fm = FileManager(bufferPool=bp)
    bp.setFileManager(fm)
    return (bp, fm, schema)

  def makePage(self, schema, fId, f,i):
    pId = PageId(fId, i)
    p = SlottedPage(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
    for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(1000)]:
      p.insertTuple(tup)
    return (pId, p)

  # Tests:
  def testFileReadWritePage(self):
    # Initialize database internals, and a new page
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    (pId, p) = self.makePage(schema, fId, f, 0)

    # Write it to the file, check the file size
    f.writePage(p)
    f.flush()
    self.assertEqual(f.numPages(), 1)
    self.assertEqual(f.size(), f.headerSize() + f.pageSize())

    # Read it back in
    pageBuffer = bytearray(f.pageSize())
    pIn1 = f.readPage(pId, pageBuffer)
    self.assertEqual(pIn1.pageId, pId)
    self.assertEqual(pIn1.header.numTuples(), p.header.numTuples())

    # The tuples in the freshly-read page should be equal
    # to those in the original.
    for (tup1, tup2) in zip(pIn1, p):
      self.assertEqual(tup1, tup2)

  def testFileAllocatePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Keep allocating pages, making sure the number of pages in
    # the file is increasing.
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.numPages(), i+1)

  def testFileAvailablePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)

    # Since we aren't adding any data, pageIndex 1 should be the first available.
    # Even as we allocate more pages
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.availablePage().pageIndex, 1)

    # Now we fill some pages to check that the available page has changed.
    for i in range(1000):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
    self.assertNotEqual(f.availablePage().pageIndex, 2)

  def testFileInsertTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert 1000 tuples, checking the files numTuples()
    for i in range(1000):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
    self.assertEqual(f.numTuples(), 1000)

  def testFileDeleteTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    tids = []
    # Insert 1000 tuples, then delete them. File should have 0 tuples.
    for i in range(1000):
      tids.append(f.insertTuple(schema.pack(self.makeEmployee(i))))
    for tid in tids:
      f.deleteTuple(tid)
    self.assertEqual(f.numTuples(), 0)

  def testFileUpdateTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert and update a single tuple, then check the effect took hold.
    tid = f.insertTuple(schema.pack(self.makeEmployee(1)))
    f.updateTuple(tid, schema.pack(self.makeEmployee(10)))
    for tup in f.tuples():
      self.assertEqual(schema.unpack(tup).id, 10)

  ###########################################################
  # BufferPool Class Tests
  ###########################################################
  def testBufferPoolHasPage(self):
    (bufp, filem, schema) = self.makeDB()
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. The single page
    # should be cached in the buffer pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    self.assertEqual(bufp.hasPage(f.availablePage()), True)

  def testBufferPoolGetPage(self):
    (bufp, filem, schema) = self.makeDB()
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. We should
    # be able to get the single page from the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    self.assertIsNotNone(bufp.getPage(f.availablePage()))

  def testBufferPoolDiscardPage(self):
    (bufp, filem, schema) = self.makeDB()
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. Then, discard
    # the single page. The page should no longer be in the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    pId = f.availablePage()
    bufp.discardPage(pId)
    self.assertEqual(bufp.hasPage(pId), False)

  def testBufferPoolEvictPage(self):
    (bufp, filem, schema) = self.makeDB()
    (fId, f) = filem.relationFile(schema.name)
    (pId, p) = self.makePage(schema, fId, f, 0)

    # Insert a single tuple into a file. Then evict a page.
    # The single page should no longer be in the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    pId = f.availablePage()
    bufp.evictPage()
    self.assertEqual(bufp.hasPage(pId), False)

if __name__ == '__main__':
  unittest.main(argv=[sys.argv[0], '-v'])
