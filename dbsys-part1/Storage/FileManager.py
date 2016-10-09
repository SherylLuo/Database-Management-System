import io, json, os, os.path, pickle

from Catalog.Schema      import DBSchema
from Catalog.Identifiers import FileId
from Storage.File        import StorageFile

class FileManager:
  """
  A file manager, maintaining the storage files for the database relations.

  The file manager is implemented as two dictionaries, one mapping the
  relation name to a file identifier, and the second mapping a file
  identifier to the storage file object.

  >>> import Storage.BufferPool
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = Storage.BufferPool.BufferPool()
  >>> fm = FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Test addition and removal of relations
  >>> fm.createRelation(schema.name, schema)
  >>> list(fm.relations())
  ['employee']

  >>> (fId, rFile) = fm.relationFile(schema.name)

  >>> fm.detachRelation(schema.name)
  >>> list(fm.relations())
  []

  >>> fm.addRelation(schema.name, fId, rFile)
  >>> list(fm.relations())
  ['employee']

  # Test FileManager construction on existing directory
  >>> fm = FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)
  >>> list(fm.relations())
  ['employee']
  """

  defaultDataDir     = "data/"
  defaultFileClass   = StorageFile

  checkpointEncoding = "latin1"
  checkpointFile     = "db.fm"

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      self.pageSize   = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
      self.bufferPool = kwargs.get("bufferPool", None)
      self.datadir    = kwargs.get("datadir", FileManager.defaultDataDir)

      if self.bufferPool is None:
        raise ValueError("No buffer pool found when initializing a file manager")

      checkpointFound = os.path.exists(os.path.join(self.datadir, FileManager.checkpointFile))
      restoring       = "restore" in kwargs

      if not os.path.exists(self.datadir):
        os.makedirs(self.datadir)

      if restoring or not checkpointFound:
        self.fileClass     = kwargs.get("fileClass", FileManager.defaultFileClass)
        self.fileCounter   = kwargs.get("fileCounter", 0)
        self.relationFiles = kwargs.get("relationFiles", {})
        self.fileMap       = kwargs.get("fileMap", {})
        
        if restoring:
          self.relationFiles = dict([(i[0], FileId(i[1])) for i in kwargs["restore"][0]])
          for i in kwargs["restore"][1]:
            fId   = FileId(i[0])
            fPath = i[1]
            print("Restoring: %s, %s" % (fId, FPath))
            self.fileMap[fId] = \
              self.fileClass(bufferPool=self.bufferPool, fileId=fId, filePath=fPath, mode="update")

      else:
        self.restore()

  def fromOther(self, other):
    self.bufferPool    = other.bufferPool
    self.datadir       = other.datadir
    self.fileClass     = other.fileClass
    self.fileCounter   = other.fileCounter
    self.relationFiles = other.relationFiles
    self.fileMap       = other.fileMap
  
  # Closes and flushes all storage files in the file manager.
  # This includes flushing all pages held in the buffer pool.
  def close(self):
    if self.bufferPool:
      self.bufferPool.clear()

    if self.fileMap:
      for storageFile in self.fileMap.values():
        storageFile.close()
    
    self.checkpoint()

  # Save the file manager internals to the data directory.
  def checkpoint(self):
    fmPath = os.path.join(self.datadir, FileManager.checkpointFile)
    with open(fmPath, 'w', encoding=FileManager.checkpointEncoding) as f:
      f.write(self.pack())

  # Load relations from an existing data directory.
  def restore(self):
    fmPath = os.path.join(self.datadir, FileManager.checkpointFile)
    with open(fmPath, 'r', encoding=FileManager.checkpointEncoding) as f:
      other = FileManager.unpack(self.bufferPool, f.read())
      self.fromOther(other)

  # Return the relation ids present in the file manager.
  def relations(self):
    return self.relationFiles.keys()

  def hasRelation(self, relId):
    return relId in self.relationFiles

  def createRelation(self, relId, schema):
    if relId not in self.relationFiles:
      fId = FileId(self.fileCounter)
      path = os.path.join(self.datadir, str(self.fileCounter)+'.rel')
      self.fileCounter += 1
      self.relationFiles[relId] = fId
      self.fileMap[fId] = \
        self.fileClass(bufferPool=self.bufferPool, \
                       pageSize=self.pageSize, fileId=fId, filePath=path, mode="create", schema=schema)

      self.checkpoint()

  def addRelation(self, relId, fileId, storageFile):
    if relId not in self.relationFiles and fileId not in self.fileMap:
      self.fileCounter          = max(self.fileCounter, fileId.fileIndex+1)
      self.relationFiles[relId] = fileId
      self.fileMap[fileId]      = storageFile
      self.checkpoint()

  def removeRelation(self, relId):
    fId   = self.relationFiles.pop(relId, None)
    rFile = self.fileMap.pop(fId, None) if fId else None
    if rFile:
      rFile.close()
      os.remove(rFile.filePath)
      self.checkpoint()

  # Removes a relation from the file manager without closing
  # and deleting the underlying storage file.
  def detachRelation(self, relId):
    fId   = self.relationFiles.pop(relId, None)
    rFile = self.fileMap.pop(fId, None) if fId else None
    if rFile:
      self.checkpoint()

  def relationFile(self, relId):
    fId = self.relationFiles.get(relId, None) if relId else None
    return (fId, self.fileMap.get(fId, None)) if fId else (None, None)


  # Page operations
  def readPage(self, pageId, pageBuffer):
    rFile = self.fileMap.get(pageId.fileId, None) if pageId else None
    if rFile:
      return rFile.readPage(pageId, pageBuffer)

  def writePage(self, page):
    rFile = self.fileMap.get(pageId.fileId, None) if page.pageId else None
    if rFile:
      return rFile.writePage(page)


  # Tuple operations

  # Returns a tuple id for the newly inserted data.
  def insertTuple(self, relId, tupleData):
    (_, rFile) = self.relationFile(relId)
    if rFile:
      return rFile.insertTuple(tupleData)

  def deleteTuple(self, tupleId):
    rFile = self.fileMap.get(tupleId.pageId.fileIndex, None)
    if rFile:
      rFile.deleteTuple(tupleId)

  def updateTuple(self, tupleId, tupleData):
    rFile = self.fileMap.get(tupleId.pageId.fileIndex, None)
    if rFile:
      rFile.updateTuple(tupleId, tupleData)


  # Tuple-based table scan
  def tuples(self, relId):
    (_, rFile) = self.relationFile(relId)
    if rFile:
      return rFile.tuples()

  # Page-based table scan
  def pages(self, relId):
    (_, rFile) = self.relationFile(relId)
    if rFile:
      return rFile.pages()


  # File manager serialization
  def pack(self):
    if self.relationFiles is not None and self.fileMap is not None:
      pfileClass     = pickle.dumps(self.fileClass).decode(encoding=FileManager.checkpointEncoding)
      prelationFiles = list(map(lambda entry: (entry[0], entry[1].fileIndex), self.relationFiles.items()))
      pfileMap       = list(map(lambda entry: (entry[0].fileIndex, entry[1].filePath), self.fileMap.items()))
      return json.dumps((self.datadir, pfileClass, self.fileCounter, prelationFiles, pfileMap))

  @classmethod
  def unpack(cls, bufferPool, strBuffer):
    args = json.loads(strBuffer)
    if len(args) == 5:
      unfileClass = pickle.loads(args[1].encode(encoding=FileManager.checkpointEncoding))
      return cls(bufferPool=bufferPool, datadir=args[0], fileClass=unfileClass, \
                 fileCounter=args[2], restore=(args[3], args[4]))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
