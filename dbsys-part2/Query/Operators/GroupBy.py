from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    '''if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)

    relIds = []
    try:
      for (pageId, page) in self.inputIterator:
        for tuple in page:
          key = self.groupExpr(self.subSchema.unpack(tuple)),
          partition = self.groupHashFn(key)
          relId = "newGB_" + str(partition)

          if not self.storage.hasRelation(relId):
            self.storage.createRelation(relId, self.subSchema)
            relIds.append(relId)

          partFile = self.storage.fileMgr.relationFile(relId)[1]
          if partFile:
            partFile.insertTuple(tuple)
          #self.storage.insertTuple(relId, tuple)

    except StopIteration:
      pass

    for relId in relIds:
      partFile = self.storage.fileMgr.relationFile(relId)[1]
      groupDict = dict()
      for tuple in partFile.pages():
        currInput = self.subSchema.unpack(tuple)
        key = self.subSchema.projectBinary(tuple, self.groupSchema)

        if key not in groupDict:
          currAgg = self.aggSchema.instantiate(*[e[0] for e in self.aggExprs])
        else:
          currAgg = self.aggSchema.unpack(groupDict[key])

        groupDict[key] = self.aggSchema.pack(self.aggSchema.instantiate(\
                            *[self.aggExprs[i][1](currAgg[i], currInput)\
                              for i in range(len(self.aggExprs))]))

      for k, v in groupDict.items():
        currAgg = self.aggSchema.unpack(v)
        finalVal = self.aggSchema.pack(self.aggSchema.instantiate(\
                              *[self.aggExprs[i][2](currAgg[i]) for i in range(len(self.aggExprs))]))

        output = self.loadSchema(self.groupSchema, k)
        output.update(self.loadSchema(self.aggSchema, finalVal))
        outputTuple = self.outputSchema.instantiate(*[output[f] for f in self.outputSchema.fields])
        self.emitOutputTuple(self.outputSchema.pack(outputTuple))

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())'''


    self.partitionFiles = {}
    for (pageId, page) in self.inputIterator:
      for tup in page:
        groupVal = self.groupExpr(self.subSchema.unpack(tup)),
        groupId = self.groupHashFn(groupVal)
        partitionRelId = "GBpartition_" + str(groupId)

        if not self.storage.hasRelation(partitionRelId):
          self.storage.createRelation(partitionRelId, self.subSchema)
          self.partitionFiles[groupId] = partitionRelId

        partFile = self.storage.fileMgr.relationFile(partitionRelId)[1]
        if partFile:
          partFile.insertTuple(tup)

    for partitionRelId in self.partitionFiles.values():
      partFile = self.storage.fileMgr.relationFile(partitionRelId)[1]

      groupDict = {}
      for (pageId, page) in partFile.pages():
        for tup in page:
          currInput = self.subSchema.unpack(tup)
          key = self.groupExpr(currInput),

          if key not in groupDict:
            groupDict[key] = self.aggSchema.instantiate(*[e[0] for e in self.aggExprs])

          groupDict[key] = self.aggSchema.instantiate(\
                            *[self.aggExprs[i][1](groupDict[key][i], currInput)\
                              for i in range(len(self.aggExprs))])

      for (groupVal, aggVals) in groupDict.items():
        finalVal = self.aggSchema.instantiate(\
                              *[self.aggExprs[i][2](aggVals[i]) for i in range(len(self.aggExprs))])
        outputTuple = self.outputSchema.instantiate(*(list(groupVal) + list(finalVal)))
        self.emitOutputTuple(self.outputSchema.pack(outputTuple))

      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    self.removePartitionFiles()

    return self.storage.pages(self.relationId())

  def removePartitionFiles(self):
    for partitionRelId in self.partitionFiles.values():
      self.storage.removeRelation(partitionRelId)
    self.partitionFiles = {}


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
