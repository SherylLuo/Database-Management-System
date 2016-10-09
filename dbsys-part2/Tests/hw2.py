import Database
from Catalog.Schema import DBSchema

import sys
import unittest

import warnings

class Hw2PublicTests(unittest.TestCase):
  # Utilities
  def setUp(self):
    warnings.simplefilter("ignore", ResourceWarning)

    # Start with a fresh 'employee' table
    self.db = Database.Database()
    self.db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('dept_id', 'int')])

    # Populate employees with department 0 or 1
    self.numEmployees = 20
    empSchema = self.db.relationSchema('employee')
    
    # Create an index over employee age
    keySchema  = DBSchema('employeeAge',  [('age', 'int')])
    self.idxId = self.db.storageEngine().createIndex('employee', empSchema, keySchema, False)

    for tup in [empSchema.pack(empSchema.instantiate(i, 2*i+20, i % 2)) for i in range(self.numEmployees)]:
       self.db.insertTuple(empSchema.name, tup)

  def tearDown(self):
    self.db.removeRelation('employee')
    self.db.close()

  def getResults(self, query):
    return [query.schema().unpack(tup) for page in self.db.processQuery(query) for tup in page[1] ]
 
  # Operator test cases 
  def testScan(self):
    scan = self.db.query().fromTable('employee').finalize()
    results = self.getResults(scan)
    self.assertEqual(len(results), self.numEmployees)

  def testWhere(self):
    select = self.db.query().fromTable('employee').where('age < 30').finalize()
    results = self.getResults(select)
    self.assertEqual(len(results), 5)

  def testProject(self):
    project = self.db.query().fromTable('employee').select({'id': ('id', 'int')}).finalize()
    results = self.getResults(project)
    self.assertEqual([x.id for x in results], [x for x in range(self.numEmployees)])
  
  def testNLJoin(self):
    schema = self.db.relationSchema('employee')
    e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    join = self.db.query().fromTable('employee').join( \
             self.db.query().fromTable('employee'), \
             rhsSchema=e2schema, \
             method='nested-loops', \
             expr='id == id2' \
           ).finalize()
    results = self.getResults(join)
    self.assertEqual(len(results), self.numEmployees)

  def testUnion(self):
    union = self.db.query().fromTable('employee').union(self.db.query().fromTable('employee')).finalize()
    results = self.getResults(union)
    self.assertEqual(len(results), 2*self.numEmployees)
  
  def testBNLJoin1(self):
    schema = self.db.relationSchema('employee')
    e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    join = self.db.query().fromTable('employee').join( \
             self.db.query().fromTable('employee'), \
             rhsSchema=e2schema, \
             method='block-nested-loops', \
             expr='id == id2' \
           ).finalize()
    results = self.getResults(join)
    self.assertEqual(len(results), self.numEmployees)
  
  def testBNLJoin2(self):
    schema = self.db.relationSchema('employee')
    e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    join = self.db.query().fromTable('employee').join( \
             self.db.query().fromTable('employee'), \
             rhsSchema=e2schema, \
             method='block-nested-loops', \
             expr='id <= id2' \
           ).finalize()
    results = self.getResults(join)
    self.assertEqual(len(results), 0.5 * self.numEmployees * (self.numEmployees + 1))

  def testGroupBy(self):
    # SELECT id, min(age), max(age) FROM Employee GROUP BY (id % 2)
    aggMinMaxSchema = DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])
    keySchema  = DBSchema('employeeKey',  [('id', 'int')])
    groupBy = self.db.query().fromTable('employee').groupBy( \
                groupSchema=keySchema, \
                aggSchema=aggMinMaxSchema, \
                groupExpr=(lambda e: e.id % 2), \
                aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), \
                  (0, lambda acc, e: max(acc, e.age), lambda x: x)], \
                groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
              ).finalize()
    results = self.getResults(groupBy)
    self.assertEqual(len(results), 2)
    for result in results:
      if result.id == 0:
        self.assertEqual(result.minAge, 20)
        self.assertEqual(result.maxAge, 56)
      else:
        self.assertEqual(result.minAge, 22)
        self.assertEqual(result.maxAge, 58)

  def testHashJoin(self):
    schema = self.db.relationSchema('employee')
    e2schema   = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    keySchema  = DBSchema('employeeKey',  [('id', 'int')])
    keySchema2 = DBSchema('employeeKey2', [('id2', 'int')])
    hashJoin = self.db.query().fromTable('employee').join( \
                 self.db.query().fromTable('employee'), \
                 rhsSchema=e2schema, \
                 method='hash', \
                 lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
                 rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2, \
               ).finalize()
    results = self.getResults(hashJoin)
    self.assertEqual(len(results), self.numEmployees)

  def testSort(self):
    sort = self.db.query().fromTable('employee').order( \
             sortKeyFn=lambda x: x.age, 
             sortKeyDesc='age'
           ).finalize()
    results = self.getResults(sort)
    self.assertEqual(len(results), self.numEmployees)
    self.assertEqual([x.id for x in results], [(self.numEmployees - x - 1) for x in range(self.numEmployees)])

  def testIndexJoin(self):
    schema = self.db.relationSchema('employee')
    e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2', 'dept_id': 'dept_id2'})
    lhsKeySchema  = DBSchema('employeeAge',  [('age2', 'int')])
    join = self.db.query().fromTable('employee').join( \
             self.db.query().fromTable('employee'), \
             lhsSchema=e2schema, \
             lhsKeySchema = lhsKeySchema, \
             method='indexed', \
             indexId = self.idxId, \
             expr = 'True'
           ).finalize()
    results = self.getResults(join)
    self.assertEqual(len(results), self.numEmployees)
    
if __name__ == '__main__':
  unittest.main(argv=[sys.argv[0], '-v'])
