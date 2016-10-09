import itertools

from collections import deque
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Query.Operators.TableScan import TableScan
from Query.Operators.Union import Union
from Query.Operators.GroupBy import GroupBy
from Utils.ExpressionInfo import ExpressionInfo

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass

  # Join Order Optimization
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  # Pushdown Optimization
  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    self.statsCache[plan] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    return self.statsCache[plan]

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    midPlan = self.selectPushDown(plan)
    return self.projectPushDown(midPlan)

  def selectPushDown(self, plan):
    root = plan.root
    selectResult = []

    #New a stack and put info about op into it in the form of
    # (current op, parent op, accumulateSelect)
    queue = deque([(root, None, None)])

    while queue:
      (curr, parent, accuSelect) = queue.popleft()
      children = curr.inputs()

      if children:
        #When dealing with Select, collect select expressions into accumulate select
        if isinstance(curr, Select):
          if not accuSelect:
            accuSelect = []
          for decomp in ExpressionInfo(curr.selectExpr).decomposeCNF():
            accuSelect.append(decomp)

          queue.extendleft([(children[0], curr, accuSelect)])

        #Do not pushdown project at this point, so put it into result.
        #Accumulate select can always pass project
        elif isinstance(curr, Project):
          selectResult.append((curr, parent))
          queue.extendleft([(children[0], curr, accuSelect)])

        #When encountering a join, seperate the accumulate select expressions into three parts,
        #one part goes to left, one goes to right, and the remaining place above the join operator
        elif isinstance(curr, Join):
          leftSelect = []
          rightSelect = []
          newSelect = None
          leftFields = curr.lhsSchema.fields
          rightFields = curr.rhsSchema.fields
          put = []
          if accuSelect:
            for a in accuSelect:
              f = ExpressionInfo(a).getAttributes()
              flag = False
              if set(f).issubset(set(leftFields)):
                leftSelect.append(a)
                flag = True
              if set(f).issubset(set(rightFields)):
                rightSelect.append(a)
                flag = True
              if not flag:
                put.append(a)
            if put:
              newSelect = self.placeSelect(put, curr, parent, selectResult)

          if newSelect:
            selectResult.append((curr, newSelect))
          else:
            selectResult.append((curr, parent))

          queue.extendleft([(curr.lhsPlan, curr, leftSelect)])
          queue.extendleft([(curr.rhsPlan, curr, rightSelect)])

        #When encounter groupby, place all the accumulate select
        elif isinstance(curr, GroupBy):
          newSelect = self.placeSelect(accuSelect, curr, parent, selectResult)

          if newSelect:
            selectResult.append((curr, newSelect))
          else:
            selectResult.append((curr, parent))

          queue.extendleft([(children[0], curr, None)])

        #Deal with union similarly to join
        else:
          leftSelect = []
          rightSelect = []
          newSelect = None
          attrs = curr.unionSchema.fields
          put = []

          if accuSelect:
            for a in accuSelect:
              f = ExpressionInfo(a).getAttributes()
              if set(f).issubset(set(attrs)):
                leftSelect.append(a)
                rightSelect.append(a)
              else:
                put.append(a)

            newSelect = self.placeSelect(accuSelect, curr, parent, selectResult)

          if newSelect:
            selectResult.append((curr, newSelect))
          else:
            selectResult.append((curr, parent))

          queue.extendleft([(curr.lhsPlan, curr, leftSelect)])
          queue.extendleft([(curr.rhsPlan, curr, rightSelect)])

      #Deal with tablescan, place all the accumulate select
      else:
        newSelect = self.placeSelect(accuSelect, curr, parent, selectResult)

        if newSelect:
          selectResult.append((curr, newSelect))
        else:
          selectResult.append((curr, parent))

    newRoot = selectResult[0][0]
    return Plan(root=newRoot)


  #Place accumulate select as the parent of current operation
  def placeSelect(self, accuSelect, curr, parent, result):
    newSelect = None
    if accuSelect:
      expr = ""
      for a in accuSelect:
        if len(expr) > 0:
          expr += "and"
        expr += a
      newSelect = Select(curr, expr)
      result.append((newSelect, parent))
    return newSelect


  def projectPushDown(self, plan):
    root = plan.root
    result = []

    #Keep info in the form (current op, parent, accumulate Porject)
    queue = deque([(root, None, None)])

    while queue:
      (curr, parent, accuProject) = queue.popleft()
      children = curr.inputs()

      if children:
        #Add current project into accumulate project
        if isinstance(curr, Project):
          if not accuProject:
            accuProject = curr.projectExprs
          else:
            accuProject.update({curr.projectExprs})

          queue.extendleft([(children[0], curr, accuProject)])

        elif isinstance(curr, Select):
          newProject = None
          if accuProject:
            selectAttrs = ExpressionInfo(curr.selectExpr).getAttributes()
            projectAttrs = self.getProjectAttrs(accuProject)
            newProject = Project(curr, accuProject)
            if set(selectAttrs).issubset(set(projectAttrs)):
              result.append((curr, parent))
              queue.extendleft([(children[0], curr, accuProject)])
              '''
              #If considering the order of select and project:
              #Project can go through select
              #but if the selectivity of select is smaller, we do not let project pass
              curr.useSampling(sampled=True, sampleFactor=10.0)
              newProject.useSampling(sampled=True, sampleFactor=10.0)
              if curr.selectivity(estimated=True) < newProject.selectivity(estimated=True):
                result.append((newProject, parent))
                result.append((curr, newProject))
                queue.extendleft([(children[0], curr, None)])
              else:
                result.append((curr, parent))
                queue.extendleft([(children[0], curr, accuProject)])
              '''
            #If select operation has attributes that don't belongs to project
            #project has to stop here
            else:
              result.append((newProject, parent))
              result.append((curr, newProject))
              queue.extendleft([(children[0], curr, None)])

          else:
            result.append((curr, parent))
            queue.extendleft([(children[0], curr, accuProject)])

        elif isinstance(curr, Join):
          #If we don't decompose project
          if accuProject:
            newProject = Project(curr, accuProject)
            result.append((newProject, parent))
            result.append((curr, newProject))
          else:
            result.append((curr, parent))
          queue.extendleft([(curr.lhsPlan, curr, None)])
          queue.extendleft([(curr.rhsPlan, curr, None)])
          '''
          #This part can be used to decompose project operation
          leftProject = {}
          rightProject = {}
          newProject = None
          leftFields = curr.lhsSchema.fields
          rightFields = curr.rhsSchema.fields
          put = {}

          if accuProject:
            projectAttrs = self.getProjectAttrs(accuProject)
            joinAttrs = ExpressionInfo(curr.joinExpr).getAttributes()
            if set(joinAttrs).issubset(set(projectAttrs)):
              for (k,v) in accuProject.items():
                flag = False
                f = ExpressionInfo(k).getAttributes()
                if set(f).issubset(set(leftFields)):
                  leftProject.update({k: v})
                  flag = True
                if set(f).issubset(set(rightFields)):
                  rightProject.update({k: v})
                  flag = True
                if not flag:
                  put.update({k: v})

              if put:
                newProject = Project(curr, put)
                result.append((newProject, parent))

            else:
              newProject = Project(curr, accuProject)
              result.append((newProject, parent))

          if newProject:
            result.append((curr, newProject))
          else:
            result.append((curr, parent))

          queue.extendleft([(curr.lhsPlan, curr, leftProject)])
          queue.extendleft([(curr.rhsPlan, curr, rightProject)])
          '''

        elif isinstance(curr, GroupBy):
          newProject = None

          if accuProject:
            newProject = Project(curr, accuProject)
            result.append((newProject, parent))


          if newProject:
            result.append((curr, newProject))
          else:
            result.append((curr, parent))

          queue.extendleft([(children[0], curr, None)])

        else:
          #If we don't decompose project
          if accuProject:
            newProject = Project(curr, accuProject)
            result.append((newProject, parent))
            result.append((curr, newProject))
          else:
            result.append((curr, parent))
          queue.extendleft([(curr.lhsPlan, curr, None)])
          queue.extendleft([(curr.rhsPlan, curr, None)])
          '''
          #This part can be used to decompose project
          leftProject = {}
          rightProject = {}
          newProject = None
          attrs = curr.unionSchema.fields
          put = {}

          if accuProject:
            projectAttrs = self.getProjectAttrs(accuProject)
            if set(attrs).issubset(set(projectAttrs)):
              leftProject = accuProject
              rightProject = accuProject
            else:
              newProject = Project(curr, accuProject)
              result.append((newProject, parent))

          if newProject:
            result.append((curr, newProject))
          else:
            result.append((curr, parent))

          queue.extendleft([(curr.lhsPlan, curr, leftProject)])
          queue.extendleft([(curr.rhsPlan, curr, rightProject)])
          '''

      else:
        newProject = None
        if accuProject:
          newProject = Project(curr, accuProject)
        if newProject:
          result.append((newProject, parent))
          result.append((curr, newProject))
        else:
          result.append((curr, parent))

    newRoot = result[0][0]
    return Plan(root=newRoot)


  #Get all the attributes in a project expression
  def getProjectAttrs(self, projectExpr):
    attrs = []
    for (k, v) in projectExpr.items():
      attrs.append(k)
    return attrs


  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    rels = set(plan.relations())
    optPlans = {} #Map a set of relations to the optimized plan
    #toBeProcessed = [] #Set of relations pending processing

    self.combsTried = 0
    self.plansProcessed = 0

    for r in rels:
      set_r = frozenset({r})
      #toBeProcessed.append(set_r)
      newScan = TableScan(r, self.db.relationSchema(r))
      newScan.prepare(self.db)
      optPlans[set_r] = newScan

    #For each join operator, fetch its relative relations
    #Map a set of relations to (relative relations, operator)
    joinMap = {}
    for (_, op) in plan.flatten():
      if isinstance(op, Join):
        relativeR = self.relativeRelations(rels, op)
        for r in [frozenset({r}) for r in relativeR]:
          if r in joinMap.keys():
            joinMap[r].append((relativeR, op))
          else:
            joinMap[r] = [(relativeR, op)]

    n = len(rels)
    for i in range(2, n + 1):
      for union in [frozenset(union) for union in self.kRelsComb(i, rels)]:
        for right in [frozenset(right) for right in self.kRelsComb(1, union)]:
          left = frozenset(union - right)
          for t in left:
            self.combsTried += 1
            value = joinMap[frozenset({t})]

            if not value:
              continue
            else:
              for tuple in value:
                if not (set(tuple[0]).issubset(union) and left in optPlans and right in optPlans):
                  continue

                self.plansProcessed += 1
                newJoin = Join(optPlans[left], optPlans[right], expr=tuple[1].joinExpr, method="block-nested-loops")
                newJoin.prepare(self.db)

                if not union in optPlans:
                  optPlans[union] = newJoin
                  self.addPlanCost(newJoin, newJoin.cost(estimated=True))
                else:
                  formerCost = self.getPlanCost(optPlans[union])
                  if newJoin.cost(estimated=True) < formerCost:
                    optPlans[union] = newJoin
                    self.addPlanCost(newJoin, newJoin.cost(estimated=True))

    newRoot = optPlans[frozenset(rels)]
    return Plan(root=newRoot)

    '''
    while len(toBeProcessed) > 0:
      curr = toBeProcessed.pop(0)
      if len(curr) == len(rels):
        newRoot = optPlans[curr]
        return Plan(root=newRoot) #Finished

      for t in [frozenset({t}) for t in rels - curr]:
        value = joinMap[t]
        union = curr.union(t)
        self.combsTried += 1

        if not value:
          continue
        else:
          for tuple in value:
            if not (set(tuple[0]).issubset(union) and curr in optPlans and t in optPlans):
              continue

            newJoin = Join(optPlans[curr], optPlans[t], expr=tuple[1].joinExpr, method="block-nested-loops")
            newJoin.prepare(self.db)
            self.plansProcessed += 1

            if not union in optPlans:
              optPlans[union] = newJoin
              self.addPlanCost(newJoin, newJoin.cost(estimated=True))
              toBeProcessed.append(union)
            else:
              formerCost = self.getPlanCost(optPlans[union])
              if newJoin.cost(estimated=True) < formerCost:
                optPlans[union] = newJoin
                self.addPlanCost(newJoin, newJoin.cost(estimated=True))
      '''


  def relativeRelations(self, relIds, op):
    fields = ExpressionInfo(op.joinExpr).getAttributes()
    relativeR = set()
    for f in fields:
      for r in relIds:
        if f in self.db.relationSchema(r).fields:
          relativeR.add(r)
          break
    return frozenset(relativeR)


  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

  #Produce all the possible combinations of k relations selecting from n relations
  def kRelsComb(self, k, resource):
    r = list(resource)
    n = len(r)
    for comb in self.combination(k, n):
      yield set(r[e] for e in comb)


  #Helper function, in order to produce all the possible combinations
  #of k integers selecting from [0, n)
  def combination(self, k, n):
    if n < 0:
      raise ValueError("n cannot be negative!")
    elif k < 0:
      raise ValueError("k cannot be negative!")
    else:
      if k == 0 or k > n:
        yield set()
      elif k == n:
        yield set(range(n))
      else:
        #Recursion function: c(k, n) = c(k-1, n-1) + c(k, n-1)
        for e in self.combination(k - 1, n - 1):
          e.add(n - 1)
          yield e
        for e in self.combination(k, n - 1):
          yield e

if __name__ == "__main__":
  import doctest
  doctest.testmod()
