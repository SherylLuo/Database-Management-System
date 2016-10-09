import itertools
from Query.Optimizer import Optimizer
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.TableScan import TableScan

class GreedyOptimizer(Optimizer):
  def pickJoinOrder(self, plan):
    self.combsTried = 0
    self.plansProcessed = 0

    self.rels = set(plan.relations())
    #toBeProcessed = set()

    self.tableScans = {}
    for r in self.rels:
      ts = TableScan(r, self.db.relationSchema(r))
      ts.prepare(self.db)
      self.tableScans[frozenset({r})] = ts


    self.joinMap = {}
    for (_, op) in plan.flatten():
      if isinstance(op, Join):
        relativeR = self.relativeRelations(self.rels, op)
        for r in [frozenset({r}) for r in relativeR]:
          if r in self.joinMap.keys():
            self.joinMap[r].append((relativeR, op))
          else:
            self.joinMap[r] = [(relativeR, op)]

    n = len(self.rels)
    currBestPlan = None
    formerBestPlan = None
    formerRels = None
    currRels = None


    for i in range(2, n+1):
      currBestCost = float('inf')
      if i == 2:
        for left in [frozenset({left}) for left in self.rels]:
          (newCost, newJoin, newRels) = self.processJoin(self.tableScans[left], left)

          if newCost < currBestCost:
            currRels = newRels
            currBestPlan = newJoin
            currBestCost = newCost
      else:
        (newCost, newJoin, newRels) = self.processJoin(formerBestPlan, formerRels)

        if newCost < currBestCost:
          currRels = newRels
          currBestPlan = newJoin
          currBestCost = newCost

      formerBestPlan = currBestPlan
      currBestPlan = None
      formerRels = currRels
      currRels = None

    newRoot = formerBestPlan
    return Plan(root=newRoot)


  def processJoin(self, leftPlan, leftRels):
    newCost = None
    newJoin = None
    newRels = None
    for right in [frozenset({right}) for right in self.rels - leftRels]:
      value = self.joinMap[right]
      self.combsTried += 1
      union = leftRels.union(right)

      if not value:
        continue
      else:
        for tuple in value:
          if not tuple[0].issubset(union):
            continue

          self.plansProcessed += 1
          newJoin = Join(leftPlan, self.tableScans[right], expr=tuple[1].joinExpr, method="block-nested-loops")
          newJoin.prepare(self.db)
          newCost = newJoin.cost(estimated=True)
          newRels = frozenset(leftRels.union(right))

    return (newCost, newJoin, newRels)
