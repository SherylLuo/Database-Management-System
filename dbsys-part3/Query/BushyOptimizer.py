import itertools
from Query.Optimizer import Optimizer
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.TableScan import TableScan


class BushyOptimizer(Optimizer):
  def pickJoinOrder(self, plan):
    self.combsTried = 0
    self.plansProcessed = 0

    rels = set(plan.relations())
    optPlans = {} #Map a set of relations to the optimized plan

    for r in rels:
      set_r = frozenset({r})
      newScan = TableScan(r, self.db.relationSchema(r))
      newScan.prepare(self.db)
      optPlans[set_r] = newScan


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
        for j in range(1, int(i / 2) + 1):
          for right in [frozenset(right) for right in self.kRelsComb(j, union)]:
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