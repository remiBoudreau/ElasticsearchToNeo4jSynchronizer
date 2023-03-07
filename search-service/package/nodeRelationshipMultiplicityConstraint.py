from enum import Enum, auto

class NodeRelationshipMultiplicityConstraint(Enum):
  ONE_TO_ONE_OR_MORE = auto()
  ONE_TO_ZERO_OR_MORE = auto()
  MANY_TO_MANY = auto() # need an example
  ONE_TO_EXACTLY_ONE = auto()
