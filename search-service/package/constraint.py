from pattern import Pattern
from constraintType import ConstraintType

class Constraint(Pattern):
  def __init__(self,
               constrainedAttributeName:str,
               constraintType:ConstraintType,
               constraintValue):
    Pattern.__init__(self)
    self.constrainedAttributeName=constrainedAttributeName
    self.constraintType=constraintType
    self.constraintValue=constraintValue

  def getConstrainedAttributeName(self):
    return self.constrainedAttributeName

  def getConstraintType(self):
    return self.constraintType

  def getConstraintValue(self):
    return self.constraintValue
