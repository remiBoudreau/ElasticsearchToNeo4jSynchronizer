from constraint import Constraint
from nodeRelationshipType import NodeRelationshipType
from nodeRelationshipMultiplicityConstraint import NodeRelationshipMultiplicityConstraint
from constraintType import ConstraintType


class RelationshipConstraint(Constraint):
  def __init__(self,
               affectedRelationshipId:str,
               relationshipType: NodeRelationshipType,
               constrainedAttributeName:str,
               constraintType:ConstraintType,
               constraintValue):
    Constraint.__init__(self, constrainedAttributeName,constraintType, constraintValue)
    self.relationshipType=relationshipType
    self.affectedRelationshipId=affectedRelationshipId
    # Validate that the constraint type is of the valid name and type for this node type
    nr=NodeRelationship(relationshipType,
                        NodeRelationshipMultiplicityConstraint.MANY_TO_MANY,
                        sourceNodeId=" ",
                        targetNodeId=" ",
                        relationshipPropertyValue=123,
                        predictionConfidence=.123456)
    if not constrainedAttributeName in nr.getAttributesList().keys():
      raise TypeError(f"Attribute {constrainedAttributeName} does not exist for the relationshipType {relationshipType}.")
    # TODO: See rangeIncludes in the schema, and the definitions here: https://partsol.atlassian.net/wiki/spaces/CHKM/pages/6226060/Query+Builder
    pass

  def getRelationshipType(self):
    return self.relationshipType

  def getAffectedRelationshipId(self):
    return self.affectedRelationshipId

  def getAttributesList(self):
    return {"relationshipType":self.getRelationshipType(),
            "affectedRelationshipId":self.getAffectedRelationshipId(),
            "constrainedAttributeName":self.getConstrainedAttributeName(),
            "constraintType":self.getConstraintType(),
            "constraintValue":self.constraintValue()
            }
