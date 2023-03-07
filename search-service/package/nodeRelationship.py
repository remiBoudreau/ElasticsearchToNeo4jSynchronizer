from pattern import Pattern
from nodeRelationshipType import NodeRelationshipType
from nodeRelationshipMultiplicityConstraint import NodeRelationshipMultiplicityConstraint

class NodeRelationship(Pattern):
  def __init__(self,
               nodeRelationshipType:NodeRelationshipType,
               multiplicity:NodeRelationshipMultiplicityConstraint,
               sourceNodeId:str,    # in a taxonomy this is another taxonomy node, not a "real" node in the Knowledge Graph
               targetNodeId:str,    # in a taxonomy this is another taxonomy node, not a "real" node in the Knowledge Graph
               relationshipPropertyValue,       # a number (or string?) that annotates the relationship
               predictionConfidence #must be able to be null in a taxonomy
               ):
    Pattern.__init__(self)
    self.nodeRelationshipType = nodeRelationshipType
    self.multiplicity=multiplicity
    self.sourceNodeId=sourceNodeId
    self.targetNodeId=targetNodeId
    self.relationshipPropertyValue=relationshipPropertyValue
    self.predictionConfidence=predictionConfidence

  def getNodeRelationshipType(self):
    return self.nodeRelationshipType

  def getMultiplicity(self):
    return self.multiplicity

  def getSourceNodeId(self):
    return self.sourceNodeId

  def getTargetNodeId(self):
    return self.targetNodeId

  def getRelationshipPropertyValue(self):
    return self.relationshipPropertyValue

  def getPredictionConfidence(self):
    return self.predictionConfidence

  def getAttributesList(self):
    return {"nodeRelationshipType"     :self.getNodeRelationshipType(),
            "multiplicity"             :self.getMultiplicity(),
            "sourceNodeId"             :self.getSourceNodeId(),
            "targetNodeId"             :self.getTargetNodeId(),
            "relationshipPropertyValue":self.getRelationshipPropertyValue(),
            "predictionConfidence"     :self.getPredictionConfidence(),
            }
