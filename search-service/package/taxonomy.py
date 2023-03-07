from pattern import Pattern
from node import Node
from nodeRelationship import NodeRelationship
from nodeConstraint import NodeConstraint
from relationshipConstraint import RelationshipConstraint

# A Taxonomy is a graph with a unique id
# TODO: Graph nodes should be marked with the identity of the expansion query node(s) that made them

# Constrain each step of the search. This is a stateful "graph" guiding the search
class Taxonomy(Pattern):
  def __init__(self,
               taxonomyName:str,
               startNode:Node,
               taxonomyNodes:[Node],
               taxonomyRelationships:[NodeRelationship],
               nodeConstraints:[NodeConstraint],
               relationshipConstraints:[RelationshipConstraint]):
    Pattern.__init__(self)
    self.taxonomyName=taxonomyName
    self.startNode=startNode
    self.taxonomyNodes=taxonomyNodes
    self.taxonomyRelationships=taxonomyRelationships
    self.nodeConstraints=nodeConstraints
    self.relationshipConstraints=relationshipConstraints
  

  def getTaxonomyName(self):
    return self.taxonomyName

  def getTaxonomyNodes(self):
    return self.taxonomyNodes

  def getStartNode(self):
    return self.startNode

  def getTaxonomyRelationships(self):
    return self.taxonomyRelationships

  def getNodeConstraints(self):
    return self.nodeConstraints

  def getRelationshipConstraints(self):
    return self.relationshipConstraints

  def getJson(self):
    response = {}
    response["nodes"]={}
    for node in self.taxonomyNodes:
      attributes={}
      for attribute in node.getAttributesList():
        attributes[attribute]=node.getAttribute(attribute)
      response["nodes"][node.getId()]=attributes

    response["relationships"]={}
    for relationship in self.taxonomyRelationships:
      attributes=relationship.getAttributesList()
      response["relationships"][relationship.getId()]=attributes
    
    response["nodeConstraints"]=[n.getAttributesList() for n in self.nodeConstraints]
    response["relationshipConstraints"]=[r.getAttributesList() for r in self.relationshipConstraints]
    response["taxonomyName"]=self.taxonomyName
    response["startNode"]=self.startNode.getId()
    

    return response

  def appendNodeConstraint(self,newConstraint:NodeConstraint):
    #TODO: must not override taxonomy constraints (can make results set more narrow, but cannot make more broad)
    self.nodeConstraints.append(newConstraint)
    pass

  def appendRelationshipConstraint(self,newConstraint:RelationshipConstraint):
    #TODO: must not override taxonomy constraints (can make results set more narrow, but cannot make more broad)
    self.relationshipConstraints.append(newConstraint)
    pass
