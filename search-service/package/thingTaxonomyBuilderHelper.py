from pattern import Pattern
from node import Node
from nodeType import NodeType
from nodeRelationship import NodeRelationship
from nodeRelationshipType import NodeRelationshipType
from nodeConstraint import NodeConstraint
from relationshipConstraint import RelationshipConstraint
from taxonomy import Taxonomy
from nodeRelationshipMultiplicityConstraint import NodeRelationshipMultiplicityConstraint

class ThingTaxonomyBuilderHelper():
  def __init__(self):
    return

  def person(self,name:str):
    p=Node(NodeType.PERSON)
    p.setAttribute("name",name)
    return p

  def org(self,name:str):
    o=Node(NodeType.ORGANIZATION)
    o.setAttribute("name",name)
    return o

  def thing(self,name:str):
    t=Node(NodeType.THING)
    t.setAttribute("name",name)
    return t

  def sco1exact(self,source:Node,dest:Node):
    return NodeRelationship(NodeRelationshipType.SCO,NodeRelationshipMultiplicityConstraint.ONE_TO_EXACTLY_ONE,source.getId(),dest.getId(),None,None)

  def sco1plus(self,source:Node,dest:Node):
    return NodeRelationship(NodeRelationshipType.SCO,NodeRelationshipMultiplicityConstraint.ONE_TO_ONE_OR_MORE,source.getId(),dest.getId(),None,None)

  def sco0plus(self,source:Node,dest:Node):
    return NodeRelationship(NodeRelationshipType.SCO,NodeRelationshipMultiplicityConstraint.ONE_TO_ZERO_OR_MORE,source.getId(),dest.getId(),None,None)

  #Note: first thing is the start node!!
  def createThingTaxonomy(self,taxonomyName:str,thingList:[str],thingRelationships):
    startNode=None
    thingDict={}
    for t in thingList:
      thingDict[t]=self.thing(t)
      if startNode is None:
        startNode=thingDict[t]
    
    relList = []
    for source,dest,relType in thingRelationships:
      # taco has SCO shell
      source=thingDict[source]
      dest=thingDict[dest]
      aRel = relType(source,dest)
      relList.append(aRel)
    
    return Taxonomy(taxonomyName,startNode,[n for n in thingDict.values()],relList,[],[])
