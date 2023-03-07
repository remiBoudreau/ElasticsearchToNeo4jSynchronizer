from pattern import Pattern
from node import Node
from nodeRelationship import NodeRelationship
from nodeConstraint import NodeConstraint
from relationshipConstraint import RelationshipConstraint
from taxonomy import Taxonomy
from dataSources import DataSources
from expansionQuery import ExpansionQuery

import joblib
import networkx as nx
import numpy as np

import itertools
from nodeRelationshipMultiplicityConstraint import NodeRelationshipMultiplicityConstraint

# Do iterative (recursive) STATEFUL searching. The state of the search is the state of the nodes in the taxonomy
class Search(Pattern):
  def __init__(self,
               taxonomyId:str,
               searchNodeConstraints:[NodeConstraint],
               searchRelationshipConstraints:[RelationshipConstraint]):
    Pattern.__init__(self)
    self.taxonomyId = taxonomyId
    self.taxonomyInstance = self.loadTaxonomy(taxonomyId)
    for nodeConstraint in searchNodeConstraints:
      self.addNodeConstraint(nodeConstraint)
    for relationshipConstraint in searchRelationshipConstraints:
      self.addRelationshipConstraint(relationshipConstraint)

    G,labeldict = self.taxonomy2networkx(self.taxonomyInstance)

    self.G = G
    self.labeldict = labeldict

    #nx.draw(G, labels=labeldict, with_labels = True)

    #inv_map = {v: k for k, v in labeldict.items()}
    
    startNode = self.taxonomyInstance.getStartNode().getId()

    self.allPaths = []
    for targetNodeID in labeldict.items():
      #skip start node
      if targetNodeID!=startNode:
        paths = nx.all_simple_paths(self.G, source=startNode, target=targetNodeID)
        for p in list(paths):
          self.allPaths.append(p)

    self.taxonomyNodeIndex = self.buildTaxonomyNodeIndex(self.taxonomyInstance.getTaxonomyNodes())
    self.taxonomyRelationshipIndex = self.buildTaxonomyRelationshipIndex(self.taxonomyInstance.getTaxonomyRelationships())
    self.taxonomyNodeConstraintIndex = self.buildNodeConstraintIndex(self.taxonomyInstance.getNodeConstraints())
    
    self.queryResponseIndex={}
    return

  def loadTaxonomy(self,taxonomyId):
    return joblib.load(f"taxonomies/taxonomy_{taxonomyId}.joblib")

  def getTaxonomyId(self):
    return self.taxonomyId

  def addNodeConstraint(self,nodeConstraint:NodeConstraint):
    #Note: must not override taxonomy constraints (can make results set more narrow, but cannot make more broad)
    self.taxonomyInstance.appendNodeConstraint(nodeConstraint)
    pass
  
  def getNodeConstraints(self):
    #Note: must not override taxonomy constraints (can make results set more narrow, but cannot make more broad)
    return self.taxonomyInstance.getNodeConstraints()
    

  def addRelationshipConstraint(self,relationshipConstraint:RelationshipConstraint):
    #Note: must not override taxonomy constraints (can make results set more narrow, but cannot make more broad)
    self.taxonomyInstance.appendRelationshipConstraint(relationshipConstraint)
    pass

  def generateMatchStatement(self):
    taxonomy=self.taxonomyInstance
    
    componentList=[]

    # Apply any additional relationship constraints, if they exist from the class RelationshipConstraint
    relConstraints = taxonomy.getRelationshipConstraints()
    for rCon in relConstraints:
      sourceNode=self.taxonomyNodeIndex[rCon.sourceNodeId]
      targetNode=self.taxonomyNodeIndex[rCon.targetNodeId]

    relationships=taxonomy.getTaxonomyRelationships()
    requiredRelationships=[]
    for r in relationships:
      if r.getMultiplicity() == NodeRelationshipMultiplicityConstraint.ONE_TO_ONE_OR_MORE:
        requiredRelationships.append(r)
    
    for rr in requiredRelationships:
      sourceNode=self.taxonomyNodeIndex[rr.sourceNodeId]
      targetNode=self.taxonomyNodeIndex[rr.targetNodeId]
      
      sourceNodeType = sourceNode.getAttribute("NodeType")
      targetNodeType = sourceNode.getAttribute("NodeType")
      relType = rr.getNodeRelationshipType()
      component=f"({rr.sourceNodeId}:{sourceNodeType})-[:{relType.value}]-({rr.targetNodeId}:{targetNodeType})"
      componentList.append(component)
    
    relationshipMatches=",".join(componentList)
    
    return f"MATCH {relationshipMatches}"

  def generateOptionalMatchStatement(self):
    taxonomy=self.taxonomyInstance
    relationships=taxonomy.getTaxonomyRelationships()

    optionalRelationships=[]
    for r in relationships:
      if r.getMultiplicity() in [NodeRelationshipMultiplicityConstraint.MANY_TO_MANY, NodeRelationshipMultiplicityConstraint.ONE_TO_ZERO_OR_MORE]:
        optionalRelationships.append(r)
    
    componentList=[]
    for ore in optionalRelationships:
      sourceNode=self.taxonomyNodeIndex[ore.sourceNodeId]
      targetNode=self.taxonomyNodeIndex[ore.targetNodeId]
      
      sourceNodeType = sourceNode.getAttribute("NodeType")
      targetNodeType = sourceNode.getAttribute("NodeType")
      
      relType = ore.getNodeRelationshipType()
      component=f"({ore.sourceNodeId}:{sourceNodeType})-[:{relType.value}]-({ore.targetNodeId}:{targetNodeType})"
      componentList.append(component)
    
    optionalRelationshipMatches=",".join(componentList)
    
    return f"OPTIONAL MATCH {optionalRelationshipMatches}"

  def generateWhereStatement(self):
    taxonomy=self.taxonomyInstance
    componentList=["1=1"]
    # TODO: force each node into a set of possible IDs
    #### use self.queryResponseIndex to limit search nodes #######

    # Apply any additional node constraints, if they exist from the class NodeConstraint in the taxonomy
    nodeConstraints = taxonomy.getNodeConstraints()

    for nc in nodeConstraints:
      affectedNodeId  = nc.getAffectedNodeId()
      attrName        = nc.getConstrainedAttributeName()
      constraintType  = nc.getConstraintType()
      constraintValue = nc.getConstraintValue().strip() # TODO avoid CYPHER injection.
      
      componentList.append(f"{affectedNodeId}.{attrName} {constraintType.getNeo4jName()}'{constraintValue}'")
    whereClauses=" AND ".join(componentList)
    return f"WHERE {whereClauses} \nRETURN DISTINCT *;"

  def taxonomy2networkx(self,t:Taxonomy):
    G = nx.MultiDiGraph()
    nodeIDs = t.getTaxonomyNodes()
    labeldict = {}
    for node in nodeIDs:
      labeldict[node.getId()]=node.getAttribute("name")
      G.add_node(node.getId())

    relationships = t.getTaxonomyRelationships()
    relationshipIDs=[]
    for r in relationships:
      relationshipIDs.append([r.getSourceNodeId(),r.getTargetNodeId()])

    G.add_edges_from(relationshipIDs, color="red")
    return G,labeldict

  # From: https://stackoverflow.com/questions/5434891/how-can-i-iterate-over-overlapping-current-next-pairs-of-values-from-a-list
  def pairwise(self,iterable):
      "s -> (s0, s1), (s1, s2), (s2, s3), ..."
      a, b = itertools.tee(iterable)
      next(b, None)
      return zip(a, b)  

  def knowledgeGraphDiscoveryQuery(self,validDataSources):
    # Expansion query
    #TODO: run cypher query on graph to see what nodes we will expand upon
    #TODO: if this is the first iteration of this search: join results of the query with root node query data
    #TODO: call APIs to get data on joined query results
    #TODO: exit condition: no more nodes left, or max steps/depth reached
    startNode=self.taxonomyInstance.getStartNode()
    self.expansionQuery(startNode)
    expansionQueries = self.bfs_names(validDataSources)
    # TODO: change from print statements to actions
    print("QUERY GRAPH")
    print("cypher query version of taxonomy")
    cypherQuery = [self.generateMatchStatement(),
                   self.generateOptionalMatchStatement(),
                   self.generateWhereStatement()
                  ]
    print(cypherQuery[0])
    print(cypherQuery[1])
    print(cypherQuery[2])
    return expansionQueries,cypherQuery

  def buildTaxonomyRelationshipIndex(self,relationships:[NodeRelationship]):
    taxonomyRelationshipIndex={}
    for r in relationships:
      aKey = (r.sourceNodeId,r.targetNodeId)
      #print(aKey)
      taxonomyRelationshipIndex[aKey]=r
    return taxonomyRelationshipIndex

  def buildTaxonomyNodeIndex(self,nodes:[Node]):
    taxonomyNodeIndex={}
    for n in nodes:
      taxonomyNodeIndex[n.getId()]=n
    return taxonomyNodeIndex

  def buildNodeConstraintIndex(self, taxonomyNodeConstraints:[NodeConstraint]):
    taxonomyNodeConstraintIndex={}
    for nc in taxonomyNodeConstraints:
      targetId = nc.getAffectedNodeId()
      if targetId in taxonomyNodeConstraintIndex.keys():
        taxonomyNodeConstraintIndex[targetId]=taxonomyNodeConstraintIndex[targetId]+[nc]
      else:
        taxonomyNodeConstraintIndex[targetId]=[nc]
    return taxonomyNodeConstraintIndex


  def getNodeProperties(self, nodeId:str):
    
    if nodeId not in self.taxonomyNodeConstraintIndex.keys():
        return []
    
    properties=[]
    for constraint in self.taxonomyNodeConstraintIndex[nodeId]:
      constrainedAttributeName = constraint.getConstrainedAttributeName()

      # TODO: support more types of data than just "name"... this is lame!
      # For now skip anything that is not affecting the property "name" that we currently support :(
      #if constrainedAttributeName == "name":
      constraintType=constraint.getConstraintType()
      constraintValue=constraint.getConstraintValue()
      aProperty = {"key":f"{constraintType}",
                    "value":f"{constraintValue}",
                    "subject":constraint.getConstrainedAttributeName(),"type":"property"}
      properties.append(aProperty)
    return properties

  def bfs_names(self,validDataSources):
    expansionQueries = []    
    tenant={"key":"tenant-name", "value":"MOCKED-ed-will-do-this-uuid", "subject":"Tenant", }
    unique_payloads = set()
    for path in self.allPaths:
      components=[]
      draft_ce = []
      hasPropertyConstraint=False
      for edge in self.pairwise(path):
        #ic(path,edge)
        rel = self.taxonomyRelationshipIndex[(edge[0],edge[1])]
        type0=self.taxonomyNodeIndex[edge[0]].getAttribute("NodeType")
        node0type = self.taxonomyNodeIndex[edge[0]].getAttribute("NodeType")
        nodeId = edge[0]
        nodeName = self.labeldict[edge[0]]
        components.append(f"{type0} {nodeName}")
        properties = self.getNodeProperties(nodeId)
        print(properties)
        # TODO: support more data sources
        # TODO: support more types of data than just "name"
         
        for source in validDataSources:
            if len(properties) > 0:
              hasPropertyConstraint=True
              aPart = {"key":"name", "value":f"{nodeName}", "subject":f"{node0type}","taxonomy-node-id":f"{nodeId}", 
              "properties": properties, "data-source": source}
              draft_ce.append(aPart)

      pathToNode = ", ".join(components)
      #print(taxonomyRelationshipIndex[edge].getAttributesList())
      #multType = str(rel.getMultiplicity()).split(".")[1]
      #rtype=     str(rel.getNodeRelationshipType()).split(".")[1]
      
      type1=self.taxonomyNodeIndex[path[-1]].getAttribute("NodeType")
      node1type=self.taxonomyNodeIndex[path[-1]].getAttribute("NodeType")
      # TODO - Find matches to this node from neo4j
      # TODO - If we don't know from neo4j, put in the relationship name (e.g., spouse, mother). Then it will be a unique search!
      finalNodeName = self.labeldict[path[-1]]
      print(f"Expand on {pathToNode} find IDs for {type1} {finalNodeName}")
      properties = self.getNodeProperties(path[-1])
      for source in validDataSources:
        finalPart={"key":"name", 
                     "value":f"{finalNodeName}", 
                     "subject":f"{node1type}",
                     "taxonomy-node-id":f"{path[-1]}", 
                     "properties": properties, 
                     "data-source": source
                }
        if len(properties) > 0:
          hasPropertyConstraint=True
          draft_ce.append(finalPart)

      draft_ce.append(tenant)
      eq = ExpansionQuery(searchId=self.getId(),taxonomyId=self.taxonomyId,cloudEventPayload=draft_ce)
      if not str(draft_ce) in unique_payloads and hasPropertyConstraint:
        unique_payloads.add(str(draft_ce))
        expansionQueries.append(eq)
    return expansionQueries

  #TODO: MAKE THIS REAL BY CONNECTING IT TO THE DATA PIPELINE AND NEO4J
  def queryResponseMock(self,aQuery):
    amount = np.random.randint(1, high=5, dtype=int)
    return [Pattern().getId() for _ in range(amount)]

  #print("POPULATE GRAPH")

  def expansionQuery(self,taxonomyNode:Node):
    name = self.labeldict[taxonomyNode.getId()]
    if len(self.queryResponseIndex.keys())==0:
      print(f"Query data pipeline for {name} (data expansion)")
      print(f"Query neo4j for nodeIDs related to {name}")
      self.queryResponseIndex[taxonomyNode.getId()]=self.queryResponseMock(name)
      print("NodeIDs response from cypher query =  ", self.queryResponseIndex[taxonomyNode.getId()])
    #Given THING taco find IDs that THING shell
    return
