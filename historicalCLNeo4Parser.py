# -*- coding: utf-8 -*-
#Court Listener Web Parser
import re, json
from items import CourtlistenerItem
from subprocess import check_output
from py2neo import Node, Relationship, Graph
from collections import defaultdict
from time import time
import sys
import logging
from datetime import datetime


logging.basicConfig(filename='/data/parser/cLNeo4Parser.log',level=logging.INFO)

def getHeaders():
    headers = ['court', 'date_filed', 'html_with_citations', 'html_lawbox', 'id', 'judges', 'plain_text', 'precedential_status']
    citation_headers = ['case_name', 'docket_number', 'federal_cite_one', 'federal_cite_two', 'federal_cite_three', 'lexis_cite', 'neutral_cite', 'scotus_early_cite', 'specialty_cite_one', 'state_cite_one', 'state_cite_one', 'state_cite_two', 'state_cite_three', 'state_cite_regional', 'westlaw_cite']
    return headers, citation_headers



def fileParser(filepath):
    try:
        #Open up the input file
        out = json.loads(open(filepath,'r').read())
        #
        headers, citation_headers = getHeaders()
        result = dict((k,v) for k, v in out.iteritems() if k in headers and v != None)
        resultCit = dict((k,v) for k,v in (out['citation']).iteritems() if k in citation_headers and v != None)
        #
        item = CourtlistenerItem()
        #
        if('case_name' in resultCit.keys()):
            item['title'] = resultCit['case_name']
            resultCit.pop('case_name')
        else:
            item['title'] = None
        #
        if('docket_number' in resultCit.keys()):
            item['dockeno'] = resultCit['docket_number']
            resultCit.pop('docket_number')
        else:
            item['dockeno'] = None
        #
        item['link'] = str(result['id'])+".html"
        item['dateFiled'] =result['date_filed']
        item['court'] = result['court']
        item['statu'] = result['precedential_status']
        item['opinionid'] = result['id']
        item['judges'] = result['judges']
        #
        if(len(resultCit.values())>=1):
            item['citations'] = resultCit.values()
        else:
            item['citations'] = None
        #
        return item
    except:
        logging.info("Could not process request " + filepath)



def createNode(filepath):
    try:
        global tx, statement
        item = fileParser(filepath)
        tx.append(statement, **item)
        return item
    except:
        logging.info("Node creation failed " + filepath)



def listComp(inputFiles, i):
    finalList = []
    #
    while(inputFiles):
        if(len(inputFiles)>=i):
            finalList.append(inputFiles[:i])
            del inputFiles[:i]
        else:
            finalList.append(inputFiles)
            del inputFiles
            break
    #
    return finalList



def createRelationship(rootOpinionId):
    try:
        global rtx, citationsDict
        #Taking citations where the opinionid is the first reference number.
        cites = citationsDict[str(rootOpinionId)]
        #Get referenced opinionids
        if cites:
            #Reduces multi populated lists down to a single list
            cites = reduce((lambda x,y:x + y), map((lambda x:x.split(',')), cites))
            map((lambda x: rtx.append(relationshipStatement, {"nOpin":rootOpinionId, "mOpin":x})), map((lambda x:int(x)), cites))
    except:
        logging.info("Partial relationship creation failed for " + rootOpinionId)





if __name__ == '__main__':
    time1 = time()
    logging.info("Process Started: " + datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))
    #Create the graph database
    gdb = Graph("http://neo4j:Capso123@ec2-54-164-106-231.compute-1.amazonaws.com:7474/db/data/")
    #gdb = Graph()

    citationsList = open(sys.argv[1],'r').read().split()
    citationsDict = defaultdict(list)
    map((lambda x:citationsDict[x[0]].append(x[1])),map((lambda x:x.split(':')),citationsList))
    del citationsList

    inputFiles = check_output(["ls",sys.argv[2]]).split()
    inputFiles = filter((lambda x:'.json' in x), inputFiles)
    inputFiles = listComp(inputFiles, 5000)

    #Add a unique constraint on opinionid
    gdb.cypher.execute("CREATE CONSTRAINT ON (n:Opinion) ASSERT n.opinionid IS UNIQUE")

    statement = "CREATE (n:Opinion {statu:{statu}, court:{court}, title:{title}, dockeno:{dockeno}, judges:{judges}, dateFiled:{dateFiled}, citations:{citations}, link:{link}, opinionid:{opinionid}}) RETURN n"

    neo4jNodes = []
    for i in range(0,len(inputFiles)):
        tx = gdb.cypher.begin()
        neo4jNodes.append(map(createNode, map((lambda x:sys.argv[2]+x),inputFiles[i])))
        tx.process()
        tx.commit()


    logging.info("Node Creation Complete: " + datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))

    #Create a list of rootOpinionIds to search the mapped citation file
    rootOpinionIds = map((lambda x:x['opinionid']), filter((lambda x:x != None), reduce((lambda x,y: x+y),neo4jNodes)))
    rootOpinionIds = listComp(rootOpinionIds, 1000)

    #Create a hollow statement
    relationshipStatement = "MATCH (n:Opinion{opinionid:{nOpin}}), (m:Opinion{opinionid:{mOpin}}) USING INDEX n:Opinion(opinionid) USING INDEX m:Opinion(opinionid) CREATE (n)-[:CITES]->(m)"

    for i in range(0, len(rootOpinionIds)):
        rtx = gdb.cypher.begin()
        map(createRelationship, rootOpinionIds[i])
        rtx.process()
        rtx.commit()
    
    logging.info("Relationship Creation Complete: " + datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))

    time2 = time()

    logging.info("Time to completion: " + str(time2-time1) + " seconds")

