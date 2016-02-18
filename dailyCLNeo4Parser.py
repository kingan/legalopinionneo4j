from datetime import datetime, timedelta
import requests
from py2neo import Node, Relationship, Graph
from items import CourtlistenerItem
import logging


# ~~ Get headers for jsonNodeParser  ~~ #

def getHeaders():
    headers = ['court', 'date_filed', 'html_with_citations', 'html_lawbox', 'id', 'judges', 'plain_text', 'precedential_status']
    citation_headers = ['case_name', 'docket_number', 'federal_cite_one', 'federal_cite_two', 'federal_cite_three', 'lexis_cite', 'neutral_cite', 'scotus_early_cite', 'specialty_cite_one', 'state_cite_one', 'state_cite_one', 'state_cite_two', 'state_cite_three', 'state_cite_regional', 'westlaw_cite']
    return headers, citation_headers

# ~~ ~~ #



# ~~ Json Parser to Dictionary with Transaction Append ~~ #

def jsonNodeParser(i):
    #global tx, statement, nodeID
    headers, citation_headers = getHeaders()
    result = dict((k,v) for k, v in i.iteritems() if k in headers and v != None)
    result.update({"citations" : dict((k,v) for k, v in i['citation'].iteritems() if k in citation_headers and v != None)})
    #
    if('case_name' in result['citations'].keys()):
        result['title']=result['citations'].pop('case_name')
    else:
        result['title'] = None
    #
    if('docket_number' in result['citations'].keys()):
        result['dockeno']=result['citations'].pop('docket_number')
    else:
        result['dockeno'] = None
    #
    if(len(result['citations'].values())>=1):
        result['citations'] = result['citations'].values()
    else:
        result['citations'] = None
    #
    result['link']=str(result['id'])+'.html'
    #
    tx.append(statement, **result)
    nodeID.append(result['id'])
    return result

# ~~ ~~ #



# ~~ Json Parser for Neo4J Node Creation  ~~ #

def jsonNodeHandler(inputDate, offset):
    query = "https://www.courtlistener.com/api/rest/v2/document/?date_modified__gt=%s+00:00Z&date_modified__lt=%s+00:00Z&order_by=date_modified&offset=%s"%(inputDate.strftime('%Y-%m-%d') , (inputDate + timedelta(days=1)).strftime('%Y-%m-%d'), (offset*20))
    r = requests.get(query, auth=('username','password'))
    if r.ok:
        if (r.json()['objects']):
             map(jsonNodeParser, r.json()['objects'])
        else:
             logging.info("No results returned")
    else:
        logging.info("Bad node request")

# ~~ ~~ #



# ~~ Handle for Json Relationship Parser ~~ #

def jsonRelationshipHandler(nodeId):
    query = "https://www.courtlistener.com/api/rest/v2/cites/?id=%s"%nodeId
    r = requests.get(query, auth=('username','password'))
    relCount = ((r.json()['meta']['total_count'])/20)+1
    map(jsonRelationshipParser, [nodeId]*len(range(relCount)), range(relCount))

# ~~ ~~ #



# ~~ Json Parser for Neo4J Relationship Creation ~~ #

def jsonRelationshipParser(nodeId, offset):
    #global rtx, relationshipStatement
    query = "https://www.courtlistener.com/api/rest/v2/cites/?id=%s&offset=%s&format=json&fields=id"%(nodeId, (offset*20))
    r = requests.get(query, auth=('username','password'))
    if r.ok:
        if (r.json()['objects']):
            map((lambda x:rtx.append(relationshipStatement, {"nOpin":nodeId, "mOpin":x})), map((lambda x:x['id']), r.json()['objects']))
        else:
            logging.info("No citation results returned for " + str(nodeId))
    else:
        logging.info("Bad citation request")

# ~~ ~~ #



# ~~ Initial Graph Creation  ~~ #

def graphCreation():
    gdb = Graph("localhost:7474/db/data/")
    gdb.cypher.execute("CREATE CONSTRAINT ON (n:dailyOpinion) ASSERT n.opinionid IS UNIQUE")
    return gdb

# ~~ ~~ #



# ~~ ~~ #

def setInitialGraphStatement():
    statement = "CREATE (n:dailyOpinion {statu:{precedential_status}, court:{court}, title:{title}, dockeno:{dockeno}, judges:{judges}, dateFiled:{date_filed}, citations:{citations}, link:{link}, opinionid:{id}}) RETURN n"
    return statement

# ~~ ~~ #



# ~~ ~~ #

def setInitialRelationalStatement():
    statement = "MATCH (n:dailyOpinion{opinionid:{nOpin}}), (m:dailyOpinion{opinionid:{mOpin}}) USING INDEX n:dailyOpinion(opinionid) USING INDEX m:dailyOpinion(opinionid) CREATE (n)-[:CITES]->(m)"
    return statement

# ~~ ~~ #



# ~~ ~~ #

def testInitialContact(inputDate):
    #Query to check the number of records. Also, check if initial contact can be made with a site. 
    testQueryResult=None
    while testQueryResult is None:
        try:
            query = "https://www.courtlistener.com/api/rest/v2/document/?date_modified__gt=%s+00:00Z&date_modified__lt=%s+00:00Z&order_by=date_modified"%(inputDate.strftime('%Y-%m-%d') , (inputDate + timedelta(days=1)).strftime('%Y-%m-%d'))
            r = requests.get(query, auth=('username','password'))
            testQueryResult = "SUCCESS"
            logging.info(testQueryResult + ": REQUEST SUCCESSFULLY HANDLED AT: " + inputDate.strftime("%Y-%m-%d:%H:%M:%S"))
            #
            #Number of iterations to be executed to retrieve all data
            count = ((r.json()['meta']['total_count'])/20)+1
            return count
        except:
            logging.info("ERROR: COULD NOT HANDLE REQUEST ... RETRYING AT : " + inputDate.strftime("%Y-%m-%d:%H:%M:%S"))
            sleep(60)
            pass

# ~~ ~~ #



# ~~ Main Handler ~~ #

def mainHandler(inputDate):
    logging.basicConfig(filename='/data/parser/dailyCLNeo4Parser.%s.log'%inputDate.strftime('%Y%m%d'),level=logging.DEBUG)
    count = testInitialContact(inputDate)
    map(jsonNodeHandler, [inputDate]*len(range(count)), range(count))
    logging.info("Successfully populated database")

# ~~  ~~ #




if __name__ == '__main__':

    nodeID = []
    gdb = graphCreation()
    statement = setInitialGraphStatement()
    day = datetime.now()

    tx = gdb.cypher.begin()
    mainHandler(day)
    tx.process()
    tx.commit()

    relationshipStatement = setInitialRelationalStatement()
    rtx = gdb.cypher.begin()
    map(jsonRelationshipHandler, nodeID)
    rtx.process()
    rtx.commit()

