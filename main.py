import py2neo.data
from lxml import etree
from io import StringIO, BytesIO
from py2neo import Graph, Node, Relationship, NodeMatcher
import os

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
# graph.run("CREATE CONSTRAINT ON (a:Author) ASSERT a.author_name IS UNIQUE")
# graph.run("CREATE CONSTRAINT ON (p:Paper) ASSERT p.id IS UNIQUE")
matcher = NodeMatcher(graph)
fromNodeTempName = ''
fromNode = ''
def doDblp2Neo():
    events = ("start", "end")
    context = etree.iterparse("dblp_test.xml", events=events, huge_tree=True, load_dtd=True)
    type_list = ['article', 'inproceedings', 'proceedings', 'book', 'incollection', 'phdthesis', 'mastersthesis', 'www']
    needed_type_list = ['article', 'phdthesis', 'book']
    neo4j_query_data = {}
    authorList = []
    i = 0
    for action, elem in context:
        i = i+1
        # read order: paper type -> author....
        tag = elem.tag
        # value = elem.text
        # do check and insert value in start
        if action == "start":
            if tag in needed_type_list:
                neo4j_query_data['type'] = tag
            elif neo4j_query_data:
                if tag == "author":
                    if elem.text:
                        authorList.append(elem.text)
                        neo4j_query_data['name_list'] = authorList
                    else:
                        neo4j_query_data = {}
                if tag == "title":
                    if elem.text:
                        neo4j_query_data['title'] = elem.text
                    else:
                        neo4j_query_data = {}
                if tag == "isbn":
                    if elem.text:
                        neo4j_query_data['isbn'] = elem.text
                # if tag == "url":
                #     if elem.text:
                #         pass
                if tag == "ee":
                    if elem.text:
                        neo4j_query_data['ee'] = elem.text

        # do data reset
        if action == "end":
            if tag in type_list:
                if tag in needed_type_list:
                    # check empty
                    if ('name_list' in neo4j_query_data) \
                            & ('title' in neo4j_query_data)\
                            &(('ee' in neo4j_query_data)|('isbn'in neo4j_query_data)):
                        # print(neo4j_query_data)
                        doPy2neo(neo4j_query_data)
                        print(i)

                #remove lists
                authorList = []
                neo4j_query_data = {}
                # print("="*25)



    print("finish")


def doPy2neo(data_dict):
    # publish relationship
    paper = Node("Paper", title=data_dict['title'])
    graph.merge(paper, "Paper", "title")
    if 'ee' in data_dict:
        paper['ee'] = data_dict['ee']
    if 'isbn' in data_dict:
        paper['isbn'] = data_dict['isbn']
    # paper["title"] = data_dict["title"]
    graph.push(paper)

    for x in data_dict['name_list']:
        author = Node("Author", author_name=x)
        graph.merge(author, "Author", "author_name")
        graph.push(author)
        POSTS = Relationship.type("POSTS")
        graph.merge(POSTS(author, paper))

# def test_neo_connection():
#     # wrong password will throw error

# this part is for test reference binding between 2 papers
# use data from OpenCitations
def searchTest(from_node,to_node):
    #find from_node whether exists doi
    #according to the result that 1 paper -refer->multi papers
    #use a temp node to avoid search is possible

    # if fromNodeTempName == from_node:
    #     from_node = from_node

    from_results = graph.run("match (n:Paper) where n.ee contains \'"
                        +from_node
                        +"\' OR n.isbn contains \'"
                        +from_node
                        +"\' return n").data()
    cite_flag = True
    if from_results:
        # fromNodeTempName = from_node
        # fromNode = from_results[0]['n']
        for record in from_results:
            fromNode = record['n']
    else:
        cite_flag = False
    to_results = graph.run("match (n:Paper) where n.ee contains \'"
                        +to_node
                        +"\' OR n.isbn contains \'"
                        +to_node
                        +"\' return n").data()
    if to_results:
        for record in to_results:
            toNode = record['n']
    else:
        cite_flag = False

    if cite_flag:
        POSTS = Relationship.type("Cites")
        graph.merge(POSTS(fromNode, toNode))
        print("cited")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    doDblp2Neo()
    #searchTest("https://d-nb.info/1009095463","http://hdl.handle.net/10589/88161")
    #test_neo_connection()


