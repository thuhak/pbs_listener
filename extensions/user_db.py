import logging
from py2neo import Graph


logger = logging.getLogger(__name__)


class UserInfo:
    """
    get user info from user database
    """

    def __init__(self, uri: str, db_user: str, db_pass: str):
        self.graph = Graph(uri, auth=(db_user, db_pass))

    def __call__(self, username: str) -> dict:
        result = {}
        dep_cypher = f'''
        MATCH (:People {{workerUserId: '{username}'}})-[:WORK_AT]->(d:Department)
        RETURN d.name AS dep UNION
        MATCH (:People {{workerUserId: '{username}'}})-[:WORK_AT]->(:Department)-[:SUP_DEP*]->(up:Department)
        RETURN up.name AS dep'''
        email_cypher = f'''
        MATCH (n:People {{workerUserId: '{username}'}})
        RETURN n.email AS email
        '''
        logger.info(f'getting date of {username}')
        try:
            deps = [x['dep'] for x in self.graph.run(dep_cypher).data()]
            email = self.graph.run(email_cypher).data()[0]['email']
            result = {
                'email': email,
                'departments': deps
            }
        except Exception as e:
            logger.error(f'fail to get data from ipeople db, {str(e)}')
        return result
