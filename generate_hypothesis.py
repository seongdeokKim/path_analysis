from neo4j import GraphDatabase


class Neo4jManager:
    def __init__(self):
        self.driver = None

    def get_driver(self, uri,
                   user, password):
        self.driver = GraphDatabase.driver(uri,
                                           auth=(user, password))

    def close(self):
        self.driver.close()

    @staticmethod
    def get_entity_id_map(samples):
        '''
        Get entity_id_map
        '''
        entity_id_map = {}
        entity_list = []
        for s in samples:
            entity_list.append(s["head_entity"])
            entity_list.append(s["tail_entity"])
        for id, entity in enumerate(set(entity_list)):
            entity_id_map[entity] = id

        return entity_id_map

    @staticmethod
    def get_ner_tag_id_map(samples):
        '''
        Get ner_tag_id_map
        '''
        ner_tag_id_map = {}
        ner_tag_list = []
        for s in samples:
            ner_tag_list.append(s["head_entity_tag"])
            ner_tag_list.append(s["tail_entity_tag"])
        ner_tag_list = list(set(ner_tag_list))
        print("ner tags are as follows:", ner_tag_list)

        for id, ner_tag in enumerate(ner_tag_list):
            ner_tag_id_map[ner_tag] = id

        return ner_tag_id_map

    def clear_previous_record(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_all)

    def create_all_entity(self,
                          samples,
                          entity_id_map,
                          ner_tag_id_map):
        '''
        Create all entity through cypher
        '''
        with self.driver.session() as session:

            entity_and_tag_id_set = set()
            for s in samples:
                h_name = s["head_entity"]
                t_name = s["tail_entity"]
                h_ner_tag = s["head_entity_tag"]
                t_ner_tag = s["tail_entity_tag"]

                entity_and_tag_id = str(entity_id_map.get(h_name)) + \
                                    "_" + \
                                    str(ner_tag_id_map.get(h_ner_tag))

                if entity_and_tag_id not in entity_and_tag_id_set:
                    entity_and_tag_id_set.add(entity_and_tag_id)

                    head_entity = Entity()
                    head_entity.name = h_name
                    head_entity.ner_tag = h_ner_tag
                    record = session.write_transaction(self._create_entity_tx,
                                                       entity=head_entity)
                    print(record)

                entity_and_tag_id = str(entity_id_map.get(t_name)) + \
                                    "_" + \
                                    str(ner_tag_id_map.get(t_ner_tag))

                if entity_and_tag_id not in entity_and_tag_id_set:
                    entity_and_tag_id_set.add(entity_and_tag_id)

                    tail_entity = Entity()
                    tail_entity.name = t_name
                    tail_entity.ner_tag = t_ner_tag
                    record = session.write_transaction(self._create_entity_tx,
                                                       entity=tail_entity)
                    print(record)

    def create_all_relationship(self,
                                samples,
                                entity_id_map,
                                ner_tag_id_map):
        '''
        Create all relationship through cypher
        '''
        with self.driver.session() as session:

            relationship_counter = self._compute_relationship_freq(samples,
                                                                   entity_id_map,
                                                                   ner_tag_id_map)
            for relationship, freq in relationship_counter.items():
                rel_field = relationship.split("||")
                record = session.write_transaction(self._create_relationship_tx,
                                                   h_name=rel_field[0],
                                                   h_ner_tag=rel_field[1],
                                                   rel_type=rel_field[2],
                                                   rel_frequency=int(freq),
                                                   t_name=rel_field[3],
                                                   t_ner_tag=rel_field[4])
                print(record)

    @staticmethod
    def _compute_relationship_freq(samples,
                                   entity_id_map,
                                   ner_tag_id_map):
        '''
        Count the frequency of relationship through cypher
        '''
        relationship_counter = {}
        for s in samples:
            h_name = s["head_entity"]
            t_name = s["tail_entity"]
            rel_type = s["relationship_type"]
            h_ner_tag = s["head_entity_tag"]
            t_ner_tag = s["tail_entity_tag"]

            rel_field = []
            if entity_id_map.get(h_name) < entity_id_map.get(t_name):
                rel_field += [h_name, h_ner_tag, rel_type, t_name, t_ner_tag]
            elif entity_id_map.get(h_name) > entity_id_map.get(t_name):
                rel_field += [t_name, t_ner_tag, rel_type, h_name, h_ner_tag]
            else:
                if ner_tag_id_map.get(h_ner_tag) < ner_tag_id_map.get(t_ner_tag):
                    rel_field += [h_name, h_ner_tag, rel_type, t_name, t_ner_tag]
                else:
                    rel_field += [t_name, t_ner_tag, rel_type, h_name, h_ner_tag]

            relationship = "||".join(rel_field)
            if relationship not in relationship_counter:
                relationship_counter[relationship] = 1
            else:
                relationship_counter[relationship] += 1

        return relationship_counter

    def traverse_path(self,
                      source_node_name,
                      target_node_name,
                      path_max_depth):
        '''
        Traverse for path analysis through cypher
        '''
        ### Find path
        result = []
        with self.driver.session() as session:
            paths = session.read_transaction(self._traverse_tx,
                                             start_node_name=source_node_name,
                                             end_node_name=target_node_name,
                                             max_depth=path_max_depth)
            print(f'A total of {len(paths)} paths')
            for path in paths:
                rels = [relationship for relationship in path.relationships]
                nodes = [node for node in path.nodes]

                p = []
                path_score = 0
                path_depth = len(rels)
                for i in range(len(nodes)):
                    if i == 0:
                        p.append("({})".format(nodes[i]['name']))
                    else:
                        p.append("[{}]".format(rels[i - 1].type))
                        p.append("({})".format(nodes[i]['name']))
                        path_score += rels[i - 1]['score']

                p = "--".join(p)
                path_score = float(path_score / len(rels))

                result.append(
                    (p, path_depth, path_score)
                )

        return result

    @staticmethod
    def _delete_all(tx):
        tx.run("MATCH (e) DETACH DELETE e")
        tx.run("MATCH (e) DELETE e")

    @staticmethod
    def _create_entity_tx(tx,
                          entity: 'Entity'):
        query = "CREATE " + \
                "(e:Entity {name: $name, ner_tag: $ner_tag}) " + \
                "RETURN id(e) + '||' + e.name + '||' + e.ner_tag + '}'"

        result = tx.run(query,
                        name=entity.name,
                        ner_tag=entity.ner_tag)

        return result.single()[0]

    @staticmethod
    def _create_relationship_tx(tx,
                                h_name: str, h_ner_tag: str,
                                rel_type: str, rel_frequency: int,
                                t_name: str, t_ner_tag: str):
        # https://moondol-ai.tistory.com/206?category=921465

        query = "MATCH (h:Entity {name: $h_name, ner_tag: $h_ner_tag})" + \
                "MATCH (t:Entity {name: $t_name, ner_tag: $t_ner_tag})" + \
                "CREATE (h)-[r:" + rel_type + " {score: $score}]->(t)" + \
                "RETURN '(' + h.name + ')--[' + type(r) + ']--(' + t.name + ')'"

        result = tx.run(
            query,
            h_name=h_name, h_ner_tag=h_ner_tag,
            score=rel_frequency,
            t_name=t_name, t_ner_tag=t_ner_tag,
        )

        return result.single()[0]

    @staticmethod
    def _traverse_tx(tx,
                     start_node_name: str,
                     end_node_name: str,
                     max_depth: int):
        # https://neo4j.com/docs/cypher-manual/current/execution-plans/shortestpath-planning/

        query = "MATCH " + \
                "path = " + \
                "(:Entity {name: $start_node_name})" + "-" + \
                "[*.." + str(max_depth) + "]" + "-" + \
                "(:Entity {name: $end_node_name}) " + \
                "RETURN path"

        result = tx.run(
            query,
            start_node_name=start_node_name,
            end_node_name=end_node_name,
        )

        paths = []
        for record in result:
            paths.append(record["path"])

        return paths


def get_samples(file: str):
    '''
    Read data
    '''
    samples = []
    with open(file, "r", encoding='utf-8') as fr:
        for i, line in enumerate(fr):
            if i != 0:
                field_line = line.strip().split("\t")
                # print(field_line)

                try:
                    relationship_type = field_line[0].strip()
                    left_entity = field_line[3].strip()
                    left_entity_tag = field_line[4].strip()
                    right_entity = field_line[6].strip()
                    right_entity_tag = field_line[7].strip()

                except Exception as e:
                    print(e)
                    continue

                sample = {
                    "relationship_type": relationship_type,
                    "head_entity": left_entity,
                    "head_entity_tag": left_entity_tag,
                    "tail_entity": right_entity,
                    "tail_entity_tag": right_entity_tag,
                }
                samples.append(sample)

    return samples


class Entity:
    def __init__(self):
        self._ner_tag = None
        self._name = None

    @property
    def ner_tag(self):
        return self._ner_tag
    @ner_tag.setter
    def ner_tag(self, value: str):
        self._ner_tag = value

    @property
    def name(self):
        return self._name
    @name.setter
    def name(self, value: str):
        self._name = value


if __name__ == '__main__':

    uri = "bolt://localhost:7687"
    user = "test"
    password = "test"

    update_neo4j = True
    data_for_update = "data/re_result.txt"

    path_max_depth = 7
    source_node_name = "the c-collar"
    target_node_name = "allergies"

    manager = Neo4jManager()
    manager.get_driver(uri, user, password)

    if update_neo4j is True:
        manager.clear_previous_record()

        samples = get_samples(file=data_for_update)

        entity_id_map = manager.get_entity_id_map(samples)
        ner_tag_id_map = manager.get_ner_tag_id_map(samples)

        manager.create_all_entity(samples,
                                  entity_id_map,
                                  ner_tag_id_map)
        manager.create_all_relationship(samples,
                                        entity_id_map,
                                        ner_tag_id_map)

    result = manager.traverse_path(source_node_name,
                                   target_node_name,
                                   path_max_depth)

    result.sort(key=lambda t: t[2], reverse=True)
    for path, path_depth, path_score in result:
        print("{:.4f}, {}, {}".format(
            path_score, path_depth, path
        ))

    manager.close()


