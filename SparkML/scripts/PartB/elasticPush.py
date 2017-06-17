from datetime import datetime
from elasticsearch import Elasticsearch
import time

if __name__ == "__main__":
    txt = open("output.txt").read()
    parts = txt.split("-------------------------------------------")
    valid = ''
    for s in parts:
        if len(s) > 30:
            s = s.replace('\n','')
            valid += s
    valid = valid.replace('\'}{\'','\'},{\'').split(',{')
    # valid = json.loads(valid)
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    # mappings = {
    #     'doc': {
    #         'properties': {
    #             'geo': {
    #                 'properties': {
    #                     'location': {
    #                         'type': 'geo_point'
    #                     }
    #                 }
    #             },
    #             'time_stamp': {
    #                 'type': 'date'
    #             }
    #         }
    #     }
    # }
    # es.indices.create(index='tweetfeed4-', body=mappings)
    #es_entries['geo'] = {'location': str(data['_longitude_']) + "," + str(data['_latitude_'])}
    #es.index(index="geodata", doc_type="doc", body=es_entries)
    for s in valid:
        t = s.split(', \'')
        # print s
        # print '0 --> ' + t[0]
        # print '1 --> ' + t[1]
        # print '2 --> ' + t[2]
        # print '3 --> ' + t[3]
        # print '4 --> ' + t[4]
        # print '5 --> ' + t[5]
        # print '6 --> ' + t[6]
        a = {}
        a["hash_tag"] = t[0].replace('\'','').replace('hash_tag: u','')
        a["sentiment"] = t[1].replace('\'','').replace('lavel: ','')
        # l = {}
        # l['lat'] = float(t[5].replace('\'','').replace('latitude: ',''))
        # l['lon'] = float(t[3].replace('\'','').replace('longitude: ',''))
        #a['geo'] = {'location': str(t[3].replace('\'','').replace('longitude: ','')) + "," + str(t[5].replace('\'','').replace('latitude: ',''))}
        l_s = str(t[5].replace('\'','').replace('latitude: ','')) + "," + str(t[3].replace('\'','').replace('longitude: ',''))
        a["location"] = l_s
        #a['time_stamp'] = datetime.datetime.fromtimestamp(long(t[4].replace('\'','').replace('.0','').replace('created_time: ',''))).strftime('%Y-%m-%d %H:%M:%S')
        #a['time_stamp'] = datetime.fromtimestamp(long(t[4].replace('\'','').replace('.0','').replace('created_time: ','')))
        #time.sleep(1)
        a["id"] = str(t[6].replace('\'','').replace('}','').replace('_id: u',''))
        print "curl -XPUT http://localhost:9200/tweetfeed8-/doc/"+a["id"]+" -d %"
        print a
        print "%"
        # es.index(index='tweetfeed8-', doc_type='doc', id=a["id"], body=a)