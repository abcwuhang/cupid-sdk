import requests
import json
from urllib import parse
import websockets
import asyncio
import pandas as pd
import string
import random

class Cupid:
    def __init__(self, conf):
        self.conf = conf
        self.url = 'ws://' + self.conf['ip'] + ':' + str(self.conf['port'])
        self.params = {
            'jdbc': self.conf['jdbc'],
            'user': self.conf['user'],
            'password': self.conf['password'],
            'plainTextPassword': self.conf['plainTextPassword'],
            'engine': self.conf['engine'],
            'properties': self.conf.get('properties', '')
        }
    async def connect(self):
        self.websocket = await websockets.connect(self.url, ping_interval=None, max_size=300000000)

    async def sql(self, query, flowId=None, convertToDataFrame=False, params = None):
        if params is None:
            params = self.params.copy()
        if flowId is None:
            params['flowId'] = "_flow"
        else:
            params['flowId'] = flowId
        params['requestId'] = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        params['sql'] = parse.quote(str(query), encoding='UTF-8')
        payload = json.dumps(params)
        await self.websocket.send(payload)
        response = await self.websocket.recv()
        result = json.loads(str(response))
        if result['exception'] is not None:
            print(result['exception'])
        if convertToDataFrame:
            df = pd.DataFrame(result['results'], columns=result['columnNames'])
            return df
        else:
            return result

    async def close(self):
        if self.websocket:
            await self.websocket.close()

tGeo = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"time":"2018-10-09 07:28:21.0"},"geometry":{"type":"Point","coordinates":[108.99553,34.27859]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:22.0"},"geometry":{"type":"Point","coordinates":[108.99028,34.25814]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:24.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:27.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:30.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:33.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:36.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:39.0"},"geometry":{"type":"Point","coordinates":[108.99552,34.27822]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:42.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.2759]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:45.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.27552]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:48.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.27514]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:51.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.27476]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:54.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.2744]}},{"type":"Feature","properties":{"time":"2018-10-09 07:28:57.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.27407]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:00.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.27375]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:03.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.27354]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:06.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.27343]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:10.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.27323]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:12.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.27303]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:15.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.27279]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:18.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.27257]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:21.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.27233]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:24.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.27206]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:27.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.27177]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:30.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.2715]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:33.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.27122]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:36.0"},"geometry":{"type":"Point","coordinates":[108.99545,34.27093]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:39.0"},"geometry":{"type":"Point","coordinates":[108.99545,34.27068]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:42.0"},"geometry":{"type":"Point","coordinates":[108.99545,34.27054]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:45.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.2703]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:48.0"},"geometry":{"type":"Point","coordinates":[108.99545,34.27007]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:51.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.2698]}},{"type":"Feature","properties":{"time":"2018-10-09 07:29:57.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.26946]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:01.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:03.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:06.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:09.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:12.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:15.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:18.0"},"geometry":{"type":"Point","coordinates":[108.9955,34.26731]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:21.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.26714]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:24.0"},"geometry":{"type":"Point","coordinates":[108.9955,34.26707]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:27.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.26704]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:30.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.26691]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:33.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.26675]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:36.0"},"geometry":{"type":"Point","coordinates":[108.99549,34.26662]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:39.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.26644]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:42.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.26623]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:45.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.26607]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:48.0"},"geometry":{"type":"Point","coordinates":[108.99548,34.26591]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:51.0"},"geometry":{"type":"Point","coordinates":[108.99547,34.2657]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:54.0"},"geometry":{"type":"Point","coordinates":[108.99544,34.26549]}},{"type":"Feature","properties":{"time":"2018-10-09 07:30:57.0"},"geometry":{"type":"Point","coordinates":[108.99543,34.26528]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:00.0"},"geometry":{"type":"Point","coordinates":[108.99543,34.26506]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:03.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26484]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:06.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26467]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:09.0"},"geometry":{"type":"Point","coordinates":[108.9954,34.2645]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:12.0"},"geometry":{"type":"Point","coordinates":[108.9954,34.26429]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:15.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.26406]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:18.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.2638]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:21.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26349]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:24.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26318]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:27.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26286]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:30.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.26256]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:40.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.2623]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:42.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.26158]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:46.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.26137]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:48.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.26125]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:51.0"},"geometry":{"type":"Point","coordinates":[108.99541,34.26115]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:54.0"},"geometry":{"type":"Point","coordinates":[108.99539,34.26104]}},{"type":"Feature","properties":{"time":"2018-10-09 07:31:57.0"},"geometry":{"type":"Point","coordinates":[108.99539,34.26094]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:00.0"},"geometry":{"type":"Point","coordinates":[108.9954,34.26079]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:03.0"},"geometry":{"type":"Point","coordinates":[108.99542,34.2606]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:06.0"},"geometry":{"type":"Point","coordinates":[108.99543,34.2605]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:09.0"},"geometry":{"type":"Point","coordinates":[108.99543,34.2604]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:12.0"},"geometry":{"type":"Point","coordinates":[108.99546,34.26022]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:15.0"},"geometry":{"type":"Point","coordinates":[108.99556,34.26005]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:18.0"},"geometry":{"type":"Point","coordinates":[108.99564,34.25998]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:21.0"},"geometry":{"type":"Point","coordinates":[108.99574,34.25991]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:24.0"},"geometry":{"type":"Point","coordinates":[108.99597,34.25977]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:27.0"},"geometry":{"type":"Point","coordinates":[108.99618,34.2596]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:30.0"},"geometry":{"type":"Point","coordinates":[108.99634,34.25938]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:33.0"},"geometry":{"type":"Point","coordinates":[108.99643,34.25919]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:36.0"},"geometry":{"type":"Point","coordinates":[108.99651,34.25904]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:39.0"},"geometry":{"type":"Point","coordinates":[108.99655,34.25891]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:42.0"},"geometry":{"type":"Point","coordinates":[108.99657,34.25875]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:45.0"},"geometry":{"type":"Point","coordinates":[108.99655,34.25857]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:48.0"},"geometry":{"type":"Point","coordinates":[108.99654,34.25837]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:51.0"},"geometry":{"type":"Point","coordinates":[108.99652,34.25826]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:54.0"},"geometry":{"type":"Point","coordinates":[108.99647,34.25821]}},{"type":"Feature","properties":{"time":"2018-10-09 07:32:57.0"},"geometry":{"type":"Point","coordinates":[108.99639,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:00.0"},"geometry":{"type":"Point","coordinates":[108.99624,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:03.0"},"geometry":{"type":"Point","coordinates":[108.99598,34.25819]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:06.0"},"geometry":{"type":"Point","coordinates":[108.99568,34.2582]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:07.0"},"geometry":{"type":"Point","coordinates":[108.99536,34.2582]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:22.0"},"geometry":{"type":"Point","coordinates":[108.99433,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:24.0"},"geometry":{"type":"Point","coordinates":[108.99414,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:27.0"},"geometry":{"type":"Point","coordinates":[108.99391,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:30.0"},"geometry":{"type":"Point","coordinates":[108.99363,34.25818]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:33.0"},"geometry":{"type":"Point","coordinates":[108.99337,34.25817]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:36.0"},"geometry":{"type":"Point","coordinates":[108.99312,34.25817]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:39.0"},"geometry":{"type":"Point","coordinates":[108.99287,34.25817]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:42.0"},"geometry":{"type":"Point","coordinates":[108.9926,34.25816]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:45.0"},"geometry":{"type":"Point","coordinates":[108.99245,34.25816]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:48.0"},"geometry":{"type":"Point","coordinates":[108.9923,34.25816]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:51.0"},"geometry":{"type":"Point","coordinates":[108.99205,34.25815]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:54.0"},"geometry":{"type":"Point","coordinates":[108.99171,34.25816]}},{"type":"Feature","properties":{"time":"2018-10-09 07:33:57.0"},"geometry":{"type":"Point","coordinates":[108.99133,34.25815]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:00.0"},"geometry":{"type":"Point","coordinates":[108.99099,34.25815]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:03.0"},"geometry":{"type":"Point","coordinates":[108.99066,34.25814]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:06.0"},"geometry":{"type":"Point","coordinates":[108.99028,34.25814]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:12.0"},"geometry":{"type":"Point","coordinates":[108.98954,34.25815]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:16.0"},"geometry":{"type":"Point","coordinates":[108.98919,34.25815]}},{"type":"Feature","properties":{"time":"2018-10-09 07:34:18.0"},"geometry":{"type":"Point","coordinates":[108.98897,34.25815]}}],"properties":{"oid":"afab91fa68cb417c2f663924a0ba1ff9","tid":"afab91fa68cb417c2f663924a0ba1ff92018-10-09 07:28:21.0"}}'
rsGeoJson = '{"type":"Feature","properties":{"endId":2,"level":6,"startId":1,"rsId":1,"speedLimit":30.0,"lengthInMeter":120.0,"direction":1},"geometry":{"type":"LineString","coordinates":[[111.37939453125,54.00776876193478],[116.3671875,53.05442186546102]]}}'
rnGeoJson = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"endId":70832,"level":3,"startId":70831,"rsId":100320,"speedLimit":50.0,"lengthInMeter":50.57964091335451,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.88093234592,34.1634619140625],[108.881090494792,34.1630268012153]]}},{"type":"Feature","properties":{"endId":70833,"level":3,"startId":70831,"rsId":100321,"speedLimit":50.0,"lengthInMeter":75.42983669528238,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.88093234592,34.1634619140625],[108.88098171658,34.1634822591146],[108.88104031033,34.1635129123264],[108.881092664931,34.1635579427083],[108.881158854167,34.1636238606771],[108.881283365885,34.1637198893229],[108.881476508247,34.1638031684028],[108.881594509549,34.1638427734375]]}},{"type":"Feature","properties":{"endId":70087,"level":3,"startId":70834,"rsId":100322,"speedLimit":50.0,"lengthInMeter":181.41968478264772,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.880109049479,34.1634261067708],[108.880099555122,34.1628287760417],[108.880110134549,34.1625740559896],[108.880116102431,34.1617966037326]]}},{"type":"Feature","properties":{"endId":71788,"level":3,"startId":71787,"rsId":102037,"speedLimit":50.0,"lengthInMeter":67.62631622075392,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.885338812934,34.1605669487847],[108.886017795139,34.1605604383681],[108.886072319878,34.1605672200521]]}},{"type":"Feature","properties":{"endId":71787,"level":3,"startId":72371,"rsId":103346,"speedLimit":50.0,"lengthInMeter":126.85546683095794,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.88536702474,34.1617062717014],[108.885338812934,34.1605669487847]]}},{"type":"Feature","properties":{"endId":71171,"level":2,"startId":73654,"rsId":105445,"speedLimit":70.0,"lengthInMeter":204.97597228211302,"direction":2},"geometry":{"type":"LineString","coordinates":[[108.892858344184,34.1622927517361],[108.892858072917,34.1623280164931],[108.892857801649,34.1623453776042],[108.892728135851,34.164130859375]]}},{"type":"Feature","properties":{"endId":72360,"level":3,"startId":73654,"rsId":105447,"speedLimit":50.0,"lengthInMeter":476.72811101136017,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.892858344184,34.1622927517361],[108.893034667969,34.1623008897569],[108.893322753906,34.1623033311632],[108.894059787326,34.1622943793403],[108.894883355035,34.1623000759549],[108.895741644965,34.1622995334201],[108.896628689236,34.1622875976563],[108.898033040365,34.1623136393229]]}},{"type":"Feature","properties":{"endId":31949,"level":3,"startId":31948,"rsId":10588,"speedLimit":50.0,"lengthInMeter":58.90519348208171,"direction":1},"geometry":{"type":"LineString","coordinates":[[108.880155978733,34.1687548828125],[108.880172797309,34.1692838541667]]}},{"type":"Feature","properties":{"endId":39923,"level":4,"startId":39924,"rsId":158698,"speedLimit":30.0,"lengthInMeter":176.26899769796103,"direction":2},"geometry":{"type":"LineString","coordinates":[[108.869155911486,34.1825166935547],[108.869061709772,34.1825451958659],[108.868587082988,34.1825451958659],[108.867406761117,34.1825478723328],[108.867247909593,34.1825483695206]]}},{"type":"Feature","properties":{"endId":39931,"level":2,"startId":39932,"rsId":158707,"speedLimit":50.0,"lengthInMeter":18.04282824571213,"direction":2},"geometry":{"type":"LineString","coordinates":[[108.869147949219,34.1837820095486],[108.869148922878,34.1836199300661]]}}]}'

async def main():
    conf = {'ip': 'localhost', 'port': 8901, 'jdbc': 'jdbc:cupid-db:url=http://localhost:8009;db=default',
            'user': 'root', 'password': 'cupid-db', 'plainTextPassword': 'true', 'engine': 'spark_local',
            'properties': 'spark.local=true,spark.async=false,spark.exportType=hdfs,spark.useIndex=true,spark.fetchLimit=100'}
    cupid = Cupid(conf)
    await cupid.connect()

    #print(await cupid.sql('SHOW TABLES'))
    #print(await cupid.sql('CREATE TABLE IF NOT EXISTS test6 (v1 Int, v2 String)'))
    #print(await cupid.sql('INSERT INTO TABLE test6 values (1, \'aabc\')'))
    #print(await cupid.sql('SELECT * FROM test6'))

    #print(await cupid.sql('CREATE TABLE IF NOT EXISTS test8 (TR Trajectory, Rs RoadSegment, rn RoadNetwork, SPATIAL INDEX (tr) TYPE XZ2T, SPATIAL INDEX (tr) TYPE XZStarT)'))
    #print(await cupid.sql('CREATE TABLE IF NOT EXISTS test8_2 LIKE test8'))
    #print(await cupid.sql('TRUNCATE TABLE test8_2'))
    #print(await cupid.sql("INSERT INTO TABLE test8_2 select st_traj_fromGeoJSON('" + tGeo + "') as tr, st_rs_fromGeoJSON('" + rsGeoJson + "') as rs, st_rn_fromGeoJSON('" + rnGeoJson + "') as rn"))
    #print(await cupid.sql('SHOW CREATE TABLE test8_2'))
    #print(await cupid.sql('SELECT count(*) as cnt from test8'))
    #print(await cupid.sql('SELECT count(*) as cnt from test8_2'))
    print(await cupid.sql("SELECT * from test8_2 where st_intersects(st_traj_geom(TR), st_makebbox(st_makepoint(108.989, 34.258), st_makepoint(108.990, 34.259))) AND st_traj_starttime(TR) >= toTimestamp('2018-10-09 06:00:00') AND st_traj_starttime(TR) <= toTimestamp('2018-10-09 08:59:59');"))

    #print(await cupid.sql("select st_traj_noiseFilter(st_traj_fromGeoJSON('" + tGeo + "'), 1) as aaa, st_rs_fromGeoJSON('" + rsGeoJson + "') as a1, st_rn_fromGeoJSON('" + rnGeoJson + "') as v2"))
    await cupid.close()

asyncio.get_event_loop().run_until_complete(main())