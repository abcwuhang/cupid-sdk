/* 
 * Copyright (C) 2022  ST-Lab
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.urbcomp.cupid.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.avatica.util.JsonUtil;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.urbcomp.cupid.db.config.DynamicConfig;
import org.urbcomp.cupid.db.model.roadnetwork.RoadNetwork;
import org.urbcomp.cupid.db.model.roadnetwork.RoadSegment;
import org.urbcomp.cupid.db.model.sample.ModelGenerator;
import org.urbcomp.cupid.db.model.trajectory.Trajectory;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.jts.geom.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class ConnectRemoteServer {
    private static void test_query_simple(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select 1+2 as s");
        rs.next();
        System.out.println(rs.getString(1));
        if (!"3".equals(rs.getString(1))) throw new Exception("Wrong!");
        stmt.close();
    }

    private static void test_query_noiseFilter1(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt"
        );
        String tGeo = trajectoryStp.toGeoJSON();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "select st_traj_noiseFilter(st_traj_fromGeoJSON(\'" + tGeo + "\')," + "1) as aaa"
        );
        rs.next();
        Object o = rs.getObject(1);
        System.out.println(o.getClass());
        System.out.println(rs.getObject(1).toString());
        System.out.println(
            ((Trajectory) o).getGPSPointList().equals(trajectoryStp.getGPSPointList().subList(0, 1))
        );
        stmt.close();
    }

    private static void test_query_noiseFilter2(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt"
        );
        String tGeo = trajectoryStp.toGeoJSON();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "select st_traj_asGeoJSON(st_traj_noiseFilter(st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'),"
                + "8.5)) as aaa"
        );
        rs.next();
        Object o = rs.getObject(1);
        System.out.println(o.getClass());
        System.out.println(rs.getObject(1).toString());
        System.out.println(Trajectory.fromGeoJSON(o.toString()).getGPSPointList());
        System.out.println(
            Trajectory.fromGeoJSON(o.toString())
                .getGPSPointList()
                .toString()
                .equals(
                    "[POINT (108.99553 34.27859), POINT (108.99552 34.27822), POINT (108.99552 34.27822), POINT (108.99552 34.27822), POINT (108.99552 34.27822), POINT (108.99552 34.27822), POINT (108.99598 34.25819), POINT (108.99433 34.25818), POINT (108.99391 34.25818), POINT (108.99337 34.25817), POINT (108.99312 34.25817), POINT (108.99287 34.25817), POINT (108.9926 34.25816), POINT (108.99245 34.25816), POINT (108.9923 34.25816), POINT (108.99205 34.25815)]"
                )
        );
        stmt.close();
    }

    private static void test_query_geomesa_type(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            "select st_translate(st_makePoint(1, 2), 1, 1) as a, st_translate(st_makeBBox(1, 2, 3, 4), 1, 1) as b"
        );
        rs.next();
        Object o = rs.getObject(1);
        System.out.println(o.getClass() + " " + rs.getMetaData().getColumnClassName(1));
        System.out.println(rs.getObject(1).toString());
        System.out.println(rs.getObject(2).toString());
        stmt.close();
    }

    private static void test_create_table(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test2");
        stmt.executeUpdate("CREATE TABLE test2 (name String, class String)");
        stmt.executeUpdate("insert into table test2 values ('Java', 'Hadoop scala')");
        stmt.executeUpdate("insert into table test2 values ('Python', 'Hadoop kafka')");
        stmt.executeUpdate("insert into table test2 values ('www', 'spark hive sqoop')");
        ResultSet rs = stmt.executeQuery("select name, class from test2");
        while (rs.next()) {
            Object o = rs.getObject(1);
            System.out.println(o.getClass());
            System.out.println(rs.getObject(1).toString());
            System.out.println(rs.getObject(2).toString());
        }
        stmt.close();
    }

    private static void test_insert_table1(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt",
            10
        );
        String tGeo = trajectoryStp.toGeoJSON();
        RoadSegment roadSegment = ModelGenerator.generateRoadSegment();
        String rsGeoJson = roadSegment.toGeoJSON();
        RoadNetwork roadNetwork = ModelGenerator.generateRoadNetwork(10);
        String rnGeoJson = roadNetwork.toGeoJSON();
        Statement stmt = conn.createStatement();
        // stmt.executeUpdate("DROP TABLE IF EXISTS test3");
        // stmt.executeUpdate("CREATE TABLE test3 (tr Trajectory, rs RoadSegment, rn RoadNetwork,
        // SPATIAL INDEX (tr) TYPE XZ2T, SPATIAL INDEX (tr) TYPE XZStarT)");
        /*stmt.executeUpdate(
            "insert into table test3 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        /*stmt.executeUpdate(
            "insert into table test4 values (st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        /*stmt.executeUpdate(
            "insert into table test3 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        ResultSet rs = stmt.executeQuery(
            "select tr, rs, rn from test3 where "
                + "st_within(st_traj_geom(tr), st_makebbox(108, 34, 109, 35)) and "
                + "st_traj_startTime(tr) > toTimestamp('2018-10-09 07:28:20')"
        );
        /*ResultSet rs = stmt.executeQuery("select traj, rs, rn from test3 where " +
                "st_traj_starttime(traj) > '2018-10-09 07:28:20' and st_traj_starttime(traj) < '2018-10-09 07:28:20'");*/
        while (rs.next()) {
            System.out.println(rs.getObject(1).getClass());
            System.out.println(rs.getObject(2).getClass());
            System.out.println(rs.getObject(3).getClass());
            System.out.println(rs.getObject(1, Trajectory.class).toString());
            System.out.println(rs.getObject(2, RoadSegment.class).toString());
            System.out.println(RoadNetwork.fromGeoJSON(rs.getObject(3).toString()));
            String s = rs.getObject(1, Trajectory.class).toString();
            if (!rs.getObject(1, Trajectory.class).toString().equals(tGeo)) {
                System.out.println("DIFF traj!");
                System.out.println(tGeo);
                for (int i = 0; i < s.length(); ++i) {
                    if (s.charAt(i) != tGeo.charAt(i)) {
                        System.out.println(i);
                    }
                }
            }
            if (!rs.getObject(2, RoadSegment.class).toString().equals(rsGeoJson)) {
                System.out.println("DIFF rs!");
            }
        }
        stmt.close();
    }

    private static void test_insert_table2(Connection conn) throws Exception {
        Trajectory trajectoryStp_1 = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt",
            10
        );
        Trajectory trajectoryStp_2 = ModelGenerator.generateTrajectory(
            "data/stayPointTraj.txt",
            10
        );
        String tGeo_1 = trajectoryStp_1.toGeoJSON();
        String tGeo_2 = trajectoryStp_2.toGeoJSON();
        String rsGeoJson_1 = ModelGenerator.generateRoadSegment().toGeoJSON();
        String rsGeoJson_2 = ModelGenerator.generateRoadSegment_2().toGeoJSON();
        RoadNetwork roadNetwork = ModelGenerator.generateRoadNetwork(10);
        String rnGeoJson = roadNetwork.toGeoJSON();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test_runbo");
        stmt.executeUpdate(
            "CREATE TABLE test_runbo (rs1 RoadSegment, rs2 RoadSegment, tr1 Trajectory, tr2 Trajectory, rn RoadNetwork)"
        );
        stmt.executeUpdate(
            "insert into table test_runbo values (st_rs_fromGeoJSON(\'"
                + rsGeoJson_1
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson_1
                + "\'), st_traj_fromGeoJSON(\'"
                + tGeo_1
                + "\'), st_traj_fromGeoJSON(\'"
                + tGeo_2
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );
        // ResultSet rs = stmt.executeQuery(
        // "select tr, rs, rn from test3 where "
        // + "st_within(st_traj_geom(tr), st_makebbox(108, 34, 109, 35)) and "
        // + "st_traj_startTime(tr) > toTimestamp('2018-10-09 07:28:20')"
        // );
        // ResultSet rs = stmt.executeQuery(
        // "select tr, rs, rn from test3 where "
        // + "st_within(st_traj_geom(tr), st_makebbox(st_makepoint(108, 34), st_makepoint(109, 35)))
        // and "
        // + "st_traj_startTime(tr) > toTimestamp('2018-10-09 07:28:20')"
        // );
        /* ResultSet rs = stmt.executeQuery("select traj, rs, rn from test3 where " +
                "st_traj_starttime(traj) > '2018-10-09 07:28:20' and st_traj_starttime(traj) < '2018-10-09 07:28:20'");*/
        // while (rs.next()) {
        // System.out.println(rs.getObject(1).getClass());
        // System.out.println(rs.getObject(2).getClass());
        // System.out.println(rs.getObject(3).getClass());
        // System.out.println(rs.getObject(1).toString());
        // System.out.println(rs.getObject(2).toString());
        // System.out.println(rs.getObject(3).toString());
        // }
        stmt.close();
    }

    private static void test_insert_table3(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt",
            10
        );
        String tGeo = trajectoryStp.toGeoJSON();
        RoadSegment roadSegment = ModelGenerator.generateRoadSegment();
        String rsGeoJson = roadSegment.toGeoJSON();
        RoadNetwork roadNetwork = ModelGenerator.generateRoadNetwork(10);
        String rnGeoJson = roadNetwork.toGeoJSON();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS geom");
        stmt.executeUpdate("CREATE TABLE test3 (tr Trajectory, rs RoadSegment, rn RoadNetwork)");
        stmt.executeUpdate(
            "insert into table test3 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );
        /*stmt.executeUpdate(
            "insert into table test3 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        // ResultSet rs = stmt.executeQuery("select tr, rs, rn from test3 where rn == \'aaa\'");
        // /*ResultSet rs = stmt.executeQuery(
        // "select _iso-8859-1\'afab91fa68cb417c2f663924a0ba1ff9\'"
        // );*/
        // /*ResultSet rs = stmt.executeQuery("select traj, rs, rn from test3 where " +
        // "st_traj_starttime(traj) > '2018-10-09 07:28:20' and st_traj_starttime(traj) <
        // '2018-10-09 07:28:20'");*/
        // while (rs.next()) {
        // System.out.println(rs.getObject(1).getClass());
        // System.out.println(rs.getObject(2).getClass());
        // System.out.println(rs.getObject(3).getClass());
        // System.out.println(rs.getObject(1).toString());
        // System.out.println(rs.getObject(2).toString());
        // System.out.println(rs.getObject(3).toString());
        // }
        stmt.close();
    }

    private static void test_insert_table4(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt",
            10
        );
        String tGeo = trajectoryStp.toGeoJSON();
        RoadSegment roadSegment = ModelGenerator.generateRoadSegment();
        String rsGeoJson = roadSegment.toGeoJSON();
        RoadNetwork roadNetwork = ModelGenerator.generateRoadNetwork(10);
        String rnGeoJson = roadNetwork.toGeoJSON();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test4");
        stmt.executeUpdate(
            "CREATE TABLE test4 (tr Trajectory, rs RoadSegment, rn RoadNetwork, SPATIAL INDEX (rs) TYPE XZ2, SPATIAL INDEX (rs) TYPE XZStar)"
        );
        stmt.executeUpdate(
            "insert into table test4 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );
        /*stmt.executeUpdate(
            "insert into table test4 values (st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        /*stmt.executeUpdate(
            "insert into table test3 values (st_traj_fromGeoJSON(\'"
                + tGeo
                + "\'), st_rs_fromGeoJSON(\'"
                + rsGeoJson
                + "\'), st_rn_fromGeoJSON(\'"
                + rnGeoJson
                + "\'))"
        );*/
        ResultSet rs = stmt.executeQuery("select tr, rs, rn from test4");
        /*ResultSet rs = stmt.executeQuery("select traj, rs, rn from test3 where " +
                "st_traj_starttime(traj) > '2018-10-09 07:28:20' and st_traj_starttime(traj) < '2018-10-09 07:28:20'");*/
        while (rs.next()) {
            System.out.println(rs.getObject(1).getClass());
            System.out.println(rs.getObject(2).getClass());
            System.out.println(rs.getObject(3).getClass());
            System.out.println(rs.getObject(1, Trajectory.class).toString());
            System.out.println(rs.getObject(2, RoadSegment.class).toString());
            System.out.println(RoadNetwork.fromGeoJSON(rs.getObject(3).toString()));
            String s = rs.getObject(1, Trajectory.class).toString();
            if (!rs.getObject(1, Trajectory.class).toString().equals(tGeo)) {
                System.out.println("DIFF traj!");
                System.out.println(tGeo);
                for (int i = 0; i < s.length(); ++i) {
                    if (s.charAt(i) != tGeo.charAt(i)) {
                        System.out.println(i);
                    }
                }
            }
            if (!rs.getObject(2, RoadSegment.class).toString().equals(rsGeoJson)) {
                System.out.println("DIFF rs!");
            }
        }
        stmt.close();
    }

    private static void test_insert_table11(Connection conn) throws Exception {
        Trajectory trajectoryStp = ModelGenerator.generateTrajectory(
            "data/stayPointSegmentationTraj.txt",
            10
        );
        String tGeo = trajectoryStp.toGeoJSON();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS test4");
        stmt.executeUpdate("CREATE TABLE test4 (idx int, ppqq point)");
        stmt.executeUpdate("insert into table test4 values (1, st_makePoint(1, 2.73))");
        stmt.executeUpdate("insert into table test4 values (2, st_makePoint(3, 4))");
        ResultSet rs = stmt.executeQuery("select idx, ppqq from test4");
        while (rs.next()) {
            Object o = rs.getObject(2);
            System.out.println(o.getClass());
            System.out.println(rs.getObject(1).toString());
            System.out.println(rs.getObject(2).toString());
        }
        stmt.close();
    }

    private static void test_true_trajectory(
        Connection conn,
        String trajectoryPath,
        String tableName,
        boolean createTable
    ) throws Exception {
        File inputFile = new File(trajectoryPath);
        Statement stmt;
        if (createTable) {
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            // stmt.executeUpdate("CREATE TABLE " + tableName + " (traj Trajectory)");
            stmt.executeUpdate(
                "CREATE TABLE "
                    + tableName
                    + " (traj Trajectory, SPATIAL INDEX (traj) TYPE XZ2T, SPATIAL INDEX (traj) TYPE XZStarT)"
            );
            stmt.close();
        }
        try (
            InputStream in = new FileInputStream(inputFile);
            BufferedReader inBur = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(in))
            )
        ) {
            String tGeo;
            int numTrajectories = 0;
            while ((tGeo = inBur.readLine()) != null) {
                stmt = conn.createStatement();
                stmt.executeUpdate(
                    "insert into table "
                        + tableName
                        + " values (st_traj_fromGeoJSON(\'"
                        + tGeo
                        + "\'))"
                );
                numTrajectories += 1;
                System.out.println("Trajectory " + numTrajectories + " finished");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static void fast_truncate(Connection conn, String tableName) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("TRUNCATE TABLE " + tableName);
        stmt.close();
    }

    private static void fast_drop(Connection conn, String tableName) throws Exception {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        stmt.close();
    }

    private static void test_true_trajectory2(
        Connection conn,
        String trajectoryPath,
        String tableName,
        boolean createTable
    ) throws Exception {
        Statement stmt;
        if (createTable) {
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            // stmt.executeUpdate("CREATE TABLE " + tableName + " (traj Trajectory)");
            stmt.executeUpdate(
                "CREATE TABLE "
                    + tableName
                    + " (traj Trajectory, SPATIAL INDEX (traj) TYPE XZ2T, SPATIAL INDEX (traj) TYPE XZStarT)"
            );
            stmt.close();
        }

        /*try (
                InputStream in = new FileInputStream(new File(trajectoryPath));
                BufferedReader inBur = new BufferedReader(
                        new InputStreamReader(Objects.requireNonNull(in))
                )
        ) {
            int numTrajectory = 0;
            String tGeo;
            while ((tGeo = inBur.readLine()) != null) {
                stmt.executeUpdate(
                        "insert into table " + tableName + " values (st_traj_fromGeoJSON(\'"
                                + tGeo
                                + "\'))"
                );
                numTrajectory += 1;
                if (numTrajectory % 1000 == 0) {
                    System.out.println(numTrajectory + " finished...");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        for (int i = 0; i < 1; ++i) {
            stmt = conn.createStatement();
            stmt.executeUpdate(
                "LOAD CSV INPATH \""
                    + trajectoryPath
                    + "\" TO "
                    + tableName
                    + " (traj st_traj_fromGeoJSON(_c0)) FIELDS DELIMITER \"!\" QUOTES '\"' WITHOUT HEADER"
            );
            stmt.close();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) as cnt from " + tableName);
            while (rs.next()) {
                System.out.println(rs.getObject(1).toString());
            }
            System.out.println("Insert " + i + " times finished");
            stmt.close();
        }
        /*stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) as cnt from " + tableName);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }
        stmt.close();*/
    }

    private static void test_insert_select(Connection conn, String tableName, boolean createTable)
        throws Exception {
        Statement stmt;
        if (createTable) {
            stmt = conn.createStatement();
            stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            // stmt.executeUpdate("CREATE TABLE " + tableName + " (traj Trajectory)");
            stmt.executeUpdate("CREATE TABLE " + tableName
            // + " (traj Trajectory, tid String, geom Geometry, ti Timestamp, SPATIAL INDEX (traj)
            // TYPE XZ2T, SPATIAL INDEX (traj) TYPE XZStarT)"
                + " (tid String, starttime Timestamp, startpoint Point, endpoint Point, matchresult String)"
            );
            stmt.close();
        }
        for (int i = 0; i < 1; ++i) {
            /*stmt = conn.createStatement();
            stmt.executeUpdate(
                    "INSERT INTO TABLE " + tableName + " select traj, st_traj_tid(traj) as tid, st_traj_geom(traj) as geom, st_traj_starttime(traj) as ti from small_testcase"
            );
            stmt.close();*/
            stmt = conn.createStatement();
            // ResultSet rs = stmt.executeQuery("select traj, st_traj_tid(traj) as tid,
            // st_traj_geom(traj) as geom, st_traj_starttime(traj) as ttt from " + tableName
            // + " where st_traj_starttime(traj) >= toTimestamp('2022-06-13 00:00:00') and
            // st_traj_starttime(traj) <= toTimestamp('2022-06-19 23:59:59') limit 2");
            String mapMatchSQL = "select count(*) as cnt from "
                + tableName
                + " where "
                + "st_intersects(st_traj_geom(traj), st_makebbox(st_makepoint(113.261, 23.123), st_makepoint(113.27, 23.13))) and "
                + "st_traj_starttime(traj) >= toTimestamp('2022-05-31 00:00:00') and st_traj_starttime(traj) <= toTimestamp('2022-06-02 23:59:59')";
            /*String mapMatchSQL =
                " select st_traj_tid(table2.traj) as tid, st_traj_starttime(table2.traj) as starttime, st_traj_startPoint(table2.traj) as start_point, st_traj_endPoint(table2.traj) as end_point,"
                    + " st_traj_dynamicDPMatchToTrajectory(table2.traj, 'road_segment_table_index', 100) as matched_rs from "
                    + "xiong_temp_traj_table as table2";*/
            /*String filterRoadSegmentSQL = " select distinct(table1.road_segment) as road_segment from xiong_temp_traj_table as table2, road_segment_table as table1 "
                    + "where st_distanceSpheroid(st_rs_geom(table1.road_segment), st_traj_geom(table2.traj)) < 5000";*/
            /*stmt.executeUpdate(mapMatchSQL);
            stmt.close();
            stmt = conn.createStatement();
            mapMatchSQL = "select count(*) as cnt from " + tableName;*/
            ResultSet rs = stmt.executeQuery(mapMatchSQL);
            // String mapMatchSQL = "select st_traj_tid(traj) as tid, st_traj_starttime(traj) as
            // starttime from small_testcase";
            /*String mapMatchSQL =
                "select st_traj_tid(table2.traj) as tid, st_traj_startPoint(table2.traj) as start_point, st_traj_endPoint(table2.traj) as end_point,"
                    + " st_traj_mapMatch(table1.rn_result, table2.traj) as matched_rs from "
                    + "(select st_rn_makeRoadNetwork(collect_list(road_segment)) as rn_result from road_segment_table) as table1, "
                    + "(select traj from small_testcase where "
                    + "st_traj_starttime(traj) >= toTimestamp('2022-05-31 00:00:00') and st_traj_starttime(traj) <= toTimestamp('2022-06-02 23:59:59')) as table2";
            ResultSet rs = stmt.executeQuery(mapMatchSQL);*/
            while (rs.next()) {
                System.out.println(rs.getObject(1).toString());
                System.out.println(rs.getObject(2).toString());
                System.out.println(rs.getObject(3).toString());
                System.out.println(rs.getObject(4).toString());
                System.out.println(rs.getObject(5).toString());
            }
            System.out.println("Insert " + i + " times finished");
            stmt.close();
        }
        /*stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) as cnt from " + tableName);
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }
        stmt.close();*/
    }

    private static void test_show_tables(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("show tables");
        while (rs.next()) {
            System.out.println(rs.getObject(1).toString());
        }
        stmt.close();
    }

    private static void test_remote_hdfs() throws Exception {
        final Properties conf = new Properties();
        conf.put("user", "root");
        conf.put("password", "cupid-db");
        conf.put("plainTextPassword", "true");
        conf.put("engine", "spark_local");
        conf.put("flowId", "flow1");
        conf.put("spark.local", "true");
        conf.put("spark.async", "false");
        conf.put("spark.exportType", "cache");
        conf.put("spark.useIndex", "true");
        conf.put("spark.fetchLimit", "-1");
        try (
            Connection conn = DriverManager.getConnection(
                "jdbc:cupid-db:url=http://localhost:8009;db=default",
                conf
            )
        ) {
            //test_query_simple(conn);
            // test_query_noiseFilter1(conn);
            // test_query_noiseFilter2(conn);
            // test_query_geomesa_type(conn);

            // test_create_table(conn);
            // test_insert_table1(conn);
            // test_insert_table2(conn);
            // test_insert_table3(conn);
            // test_insert_table4(conn);
            // test_push_rs_filter(conn);
            // test_true_trajectory(conn);
            test_insert_table11(conn);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        test_remote_hdfs();
    }
}
