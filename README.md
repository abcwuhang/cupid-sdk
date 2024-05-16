# cupid-sdk
This is a demo SDK of Cupid, which runs on a single machine. We are working on releasing distributed versions of Cupid SDK.

Download the cupid-db_encrypt.jar and libdecrypt.so from https://drive.google.com/drive/folders/1gc0lZBMEzODX3CcVxF3aSob9iF0FRSb8?usp=drive_link.

To run the sdk, you need to install MySQL (version >= 5.7), Hadoop (version 2.7.1) and HBase (version 2.2.7).

Run ddl.sql to insert all necessary tables and data into MySQL.

Put cupid.conf into the directory hdfs://opt/spark-apps

Add libdecrypt.so to LD_LIBRARY_PATH environment variable

Then run "java -agentlib:decrypt -jar cupid-db.jar org.urbcomp.cupid.db.server.CupidDbServerStart" to start the Cupid server.

You can compile ConnectRemoteServer.java with the cupid-db.jar to run Cupid.
