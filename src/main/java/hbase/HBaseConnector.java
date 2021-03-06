package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class HBaseConnector {

    public static Configuration config;
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("f");
    public static final byte[] COLUMN_FAMILY1 = Bytes.toBytes("f1");

    final static String HbaseMaster1 = "ip-10-185-143-196.ap-northeast-1.compute.internal";
    final static String HbaseMaster2 = "ip-10-168-153-86.ap-northeast-1.compute.internal";
    //ip-10-132-128-131.ap-northeast-1.compute.internal


    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.zookeeper.quorum", HbaseMaster1 +","+HbaseMaster2);
        //configuration.set("hbase.master", "hdfs://ip-10-185-143-196.ap-northeast-1.compute.internal:60000");
        config.set("hbase.rootdir", "hdfs://nameservice1/hbase");
    }

    public static void main(String[] args) throws IOException {
        //getAllRecord("user_behavior_log");
        //SelectUserBehaviorLog();
        SelectItemReco();

    }

    private static void SelectItemReco() throws IOException {
        HTable hTable = new HTable(config, "item_reco");

        Scan scan = new Scan();
        scan.setMaxResultSize(10L);

//        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(myfeatureId)) );
//        filterList.addFilter(filter);

        final byte[] rowKey = new byte[] { (byte)0xe0, 0x4f, (byte)0xd0,
                0x20, (byte)0xea, 0x3a, 0x69, 0x10, (byte)0xa2, (byte)0xd8, 0x08, 0x00, 0x2b,
                0x30, 0x30, (byte)0x9d };


        SingleColumnValueFilter f1 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("udt"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("2016-07-03 00:00:00"));
        SingleColumnValueFilter f2 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("236"));
        SingleColumnValueFilter f3 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("168"));
        SingleColumnValueFilter f4 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("109"));
        SingleColumnValueFilter f5 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("110"));
        SingleColumnValueFilter f6 = new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("84"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        //scan.setTimeRange(1471255200000L, 1471600800000L);
        //scan.setFilter(new SingleColumnValueFilter(COLUMN_FAMILY1, Bytes.toBytes("sid"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("109")));
        filterList.addFilter(f1);
        //filterList.addFilter(f2);
        //filterList.addFilter(f3);
        //filterList.addFilter(f4);
        //filterList.addFilter(f5);
        //filterList.addFilter(f6);
        scan.setFilter(filterList);

        ResultScanner resultScanner = hTable.getScanner(scan);

        for (Result result : resultScanner) {
            byte[] rowKey2 = result.getRow().clone();
            //System.out.println("####################"+ Bytes.toString(rowKey2) + "##############"+);
            byte[] rowKey3 = new byte[] { (byte)rowKey2[0], (byte)rowKey2[1], (byte)rowKey2[2] };

            System.out.println("##################################################################################"+ Bytes.toString(rowKey3));
            for (KeyValue kv : result.raw()) {
                //System.out.println("Family - "+Bytes.toString(kv.getFamily()));
                //System.out.println("Qualifier - "+Bytes.toString(kv.getQualifier() ));
                //System.out.println("_______________________________");
                if(kv.toString().indexOf("f1:udt")> -1 || kv.toString().indexOf("f1:sid")> -1) {
                    //if(Bytes.toString(kv.getValue()).indexOf("2016-05") >-1 || Bytes.toString(kv.getValue()).indexOf("2016-06") >-1) {
                        //System.out.println("kv:" + kv);
                        //System.out.println("Key:"+Bytes.toString(kv.getKey()));
                        System.out.println("Value:" + Bytes.toString(kv.getValue()));
                    //}
                }
                //System.out.println("getRow : " +Bytes.toString(kv.getRow()));

            }

            /*System.out.println("0:"+r);
            System.out.println("1:" + r.getValue(COLUMN_FAMILY1, Bytes.toBytes("sid")));
            System.out.println("1:" + (r.getValue(COLUMN_FAMILY1, Bytes.toBytes("sid"))).toString());
            System.out.println("2:" + r.getValue(COLUMN_FAMILY1, Bytes.toBytes("iid")));
            System.out.println("3:" + r.getValue(COLUMN_FAMILY1, Bytes.toBytes("nid")));
            System.out.println("4:" + r.getValue(COLUMN_FAMILY1, Bytes.toBytes("cid")));*/

            //Integer sid = Bytes.toInt(r.getValue(COLUMN_FAMILY, Bytes.toBytes("18")));
            //int cid = Bytes.toInt(r.getValue(COLUMN_FAMILY, Bytes.toBytes("cid")));

            //System.out.println(String.format("sid : %d", sid));
            //System.out.println(String.format("sid : %d, ucd : %d", sid, cid));
        }
    }

    public static void SelectUserBehaviorLog() throws IOException {
        HTable hTable = new HTable(config, "user_behavior_log");

        Scan scan = new Scan();
        scan.setMaxResultSize(10L);
        //scan.setTimeRange(1464739200000L, 1464739260000L); //01 Jun 2016 00:00:00 GMT ~ 01 Jun 2016 00:01:00 GMT
        scan.setFilter(new SingleColumnValueFilter(COLUMN_FAMILY, Bytes.toBytes("sid"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(1628)));

        ResultScanner resultScanner = hTable.getScanner(scan);

        for (Result r : resultScanner) {
            int sid = Bytes.toInt(r.getValue(COLUMN_FAMILY, Bytes.toBytes("sid")));
            String uid = new String(r.getValue(COLUMN_FAMILY, Bytes.toBytes("uid")));
            String iid = new String(r.getValue(COLUMN_FAMILY, Bytes.toBytes("iid")));

            int aid = Bytes.toInt(r.getValue(COLUMN_FAMILY, Bytes.toBytes("aid")));
            String ref = new String(r.getValue(COLUMN_FAMILY, Bytes.toBytes("ref")));
            String guid = new String(r.getValue(COLUMN_FAMILY, Bytes.toBytes("guid")));
            String tag = new String(r.getValue(COLUMN_FAMILY, Bytes.toBytes("tag")));

            System.out.println(String.format("sid : %d, uid : %s, iid : %s, aid : %d, ref : %s, guid : %s, tag : %s",
                    sid, uid, iid, aid, ref, guid, tag));
        }
    }

    public static void QueryByCondition1(String tableName) {

        HTablePool pool = new HTablePool(config, 1000);

        HTable table = (HTable) pool.getTable(tableName);
        try {
            Get scan = new Get("abcdef".getBytes());// ??rowkey??
            Result r = table.get(scan);
            System.out.println("???rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("??" + new String(keyValue.getFamily())
                        + "====?:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(config, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow().toString()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {

        // 1624 -> toByte로 변환  -109, 39, -106, -112
        // 1628 -> toByte로 변환  -17, -73, 108, -1

        //final byte[] startPartialRowKey = new byte[] {-109, 39, -106, -112};
        final byte[] startPartialRowKey = new byte[] {-17, -73, 108, -1};
        //final byte[] startPartialRowKey = new byte[] {-13, 64, -15, -79}; // 241

        final byte[] stopPartialRowKey = Arrays.copyOfRange(startPartialRowKey, 0, Bytes.SIZEOF_INT);
        // -109, 39, -106, -111
        stopPartialRowKey[Bytes.SIZEOF_INT - 1]++;

        byte[] temp = new byte[1];

        byte[] beginRowKey = new byte[0];
        byte[] endRowKey = new byte[0];

        try{
            HTable table = new HTable(config, tableName);

             for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
             //for (int b = -128; b <= -128; b++) {
                temp[0] = (byte) b;
                beginRowKey = Bytes.add(temp, startPartialRowKey);
                endRowKey = Bytes.add(temp, stopPartialRowKey);

                 final String msg = String.format("[INFO] StartKey=%s\tStopKey=%s",
                         Arrays.toString(beginRowKey), Arrays.toString(endRowKey));
                 System.out.println(msg);

                // Scan scan = new Scan(beginRowKey);
                // 01 Jun 2016 00:00:00 GMT
                // 01 Jun 2016 01:00:00 GMT
                Scan scan = new Scan(beginRowKey, endRowKey);
                scan.setMaxResultSize(10L);
                scan.setTimeRange(1464739200000L, 1464742800000L);

                ResultScanner resultScanner = table.getScanner(scan);

                for (Result result : resultScanner) {
                    String qualifier = null;
                    Integer aid = null;
                    Integer sid = null;
                    String guid = null;
                    String iid = null;
                    String tag = null;
                    String uid = null;
                    String ref = null;
                    String key1 = null;
                    String key2 = null;
                    String key3 = null;
                    String key4 = null;
                    String binaryKey = null;

                    for (KeyValue kv : result.raw()) {

                        if (Bytes.toString(kv.getQualifier()).equals("aid")) {
                            //System.out.println("[kv.getValue()]" + toInt(kv.getValue(), 0));
                            aid = toInt(kv.getValue(), 0);
                        } else if (Bytes.toString(kv.getQualifier()).equals("sid")) {
                            //System.out.println("[kv.getValue()]" + toInt(kv.getValue(), 0));
                            sid = toInt(kv.getValue(), 0);
                            byte[] bytes = kv.getKey().clone();
                            //System.out.println("kv.getFamilyArray()() : " +kv.getKeyString());

                            key1 = Arrays.toString(bytes);
                            key2 = new String(bytes);
                            key3 = kv.getKeyString();
                            key4= kv.getKey().toString();

                            binaryKey = Bytes.toStringBinary(bytes);

                            //System.out.println("s1 : "+ s1);
                            //System.out.println("s2 : "+ s2);

                        } else if (Bytes.toString(kv.getQualifier()).equals("guid")) {
                            guid = Bytes.toString(kv.getValue());
                        } else if (Bytes.toString(kv.getQualifier()).equals("iid")) {
                            iid = Bytes.toString(kv.getValue());
                        } else if (Bytes.toString(kv.getQualifier()).equals("tag")) {
                            tag = Bytes.toString(kv.getValue());
                        } else if (Bytes.toString(kv.getQualifier()).equals("uid")) {
                            uid = Bytes.toString(kv.getValue());
                        } else if (Bytes.toString(kv.getQualifier()).equals("ref")) {
                            ref = Bytes.toString(kv.getValue());
                        } else {
                            //System.out.println("[ETC1] "+Bytes.toString(kv.getValue()));
                            //System.out.println("[ETC2] "+toInt(kv.getValue(), 0));
                        }

                    }

                    if ((sid == 1624 || sid == 1628)) {
                    //if (true) {
                        System.out.println("----------------------------------------------");
                        System.out.println("key1 :" + key1);
                        //System.out.println("key2 :" + key2);
                        System.out.println("key3 :" + key3);
                        System.out.println("key4 :" + key4);
                        System.out.println("binaryKey :" + binaryKey);
                        System.out.println("sid :" + sid);
                        System.out.println("aid :" + aid);
                        System.out.println("guid :" + guid);
                        System.out.println("uid :" + uid);
                        System.out.println("tag :" + tag);
                        System.out.println("ref :" + ref);
                        System.out.println("iid :" + iid);
                        //break;
                    }
                }

                resultScanner.close();
            } // 요기!!

            table.close();

        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void selectRowKey(String tablename, String rowKey) throws IOException
    {
        HTable table = new HTable(config, tablename);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);

        for (KeyValue kv : rs.raw())
        {
            System.out.println("--------------------" + new String(kv.getRow()) + "----------------------------");
            System.out.println("Column Family: " + new String(kv.getFamily()));
            System.out.println("Column       :" + new String(kv.getQualifier()));
            System.out.println("value        : " + new String(kv.getValue()));
        }
    }

    public static int toInt(byte[] bytes, int offset) {
        int ret = 0;
        for (int i=0; i<4 && i+offset<bytes.length; i++) {
            ret <<= 8;
            ret |= (int)bytes[i] & 0xFF;
        }
        return ret;
    }


}