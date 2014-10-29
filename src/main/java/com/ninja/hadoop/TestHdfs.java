package com.ninja.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;

import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: houzhaowei
 * Date: 9/22/13
 * Time: 11:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestHdfs {

    public static void main(String[] args) throws Exception{
        String uri = "hdfs://192.168.1.140:9000/";
        String str = "C8128DEE8D6862D11A06EEDE83A1144F|1380122934|http://stats.ipinyou.com/cubox.html?rdm=os2b3nk&pypk=sl1." +
                "iheU0pverkdlS2v3wU5u7iK7XfnURS-QhPo9UjAr3cY-e8D-SbTLiG948VuhCZcZOdXhjulsVTMU4FSjIBFvqLEJGPMyXBTs7uCE6y4JHIop." +
                "87Hb3nk.YdSw2RJRChuVfVts9nj6h0&pyre=http://adclick.g.doubleclick.net/aclk%3Fsa%3DL%26ai%3DCQVdaNgFDUvDtLsrMigfEqoD4DLW9nqAGxbupoF" +
                "L5xuyTARABIABQgMfhxARgnbnQgZAFggEXY2EtcHViLTE2NzgzMzQwMzc4NTUxNzbIAQmpAhVPMgES74U-qAMBqgRyT9CwMVAJmMEUDHZwO0ymqET2H0F_9qavFxfpYGAbG" +
                "tV_yirTOEAb0wWheav_rjtdbefQ1hk3_oBRHM-pKr8nHsfeIWyq-C5gPTLv-tjYSwEJrdy5stO7ukmoVzwJ4yRuLSzVp_jEHOhv1zIXdJuyCS02gAbVl6Kl87ekj58BoAYh%26num" +
                "%3D1%26sig%3DAOD64_1imNVe-rLYx4M6lf-eaUoPFuaz9A%26client%3Dca-pub-1678334037855176%26adurl%3Dhttp%253A%252F%252Fstats.ipinyou.com%252Fgdn%252" +
                "Fclick%253Fp%253DrFBSZ__LKb25.dEB0lnqqjULxJmKqaLq3Q6McJHsmot.87Hb3nk.Z0KAD75pk82x1MdMwzuEaeqrJZs-UfqrxDb5.W.hP8F5rUW.QA.W.kAdSQAcEd9uU1YdI5Sq" +
                "fNFdrM6BpJoBTUha1NDj1Yya5RNF1HSj.I.W.f.W.6YzrZrnxOyBfDH.v_.Rh._.p.qU.GY.US.YXs.sl1.RHh.s.Zq4*dqH*Cqt*VqU*lTW*GTV*QTG*qT-*Mh4*jhH*Xht*ghU*" +
                "EK8*7K2*rKl*SKc*zT8*iT2*ATl*KTc*4bW*ImV*9bG*8b-.IWfM._.z.kAdSQAcEdIFfw9K7wdpENpn.W.ZK.w2%2526s%253DoEn54911DGqhCG9DELFcu_%2526d%253D__p_turl" +
                "__%2526cu%253D__p_cuid__%2526pro%253D__p_pro__| stats.ipinyou.com| http://googleads.g.doubleclick.net/|/192.168.1.106|/211.151.123.5||\n";
        System.out.println(str.getBytes().length);
        Configuration configuration = new Configuration();
        System.out.println("-----");
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);
        System.out.println("-----");
        java.io.OutputStream out = fs.create(new Path("/user/hive/warehouse/test8"));
        out.write(str.getBytes());
        out.flush();
        out.close();

        out = fs.append(new Path("/user/hive/warehouse/test8/"));
        long start = System.currentTimeMillis();
        for (int i = 1 ; i < 10000 ; i++ ){
            out.write(str.getBytes());
        }
        long end = System.currentTimeMillis();

        out.flush();
        out.close();
        System.out.print(end - start);
    }
}
