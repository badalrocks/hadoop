package com.badal.adhoc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * This is adhoc example testing Bloom Filter algorithm. Reads an input file (bloom_input.txt) that contains keys to be
 * filtered in and creates a bloom_output file that stores the byte information of those keys. Then runs a test over
 * array of values to check for membership in bloom filter. The console log is copied into
 * bloom_filter_adhoc_console.txt.
 * 
 * @author badal
 *
 */
public class BloomFilterExample
{
    public static void main(String[] args) throws IOException
    {
        BloomFilter bloomFilter = new BloomFilter(10, 2, Hash.MURMUR_HASH);

        FileSystem fs = FileSystem.get(new Configuration());

        Path bloomOutput = new Path(args[1]);

        BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
        String s = null;
        while ((s = br.readLine()) != null)
        {
            System.out.println("s : " + s);

            bloomFilter.add(new Key(s.getBytes()));

        }

        br.close();

        FSDataOutputStream stream = fs.create(bloomOutput);
        bloomFilter.write(stream);
        stream.flush();
        stream.close();

        String[] test = { "GOOGL", "GOOG", "T", "TWTR", "MSFT", "FB" };

        for (String t : test)
        {
            boolean isBloom = bloomFilter.membershipTest(new Key(t.getBytes()));

            System.out.println("test: " + t + ", isBloom: " + isBloom);
        }

        System.exit(0);

    }

}
