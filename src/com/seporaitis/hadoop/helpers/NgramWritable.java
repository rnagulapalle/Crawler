package com.seporaitis.hadoop.helpers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;

public class NgramWritable implements WritableComparable<NgramWritable> {

    private String ngram;
    private int ngramSize;
    private Hashtable<Integer, Integer> values;

    public NgramWritable setNgram(String value) {
        ngram = value;
        return this;
    }

    public String getNgram() {
        return ngram;
    }

    public NgramWritable setNgramSize(int value) {
        ngramSize = value;
        return this;
    }

    public int getNgramSize() {
        return ngramSize;
    }

    public NgramWritable incrValue(Integer key) {

        if (values.containsKey(key)) {
            values.put(key, values.get(key) + 1);
        } else {
            values.put(key, 1);
        }

        return this;
    }

    public NgramWritable setValue(int key, int value) {
        values.put(key, value);
        return this;
    }

    public int getValue(Integer key) {
        return values.get(key);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        String str = input.readUTF();
        String[] listOfParts = str.split("\t");

        ngramSize = Integer.parseInt(listOfParts[0]);
        ngram = listOfParts[1];
        if (ngram.split("\\s").length != ngramSize) {
            throw new IOException("Could not recreate ngram: '" + ngram + "'.");
        }

        for (int i = 2; i < listOfParts.length; i += 2) {
            values.put(Integer.parseInt(listOfParts[i]),
                    Integer.parseInt(listOfParts[i + 1]));
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        String str = Integer.toString(ngramSize) + "\t" + ngram;

        for (Iterator<Entry<Integer, Integer>> it = values.entrySet()
                .iterator(); it.hasNext();) {
            Entry<Integer, Integer> entry = it.next();
            str += "\t" + entry.getKey().toString() + "\t"
                    + entry.getValue().toString();
        }

        output.writeUTF(str);
    }

    @Override
    public int compareTo(NgramWritable other) {
        return ngram.compareTo(other.getNgram());
    }

}
