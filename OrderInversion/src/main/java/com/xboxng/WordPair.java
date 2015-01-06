package com.xboxng;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiang on 1/5/15.
 */
public class WordPair implements WritableComparable<WordPair> {
    private String word;
    private String neighbor;

    public WordPair(String word, String neighbor) {
        this.word = word;
        this.neighbor = neighbor;
    }

    @Override
    public int compareTo(WordPair that) {
        int result = this.word.compareTo(that.word);
        return result == 0 ? this.neighbor.compareTo(that.neighbor) : result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeUTF(neighbor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word = dataInput.readUTF();
        neighbor = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "(" + word + "," + neighbor + ")";
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getNeighbor() {
        return neighbor;
    }

    public void setNeighbor(String neighbor) {
        this.neighbor = neighbor;
    }

    @Override
    public int hashCode() {
        return this.word.hashCode() * 13 + this.neighbor.hashCode() * 17;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WordPair)) {
            return false;
        }

        WordPair that = (WordPair)o;
        return that.word.equals(this.word) && that.neighbor.equals(this.word);
    }
}
