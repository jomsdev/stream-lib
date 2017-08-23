/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.cardinality;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.Bits;
import com.clearspring.analytics.util.IBuilder;
import sun.reflect.generics.tree.Tree;

/**
 * Java implementation of AdaKMV (HLL) algorithm from this paper:
 * <p/>
 * TODO: add paper reference
 * <p/>
 * TODO: add KMV description
 * <p/>
 * TODO: add KMV main feature (eg: The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.)
 * <p/>
 * <p>
 * TODO: Do we recommend the same?
 * Users have different motivations to use different types of hashing functions.
 * Rather than try to keep up with all available hash functions and to remove
 * the concern of causing future binary incompatibilities this class allows clients
 * to offer the value in hashed int or long form.  This way clients are free
 * to change their hash function on their own time line.  We recommend using Google's
 * Guava Murmur3_128 implementation as it provides good performance and speed when
 * high precision is required.  In our tests the 32bit MurmurHash function included
 * in this project is faster and produces better results than the 32 bit murmur3
 * implementation google provides.
 * </p>
 */

public class AdaKMV implements ICardinality, Serializable {

    private final RecordSet recordSet;


    /**
     * Create a new non-adaptative KMV (non-growing)
     */
    public AdaKMV(int threshold) {
        this.recordSet = new RecordSet(threshold, 0, threshold, new TreeSet<Long>(), new TreeSet<Long>());
    }

    /**
     * Create a new adaptative KMV TODO: not done
     */
    public AdaKMV(int threshold, int maxSize) {
        this.recordSet = new RecordSet(threshold, 0, maxSize, new TreeSet<Long>(), new TreeSet<Long>
                ());
    }


    /**
     * Creates a new AdaKMV instance using the given records.  Used for unmarshalling a serialized
     * instance and for merging multiple counters together.
     *
     * @param recordSet - the initial values for the records set
     */
    public AdaKMV(RecordSet recordSet) { // TODO: add missing params
        this.recordSet = recordSet;
    }

    @Override
    public boolean offerHashed(long hashedValue) {
        final long value = Math.abs(hashedValue);
        return recordSet.offer(value);
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        final long x = MurmurHash.hash64(o);
        return offerHashed(x);
    }

    @Override
    public long cardinality() {
        long cardinalityEstimation;
        if (recordSet.currentSize() < recordSet.replacementThreshold()){
            cardinalityEstimation = recordSet.currentSize();
        } else {
            final double distance = (1.0 * recordSet.maxValue()) / Long.MAX_VALUE;
            cardinalityEstimation = (long) Math.floor(1.0 / distance * recordSet.currentSize());
        }
        return cardinalityEstimation;
    }

    // TODO: unchecked need to see if its necessary to add the size first
    @Override
    public int sizeof() {
        return recordSet.currentSize() * 8; // TODO: not sure what this is used for or how
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        writeBytes(dos);

        return baos.toByteArray();
    }

    private void writeBytes(DataOutput serializedByteStream) throws IOException {
        serializedByteStream.writeInt(recordSet.currentSize() * 8); // TODO: check if 8 for long
        for (Long record : recordSet.records()){
            serializedByteStream.writeLong(record);
        }
    }

    /**
     * Add all the elements of the other set to this set.
     * <p/>
     * TODO: check this -> This operation does not imply a loss of precision.
     *
     * @param other A compatible AdaKMV instance
     * @throws CardinalityMergeException if other is not compatible
     */
    private void addAll(AdaKMV other) throws CardinalityMergeException {
        recordSet.merge(other.recordSet);
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        AdaKMV merged = new AdaKMV(new RecordSet(this.recordSet));
//        merged.addAll(this);

        if (estimators == null) {
            return merged;
        }

        for (ICardinality estimator : estimators) {
            if (!(estimator instanceof AdaKMV)) {
                throw new AdaKMVMergeException("Cannot merge estimators of different class");
            }
            AdaKMV adakmv = (AdaKMV) estimator;
            merged.addAll(adakmv);
        }

        return merged;
    }

//    private Object writeReplace() {
//        return new SerializationHolder(this);
//    }
//
//    /**
//     * This class exists to support Externalizable semantics for
//     * AdaKMV objects without having to expose a public
//     * constructor, public write/read methods, or pretend final
//     * fields aren't final.
//     *
//     * In short, Externalizable allows you to skip some of the more
//     * verbose meta-data default Serializable gets you, but still
//     * includes the class name. In that sense, there is some cost
//     * to this holder object because it has a longer class name. I
//     * imagine people who care about optimizing for that have their
//     * own work-around for long class names in general, or just use
//     * a custom serialization framework. Therefore we make no attempt
//     * to optimize that here (eg. by raising this from an inner class
//     * and giving it an unhelpful name).
//     */
//    private static class SerializationHolder implements Externalizable {
//
//        AdaKMV AdaKMVHolder;
//
//        public SerializationHolder(AdaKMV AdaKMVHolder) {
//            this.AdaKMVHolder = AdaKMVHolder;
//        }
//
//        /**
//         * required for Externalizable
//         */
//        public SerializationHolder() {
//
//        }
//
//        @Override
//        public void writeExternal(ObjectOutput out) throws IOException {
//            AdaKMVHolder.writeBytes(out);
//        }
//
//        @Override
//        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//            AdaKMVHolder = Builder.build(in);
//        }
//
//        private Object readResolve() {
//            return AdaKMVHolder;
//        }
//    }
//
//    public static class Builder implements IBuilder<ICardinality>, Serializable {
//        private static final long serialVersionUID = -2567898469253021883L;
//
//        private final double rsd;
//        private transient int log2m;
//
//        /**
//         * Uses the given RSD percentage to determine how many bytes the constructed AdaKMV will use.
//         *
//         * @deprecated Use {@link #withRsd(double)} instead. This builder's constructors did not match the (already
//         * themselves ambiguous) constructors of the AdaKMV class, but there is no way to make them match without
//         * risking behavior changes downstream.
//         */
//        @Deprecated
//        public Builder(double rsd) {
//            this.log2m = log2m(rsd);
//            validateLog2m(log2m);
//            this.rsd = rsd;
//        }
//
//        /** This constructor is private to prevent behavior change for ambiguous usages. (Legacy support). */
//        private Builder(int log2m) {
//            this.log2m = log2m;
//            validateLog2m(log2m);
//            this.rsd = rsd(log2m);
//        }
//
//        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//            in.defaultReadObject();
//            this.log2m = log2m(rsd);
//        }
//
//        @Override
//        public AdaKMV build() {
//            return new AdaKMV(log2m);
//        }
//
//        @Override
//        public int sizeof() {
//            int k = 1 << log2m;
//            return RegisterSet.getBits(k) * 4;
//        }
//
//        public static Builder withLog2m(int log2m) {
//            return new Builder(log2m);
//        }
//
//        public static Builder withRsd(double rsd) {
//            return new Builder(rsd);
//        }
//
//        public static AdaKMV build(byte[] bytes) throws IOException {
//            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
//            return build(new DataInputStream(bais));
//        }
//
//        public static AdaKMV build(DataInput serializedByteStream) throws IOException {
//            int log2m = serializedByteStream.readInt();
//            int byteArraySize = serializedByteStream.readInt();
//            return new AdaKMV(log2m,
//                    new RegisterSet(1 << log2m, Bits.getBits(serializedByteStream, byteArraySize)));
//        }
//    }

    @SuppressWarnings("serial") // TODO: check if we can suppress this warning also
    protected static class AdaKMVMergeException extends CardinalityMergeException {

        public AdaKMVMergeException(String message) {
            super(message);
        }
    }

}
