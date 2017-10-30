/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.sequins.example;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// ADDED: The only extra imports we need.
import com.spotify.sparkey.*;
import org.xerial.snappy.SnappyFramedOutputStream;

public class WordCountSparkey {

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n,;:.!?()_-*“”‘");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // ADDED: A RecordWriter that writes (Text,IntWritable) to Sparkey files.
    public static class SparkeyTextIntRecordWriter extends RecordWriter<Text,IntWritable> {
        // The output Sparkey log-file and index-file.
        private OutputStream logOutput;
        private OutputStream indexOutput;

        // We'll be writing to local temporary files. This could in some circumstances eat up all our local disk space,
        // but it's ok for this sample program.
        private File localLogFile;
        private SparkeyWriter localWriter;

        // We'll store ints as litte-endian byte sequences, and this will help us convert them.
        private ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

        private SparkeyTextIntRecordWriter(OutputStream log, OutputStream index) throws IOException {
            logOutput = log;
            indexOutput = index;

            localLogFile = File.createTempFile("sequins", ".spl");
            // Our Sparkey file should be compressed.
            localWriter = Sparkey.createNew(localLogFile, CompressionType.SNAPPY, 8192);
        }

        public void write(Text key, IntWritable value) throws IOException, InterruptedException {
            // Convert the key to plain bytes, and the value to little-endian.
            byteBuffer.putInt(value.get());
            localWriter.put(key.copyBytes(), byteBuffer.array());

            // Make room for the next value.
            byteBuffer.rewind();
        }

        // A helper to copy files.
        private void copyAndDelete(File inFile, OutputStream outStream, boolean compress) throws IOException {
            InputStream inStream = new FileInputStream(inFile);
            if (compress) {
                // Compress with framed Snappy.
                outStream = new SnappyFramedOutputStream(outStream);
            }
            IOUtils.copyLarge(inStream, outStream);
            inStream.close();
            outStream.close();
            inFile.delete();
        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // Write an index file.
            localWriter.writeHash();
            localWriter.close();

            // Copy the log and index file to the real, non-temporary outputs.
            copyAndDelete(localLogFile, logOutput, false);
            copyAndDelete(Sparkey.getIndexFile(localLogFile), indexOutput, true);
        }
    }

    // ADDED: An OutputFormat that writes (Text,IntWritable) to Sparkey files.
    public static class SparkeyTextIntOutputFormat extends FileOutputFormat<Text,IntWritable> {
        public RecordWriter<Text,IntWritable> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            // Write both a log file, and a Snappy-compressed index file.
            Path log = getDefaultWorkFile(job, ".spl");
            Path index = getDefaultWorkFile(job, ".spi.sz");
            FileSystem fs = log.getFileSystem(job.getConfiguration());
            return new SparkeyTextIntRecordWriter(fs.create(log, true), fs.create(index, true));
        }
    }

    // ADDED: Partitioner for keys, according to String hashCode.
    public static class SequinsPartitioner extends Partitioner<Text,IntWritable> {
        public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
            return (text.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountSparkey.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ADDED: Set a partitioner, to partition according to String hashCode.
        job.setPartitionerClass(SequinsPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // ADDED: Setup a number of reducers, just to demonstrate that data is partitioned well.
        job.setNumReduceTasks(5);
        // ADDED: Use our custom output format.
        job.setOutputFormatClass(SparkeyTextIntOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
