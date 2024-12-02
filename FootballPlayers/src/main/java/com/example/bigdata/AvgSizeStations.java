package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvgSizeStations extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgSizeStations(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "LeagueStats");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(LeagueStatsMapper.class);
        job.setCombinerClass(LeagueStatsCombiner.class);
        job.setReducerClass(LeagueStatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LeagueStatsMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {

        private final Text leagueId = new Text();

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            String line = lineText.toString();
            String[] fields = line.split(";");

            try {
                // pobieranie danych
                int weight = Integer.parseInt(fields[15]);
                if (weight > 100) return;  // skip pilkarzy ponad 100 kg

                String league = fields[16];  // league_id
                int wage = Integer.parseInt(fields[11]);  // wage_eur
                int age = Integer.parseInt(fields[12]);  // age

                leagueId.set(league);

                // emit danych (zarobki, wiek, liczba zawodnikow ktore przekazuje (zawsze 1))
                context.write(leagueId, new SumCountWritable(wage, age, 1));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static class LeagueStatsReducer extends Reducer<Text, SumCountWritable, Text, Text> {
        public void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
            int totalWage = 0;
            int totalAge = 0;
            int playerCount = 0;

            for (SumCountWritable val : values) {
                totalWage += val.getSumWage();
                totalAge += val.getSumAge();
                playerCount += val.getCount();
            }

            // Oblicz srednie
            double avgWage = (double) totalWage / playerCount;
            double avgAge = (double) totalAge / playerCount;

            String result = String.format("%s;%.2f;%.2f;%d;", key.toString(), avgWage, avgAge, playerCount);
            context.write(new Text(result), new Text(""));
        }
    }


    public static class LeagueStatsCombiner extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {
        public void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException, InterruptedException {
            SumCountWritable result = new SumCountWritable();

            for (SumCountWritable val : values) {
                result.add(val);
            }

            context.write(key, result);
        }
    }
}