package proj.sbe;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PublisherSpout extends BaseRichSpout { // role: grab the publication data and send it to filters

    private SpoutOutputCollector _collector;

    // test with each inputFilter below
    // company tests

    // google and microsoft
    private String inputFilter = "company=GOOGLE, company=MICROSOFT";

    // google
    //private String inputFilter = "company=GOOGLE, company=MICROSOFT, company!=MICROSOFT";

    // all but google and microsoft
    //private String inputFilter = "company!=GOOGLE, company!=MICROSOFT";

    // stockValue tests

    // all between 50 and 100, except 50.91
    //private String inputFilter = "company=GOOGLE, company=MICROSOFT, company!=MICROSOFT, stockValue>50, stockValue<=100, stockValue<120, stockValue!=50.91770248132858, stockValue=500";

    // only 50.91
    //private String inputFilter = "company=GOOGLE, company=MICROSOFT, company!=MICROSOFT, stockValue>50, stockValue<=100, stockValue<120, stockValue=50.91770248132858, stockValue=500";

    // all between 50 and 120
    //private String inputFilter = "company=GOOGLE, company=MICROSOFT, company!=MICROSOFT, stockValue>50, stockValue<120, stockValue=500";

    // all between 50 and 100, including 50.91 (see it at the end)
    //private String inputFilter = "company=GOOGLE, company=MICROSOFT, company!=MICROSOFT, stockValue>50, stockValue<=100, stockValue<120, stockValue=500";

    private List<HashMap<String, String>> publications = new ArrayList<>();

    private void readPublications() {
        try {
            File file = new File("publications.txt");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                initTuple(line);
            }
            // callBolt1();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initTuple(String line) {
        Pattern pattern = Pattern.compile("(\\w+)=(.+?(,|}))");
        Matcher matcher = pattern.matcher(line);
        HashMap<String, String> tuples = new HashMap<>();
        while (matcher.find()) {
            String item = matcher.group(1);
            String value = matcher.group(2).substring(0, matcher.group(2).length() - 1);
            tuples.put(item, value);
        }
        // publications.add(tuples);
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        Pattern pattern = Pattern.compile("(\\w+)=(.+?(,|}))");
        Matcher matcher;
        HashMap<String, String> tuples = new HashMap<>();

        try {
            FileReader fr = new FileReader(new File("pub_test.txt"));
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                // initTuple(line);
                matcher = pattern.matcher(line);
                while (matcher.find()) {
                    String item = matcher.group(1);
                    String value = matcher.group(2).substring(0, matcher.group(2).length() - 1);
                    tuples.put(item, value);
                }
                _collector.emit(new Values(tuples));
                tuples.clear();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stock"));
    }
}
