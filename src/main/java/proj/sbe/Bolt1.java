package proj.sbe;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Bolt1 extends BaseRichBolt { // role: filter based on the "company" field

    private OutputCollector _collector;

    // these lists will store the values from the input filter based on the operator
    private List<String> companyFilter = new ArrayList<>();
    private List<String> companyNotFilter = new ArrayList<>();
    //TODO subscriptions.txt: company must have only "=" and "!="

    private void process(List<HashMap<String, String>> publications, String filter) {
        // take the input filter and add to the filter lists
        processFilter(filter);

        // keep only the publications matching the filters
        for (int index = 0; index < publications.size(); index++) {
            HashMap<String, String> publication = publications.get(index);
            if (publication.containsKey("company")) {
                String value = publication.get("company");
                // remove all which are not eq
                if(!companyNotFilter.isEmpty() && companyNotFilter.contains(value)) {
                    publications.remove(publication);
                    index--;
                }
                // keep those which are eq
                else if (!companyFilter.isEmpty() && !companyFilter.contains(value)) {
                    publications.remove(publication);
                    index--;
                }
            }
        }

        callBolt2(publications, filter);
    }

    private void processFilter(String filter) {
        Pattern pattern = Pattern.compile("company(=|!=)(.+?(,|$))");
        Matcher matcher = pattern.matcher(filter);
        while (matcher.find()) {
            String operator = matcher.group(1);
            String value = matcher.group(2);
            if(value.substring(value.length() - 1).equals(",")) {
                value = value.substring(0, value.length() - 1);
            }
            if (operator.equals("=")) {
                companyFilter.add(value);
            }
            else if(operator.equals("!=")) {
                companyNotFilter.add(value);
            }
        }
    }

    private void callBolt2(List<HashMap<String, String>> publications, String filter) {
        for (HashMap<String, String> publication : publications) {
            //System.out.println(publication);
        }
        Bolt2 bolt2 = new Bolt2();
        bolt2.process(publications, filter);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        HashMap<String, String> tuple = (HashMap<String, String>)input.getValueByField("stock");
        System.out.println(tuple);
        try {
            FileWriter myWriter = new FileWriter("temp_console.txt");
            myWriter.append("RUN\n").append(String.valueOf(tuple));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("Topology Result:");
    }
}
