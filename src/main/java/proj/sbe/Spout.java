package proj.sbe;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Spout { // role: grab the publication data and send it to filters

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

    public void readPublications() {
        try {
            File file = new File("publications.txt");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null) {
                initTuple(line);
            }

            callBolt1();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initTuple(String line) {
        Pattern pattern = Pattern.compile("(\\w+)=(.+?(,|}))");
        Matcher matcher = pattern.matcher(line);
        HashMap<String, String> tuples = new HashMap<>();
        while (matcher.find()) {
            String item = matcher.group(1);
            String value = matcher.group(2).substring(0, matcher.group(2).length() - 1);
            tuples.put(item, value);
        }
        publications.add(tuples);
    }

    public void callBolt1() {
        Bolt1 bolt1 = new Bolt1();
        bolt1.process(publications, inputFilter);
    }
}
