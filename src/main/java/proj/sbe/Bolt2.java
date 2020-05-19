package proj.sbe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.max;
import static java.util.Collections.min;

public class Bolt2 { // role: filter based on the "stockValue" field

    // there are more operators possible for this field
    private List<Double> stockSmallerFilter = new ArrayList<>();
    private List<Double> stockSmallerEqFilter = new ArrayList<>();
    private List<Double> stockLargerFilter = new ArrayList<>();
    private List<Double> stockLargerEqFilter = new ArrayList<>();
    private List<Double> stockEqFilter = new ArrayList<>();
    private List<Double> stockNotEqFilter = new ArrayList<>();

    private Double globalMin;
    private Double globalMax;

    public void process(List<HashMap<String, String>> publications, String filter) {

        processFilter(filter);
        determineMinMax();

        boolean onlyEq = false;
        for (Double value : stockEqFilter) {
            if ((globalMin != null && globalMax != null && value > globalMin && value < globalMax) ||
                    (globalMin != null && globalMax == null && value > globalMin) ||
                    (globalMin == null && globalMax != null && value < globalMax) ||
                    (globalMin == null && globalMax == null)) {
                onlyEq = true;
            }
        }

        for (int index = 0; index < publications.size(); index++) {
            HashMap<String, String> publication = publications.get(index);
            if (publication.containsKey("stockValue")) {
                Double value = Double.parseDouble(publication.get("stockValue"));
                // remove all which are not eq
                if(!stockNotEqFilter.isEmpty() && stockNotEqFilter.contains(value)) {
                    publications.remove(publication);
                    index--;
                }
                // if one eq value is between min and max, keep only it
                else if(onlyEq && !stockEqFilter.contains(value)) {
                    publications.remove(publication);
                    index--;
                }
                // if all eq values are outside min and max, keep everything between min and max
                else if((globalMin != null && value < globalMin) ||
                        (globalMax != null && value > globalMax)) {
                    publications.remove(publication);
                    index--;
                }
            }
        }

        callBolt3(publications, filter);
    }

    public void processFilter(String filter) {
        Pattern pattern = Pattern.compile("stockValue(=|!=|<=|>=|<|>)(.+?(,|$))");
        Matcher matcher = pattern.matcher(filter);
        while (matcher.find()) {
            String operator = matcher.group(1);
            String valueS = matcher.group(2);
            if(valueS.substring(valueS.length() - 1).equals(",")) {
                valueS = valueS.substring(0, valueS.length() - 1);
            }
            Double value = Double.parseDouble(valueS);
            if (operator.equals("=")) {
                stockEqFilter.add(value);
            }
            else if(operator.equals("!=")) {
                stockNotEqFilter.add(value);
            }
            else if(operator.equals("<=")) {
                stockSmallerEqFilter.add(value);
            }
            else if(operator.equals("<")) {
                stockSmallerFilter.add(value);
            }
            else if(operator.equals(">=")) {
                stockLargerEqFilter.add(value);
            }
            else if(operator.equals(">")) {
                stockLargerFilter.add(value);
            }
        }
    }

    public void determineMinMax() {
        Double maxLarger = null, maxLargerEq = null, minSmaller = null, minSmallerEq = null;
        if (!stockLargerFilter.isEmpty()) maxLarger = max(stockLargerFilter);
        if (!stockLargerEqFilter.isEmpty()) maxLargerEq = max(stockLargerEqFilter);
        if (!stockSmallerFilter.isEmpty()) minSmaller = min(stockSmallerFilter);
        if (!stockSmallerEqFilter.isEmpty()) minSmallerEq = min(stockSmallerEqFilter);

        if (maxLarger == null && maxLargerEq == null) globalMin = null;
        else if (maxLarger == null) globalMin = maxLargerEq;
        else if (maxLargerEq == null) globalMin = maxLarger;
        else if (maxLarger.equals(maxLargerEq)) globalMin = maxLarger;
        else globalMin = Math.max(maxLarger, maxLargerEq);

        if (minSmaller == null && minSmallerEq == null) globalMax = null;
        else if (minSmaller == null) globalMax = minSmallerEq;
        else if (minSmallerEq == null) globalMax = minSmaller;
        else if (minSmaller.equals(minSmallerEq)) globalMax = minSmaller;
        else globalMax = Math.min(minSmaller, minSmallerEq);
    }

    public void callBolt3(List<HashMap<String, String>> publications, String filter) {
        for (HashMap<String, String> publication : publications) {
            System.out.println(publication);
        }
    }

}
