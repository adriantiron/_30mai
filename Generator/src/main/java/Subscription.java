import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Subscription {

    public int filterNumber = 5;
    private List<Filter> filters = new ArrayList<>();

    public boolean contains(String fieldName) {
        for (Filter filter : filters) {
            if (filter.getFieldName().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    public Filter get(String fieldName) {
        for (Filter filter : filters) {
            if (filter.getFieldName().equals(fieldName)) {
                return filter;
            }
        }
        return null;
    }

    public void update(String fieldName, Filter newFilter) {
        for (int index = 0; index < filters.size(); index++) {
            if (filters.get(index).getFieldName().equals(fieldName)) {
                filters.set(index, newFilter);
                break;
            }
        }
    }

    public boolean hasEq(String fieldName) {
        for (Filter filter : filters) {
            if (filter.getFieldName().equals(fieldName) &&
                filter.getOperator() == Operator.EQ) {
                return true;
            }
        }
        return false;
    }

    public void sort() {
        int index = 0;
        final String[] fields = {"company", "stockValue", "change", "variation", "date"};
        for (String fieldName : fields) {
            if (contains(fieldName)) {
                Filter aux = get(fieldName);
                update(fieldName, filters.get(index));
                filters.set(index, aux);
                index++;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");

        for (Filter filter : filters) {
            sb.append(filter.toString());
        }
        sb.append(" }\n");

        return sb.toString();
    }

    public List<Filter> getFilters() {
        return filters;
    }
}
