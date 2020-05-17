public class Filter {

    private Operator operator;
    private Object value;
    private String fieldName;

    public Filter(Operator operator, Object value, String fieldName) {
        this.operator = operator;
        this.value = value;
        this.fieldName = fieldName;
    }

    public Operator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return "( " + fieldName + ", " + operator.getOperatorString() + ", " + value.toString() + " );";
    }
}
