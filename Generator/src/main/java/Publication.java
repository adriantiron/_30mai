import java.util.Date;

public class Publication {

    private Company company;
    private double stockValue;
    private double change;
    private double variation;
    private Date date;

    public static double stockMin;
    public static double stockMax;

    public Publication(Company company, double stockValue, double change, Date date) {
        this.company = company;

        if (stockValue > stockMax) {
            this.stockValue = stockMax;
        } else if (stockValue < stockMin) {
            this.stockValue = stockMin;
        } else {
            this.stockValue = stockValue;
        }

        this.change = change;
        this.variation = Math.abs(this.change / this.stockValue);
        this.date = date;
    }

    @Override
    public String toString() {
        return "{" +
                "company=" + company.getCompanyName() +
                ", stockValue=" + stockValue +
                ", change=" + change +
                ", variation=" + variation +
                ", date=" + date.toString() +
                '}' + "\n";
    }

    public Company getCompany() {
        return company;
    }

    public double getStockValue() {
        return stockValue;
    }

    public double getChange() {
        return change;
    }

    public double getVariation() {
        return variation;
    }

    public Date getDate() {
        return date;
    }
}
