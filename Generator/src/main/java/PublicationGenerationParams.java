import java.util.Date;

public class PublicationGenerationParams {

    private double minStockValue = 0D;
    private double maxStockValue = 1000D;
    private Date startingDate = new Date(100, 0, 1);
    private int daysBetween = 1;
    private int publicationCount = 5000;
    private double maxChangePercentage = 0.1D;

    public PublicationGenerationParams(double minStockValue, double maxStockValue, Date startingDate,
                                       int daysBetween, int publicationCount, double maxChangePercentage) {
        this.minStockValue = minStockValue;
        this.maxStockValue = maxStockValue;
        this.startingDate = startingDate;
        this.daysBetween = daysBetween;
        this.publicationCount = publicationCount;
        this.maxChangePercentage = maxChangePercentage;
    }

    public PublicationGenerationParams(){

    }

    public double getMinStockValue() {
        return minStockValue;
    }

    public double getMaxStockValue() {
        return maxStockValue;
    }

    public Date getStartingDate() {
        return startingDate;
    }

    public int getDaysBetween() {
        return daysBetween;
    }

    public int getPublicationCount() {
        return publicationCount;
    }

    public double getMaxChangePercentage() {
        return maxChangePercentage;
    }
}
