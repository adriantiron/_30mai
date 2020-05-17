import java.util.Date;

public class SubscriptionGenerationParams {

    private PublicationGenerationParams publicationParams;
    private double companyPercentage = 0.7D;
    private double stockValuePercentage = 0.5D;
    private double changePercentage = 0.5D;
    private double variationPercentage = 0.3D;
    private double datePercentage = 0.6D;
    private Date endDate;
    private int subscriptionCount = 10000;
    private double equalCompanyPercentage = 0.5D;

    public SubscriptionGenerationParams(PublicationGenerationParams publicationParams, Date endDate) {
        this.publicationParams = publicationParams;
        this.endDate = endDate;
    }

    public SubscriptionGenerationParams(PublicationGenerationParams publicationParams, double companyPercentage,
                                        double stockValuePercentage, double changePercentage, double variationPercentage,
                                        double datePercentage, Date endDate, int subscriptionCount, double equalCompanyPercentage) {
        this.publicationParams = publicationParams;
        this.companyPercentage = companyPercentage;
        this.stockValuePercentage = stockValuePercentage;
        this.changePercentage = changePercentage;
        this.variationPercentage = variationPercentage;
        this.datePercentage = datePercentage;
        this.endDate = endDate;
        this.subscriptionCount = subscriptionCount;
        this.equalCompanyPercentage = equalCompanyPercentage;
        validatePercentages();
    }

    private void validatePercentages() {
        if (companyPercentage + stockValuePercentage + changePercentage + variationPercentage + datePercentage < 1) {
            throw new RuntimeException("Percentage sum is not over 1");
        }
    }

    public double getCompanyPercentage() {
        return companyPercentage;
    }

    public void setCompanyPercentage(double companyPercentage) {
        this.companyPercentage = companyPercentage;
        validatePercentages();
    }

    public double getStockValuePercentage() {
        return stockValuePercentage;
    }

    public void setStockValuePercentage(double stockValuePercentage) {
        this.stockValuePercentage = stockValuePercentage;
        validatePercentages();
    }

    public double getChangePercentage() {
        return changePercentage;
    }

    public void setChangePercentage(double changePercentage) {
        this.changePercentage = changePercentage;
        validatePercentages();
    }

    public double getVariationPercentage() {
        return variationPercentage;
    }

    public void setVariationPercentage(double variationPercentage) {
        this.variationPercentage = variationPercentage;
        validatePercentages();
    }

    public double getDatePercentage() {
        return datePercentage;
    }

    public void setDatePercentage(double datePercentage) {
        this.datePercentage = datePercentage;
        validatePercentages();
    }

    public PublicationGenerationParams getPublicationParams() {
        return publicationParams;
    }

    public void setPublicationParams(PublicationGenerationParams publicationParams) {
        this.publicationParams = publicationParams;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public int getSubscriptionCount() {
        return subscriptionCount;
    }

    public void setSubscriptionCount(int subscriptionCount) {
        this.subscriptionCount = subscriptionCount;
    }

    public double getEqualCompanyPercentage() {
        return equalCompanyPercentage;
    }

    public void setEqualCompanyPercentage(double equalCompanyPercentage) {
        this.equalCompanyPercentage = equalCompanyPercentage;
    }
}
