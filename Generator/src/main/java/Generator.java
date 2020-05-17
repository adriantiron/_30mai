import java.util.List;

public interface Generator {

    List<Publication> generatePublications(PublicationGenerationParams params);

    List<Subscription> generateSubscriptions(SubscriptionGenerationParams params);
}
