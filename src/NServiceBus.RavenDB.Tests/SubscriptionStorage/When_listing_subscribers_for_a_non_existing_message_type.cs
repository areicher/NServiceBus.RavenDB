using System.Linq;
using NServiceBus.RavenDB.Tests;
using NServiceBus.Unicast.Subscriptions.RavenDB;
using NUnit.Framework;

[TestFixture]
public class When_listing_subscribers_for_a_non_existing_message_type : RavenDBPersistenceTestBase
{
    [Test]
    public void No_subscribers_should_be_returned()
    {
        var storage = new SubscriptionPersister
        {
            DocumentStore = store
        };

        storage.Init();
        var subscriptionsForMessageType = storage.GetSubscriberAddressesForMessage(MessageTypes.MessageA);

        Assert.AreEqual(0, subscriptionsForMessageType.Count());
    }
}