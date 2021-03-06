﻿using System.Linq;
using NServiceBus;
using NServiceBus.RavenDB.Persistence.SubscriptionStorage;
using NServiceBus.RavenDB.Tests;
using NServiceBus.Unicast.Subscriptions;
using NServiceBus.Unicast.Subscriptions.RavenDB;
using NUnit.Framework;

[TestFixture]
public class When_receiving_a_subscription_message : RavenDBPersistenceTestBase
{
    [Test]
    public void A_subscription_entry_should_be_added_to_the_database()
    {
        var clientEndpoint = Address.Parse("TestEndpoint");

        var messageTypes = new[]
        {
            new MessageType("MessageType1", "1.0.0.0"),
            new MessageType("MessageType2", "1.0.0.0")
        };

        var storage = new SubscriptionPersister
        {
            DocumentStore = store
        };

        storage.Subscribe(clientEndpoint, messageTypes);

        using (var session = store.OpenSession())
        {
            var subscriptions = session
                .Query<Subscription>()
                .Customize(c => c.WaitForNonStaleResults())
                .Count();

            Assert.AreEqual(2, subscriptions);
        }
    }
}