﻿namespace NServiceBus.RavenDB.Tests.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using NServiceBus.Outbox;
    using NServiceBus.RavenDB.Outbox;
    using NUnit.Framework;
    using Raven.Client;

    [TestFixture]
    public class When_cleaning_outbox_messages : RavenDBPersistenceTestBase
    {
        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            new OutboxRecordsIndex().Execute(store);
        }

        [Test]
        public async Task Should_delete_all_OutboxRecords_that_have_been_dispatched()
        {
            var id = Guid.NewGuid().ToString("N");


            IDocumentSession sesssion;
            var options = this.NewOptions(out sesssion);

            var persister = new OutboxPersister { DocumentStore = store };
            await persister.Store(new OutboxMessage("NotDispatched", new List<TransportOperation>()), options);

            var outboxMessage = new OutboxMessage(id, new List<TransportOperation> { new TransportOperation(id, new Dictionary<string, string>(), new byte[1024 * 5], new Dictionary<string, string>()) });
            await persister.Store(outboxMessage, options);

            sesssion.SaveChanges();
            sesssion.Dispose();

            options = this.NewOptions(out sesssion);
            await persister.SetAsDispatched(id, options);
            Thread.Sleep(TimeSpan.FromSeconds(1)); //Need to wait for dispatch logic to finish

            WaitForIndexing(store);

            var cleaner = new OutboxRecordsCleaner { DocumentStore = store };
            cleaner.RemoveEntriesOlderThan(DateTime.UtcNow.AddMinutes(1));

            using (var s = store.OpenSession())
            {
                var result = s.Query<OutboxRecord>().ToList();

                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("NotDispatched", result[0].MessageId);
            }
        }
    }
}
