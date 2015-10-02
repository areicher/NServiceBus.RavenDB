namespace NServiceBus.RavenDB.Tests.Outbox
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NServiceBus.Outbox;
    using NServiceBus.RavenDB.Outbox;
    using NUnit.Framework;
    using Raven.Abstractions.Exceptions;
    using Raven.Client;
    using Raven.Client.Exceptions;

    [TestFixture]
    public class When_adding_outbox_messages : RavenDBPersistenceTestBase
    {
        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
            new OutboxRecordsIndex().Execute(store);
        }

        [Test]
        public async Task Should_throw_if__trying_to_insert_same_messageid()
        {
            IDocumentSession session;
            var options = this.NewOptions(out session);
            var persister = new OutboxPersister();

            using (session)
            {
                await persister.Store(new OutboxMessage("MySpecialId", new List<TransportOperation>()), options);
                var exception = Catch<NonUniqueObjectException>(async () => await persister.Store(new OutboxMessage("MySpecialId", new List<TransportOperation>()), options));
                Assert.NotNull(exception);
                session.SaveChanges();
            }
        }

        [Test]
        public async Task Should_throw_if__trying_to_insert_same_messageid2()
        {
            IDocumentSession session;
            var options = this.NewOptions(out session);
            var persister = new OutboxPersister();

            await persister.Store(new OutboxMessage("MySpecialId", new List<TransportOperation>()), options);
            session.SaveChanges();
            session.Dispose();

            options = this.NewOptions(out session);
            await persister.Store(new OutboxMessage("MySpecialId", new List<TransportOperation>()), options);
            Assert.Throws<ConcurrencyException>(session.SaveChanges);
        }

        [Test]
        public async Task Should_save_with_not_dispatched()
        {
            var id = Guid.NewGuid().ToString("N");
            IDocumentSession session;
            var options = this.NewOptions(out session);

            var persister = new OutboxPersister { DocumentStore = store };

            var message = new OutboxMessage(id, new List<TransportOperation> { new TransportOperation(id, new Dictionary<string, string>(), new byte[1024 * 5], new Dictionary<string, string>()) });
            await persister.Store(message, options);

            session.SaveChanges();
            session.Dispose();

            options = this.NewOptions(out session);
            var result = await persister.Get(id, options);

            var operation = result.TransportOperations.Single();

            Assert.AreEqual(id, operation.MessageId);
        }

        [Test]
        public async Task Should_update_dispatched_flag()
        {
            var id = Guid.NewGuid().ToString("N");

            IDocumentSession session;
            var options = this.NewOptions(out session);
            var persister = new OutboxPersister{ DocumentStore = store };
            var message = new OutboxMessage(id, new List<TransportOperation> { new TransportOperation(id, new Dictionary<string, string>(), new byte[1024 * 5], new Dictionary<string, string>()) });
            await persister.Store(message, options);

            session.SaveChanges();
            session.Dispose();

            options = this.NewOptions(out session);
            await persister.SetAsDispatched(id, options);

            WaitForIndexing(store);

            using (var s = store.OpenSession())
            {
                var result = s.Query<OutboxRecord>()
                    .SingleOrDefault(o => o.MessageId == id);

                Assert.NotNull(result);
                Assert.True(result.Dispatched);
            }
        }
    }
}