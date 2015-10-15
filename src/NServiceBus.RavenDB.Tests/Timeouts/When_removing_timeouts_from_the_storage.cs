namespace NServiceBus.RavenDB.Tests.Timeouts
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.Support;
    using NServiceBus.TimeoutPersisters.RavenDB;
    using NUnit.Framework;
    using Timeout = NServiceBus.TimeoutPersisters.RavenDB.TimeoutData;
    using TimeoutData = NServiceBus.Timeout.Core.TimeoutData;

    [TestFixture]
    [Ignore("These tests currently operate under the assumption TimeoutData.Id gets assigned by the persistence layer; need to revisit this")]
    class When_removing_timeouts_from_the_storage : RavenDBPersistenceTestBase
    {
        [Test]
        public async Task Should_return_the_correct_headers()
        {
            var persister = new TimeoutPersister(store);

            var headers = new Dictionary<string, string>
            {
                {"Bar", "34234"},
                {"Foo", "aString1"},
                {"Super", "aString2"}
            };

            var timeout = new TimeoutData
            {
                Time = DateTime.UtcNow.AddHours(-1),
                Destination = "timeouts@" + RuntimeEnvironment.MachineName,
                SagaId = Guid.NewGuid(),
                State = new byte[]
                {
                    1,
                    1,
                    133,
                    200
                },
                Headers = headers,
                OwningTimeoutManager = "MyTestEndpoint"
            };
            var context = new ContextBag();
            await persister.Add(timeout, context);

            var timeoutData = await persister.Remove(timeout.Id, context);

            CollectionAssert.AreEqual(headers, timeoutData.Headers);
        }

        [Test]
        public async Task Should_remove_timeouts_by_id()
        {
            new TimeoutsIndex().Execute(store);

            var query = new QueryTimeouts(store)
            {
                EndpointName = "MyTestEndpoint"
            };
            var persister = new TimeoutPersister(store);

            var t1 = new TimeoutData
            {
                Time = DateTime.Now.AddYears(-1),
                OwningTimeoutManager = "MyTestEndpoint",
                Headers = new Dictionary<string, string>
                {
                    {"Header1", "Value1"}
                }
            };
            var context = new ContextBag();
            var t2 = new TimeoutData
            {
                Time = DateTime.Now.AddYears(-1),
                OwningTimeoutManager = "MyTestEndpoint",
                Headers = new Dictionary<string, string>
                {
                    {"Header1", "Value1"}
                }
            };

            await persister.Add(t1, context);
            await persister.Add(t2, context);

            WaitForIndexing(store);

            var timeouts = await query.GetNextChunk(DateTime.UtcNow.AddYears(-3));

            foreach (var timeout in timeouts.DueTimeouts)
            {
                await persister.Remove(timeout.Id, context);
            }

            using (var session = store.OpenSession())
            {
                Assert.Null(session.Load<Timeout>(new Guid(t1.Id)));
                Assert.Null(session.Load<Timeout>(new Guid(t2.Id)));
            }
        }

        [Test]
        public async Task Should_remove_timeouts_by_sagaid()
        {
            new TimeoutsIndex().Execute(store);

            var persister = new TimeoutPersister(store);

            var sagaId1 = Guid.NewGuid();
            var sagaId2 = Guid.NewGuid();
            var t1 = new TimeoutData
            {
                SagaId = sagaId1,
                Time = DateTime.Now.AddYears(1),
                OwningTimeoutManager = "MyTestEndpoint",
                Headers = new Dictionary<string, string>
                {
                    {"Header1", "Value1"}
                }
            };
            var t2 = new TimeoutData
            {
                SagaId = sagaId2,
                Time = DateTime.Now.AddYears(1),
                OwningTimeoutManager = "MyTestEndpoint",
                Headers = new Dictionary<string, string>
                {
                    {"Header1", "Value1"}
                }
            };

            var context = new ContextBag();
            await persister.Add(t1, context);
            await persister.Add(t2, context);

            WaitForIndexing(store);

            await persister.RemoveTimeoutBy(sagaId1, context);
            await persister.RemoveTimeoutBy(sagaId2, context);

            using (var session = store.OpenSession())
            {
                Assert.Null(session.Load<Timeout>(new Guid(t1.Id)));
                Assert.Null(session.Load<Timeout>(new Guid(t2.Id)));
            }
        }
    }
}