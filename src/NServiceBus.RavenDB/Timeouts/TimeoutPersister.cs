namespace NServiceBus.TimeoutPersisters.RavenDB
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.Timeout.Core;
    using Raven.Abstractions.Data;
    using Raven.Client;
    using CoreTimeoutData = NServiceBus.Timeout.Core.TimeoutData;
    using Timeout = TimeoutData;

    class TimeoutPersister : IPersistTimeouts
    {
        public TimeoutPersister(IDocumentStore store)
        {
            documentStore = store;
        }

        public Task Add(CoreTimeoutData timeout, ContextBag context)
        {
            using (var session = documentStore.OpenSession())
            {
                session.Store(new Timeout(timeout));
                session.SaveChanges();
            }
            return Task.FromResult(0);
        }

        public Task<CoreTimeoutData> Remove(string timeoutId, ContextBag context)
        {
            using (var session = documentStore.OpenSession())
            {
                session.Advanced.UseOptimisticConcurrency = true;

                var timeout = session.Load<Timeout>(timeoutId);
                if (timeout == null)
                {
                    return Task.FromResult(default(CoreTimeoutData));
                }

                var timeoutData = timeout.ToCoreTimeoutData();
                session.Delete(timeout);
                session.SaveChanges();
                return Task.FromResult(timeoutData);
            }
        }

        public Task RemoveTimeoutBy(Guid sagaId, ContextBag context)
        {
            var operation = documentStore.DatabaseCommands.DeleteByIndex("TimeoutsIndex", new IndexQuery
            {
                Query = $"SagaId:{sagaId}"
            }, new BulkOperationOptions
            {
                AllowStale = true
            });
            operation.WaitForCompletion();
            return Task.FromResult(0);
        }

        readonly IDocumentStore documentStore;
    }
}