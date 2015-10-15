namespace NServiceBus.SagaPersisters.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.RavenDB.Persistence.SagaPersister;
    using NServiceBus.Sagas;
    using Raven.Abstractions.Commands;
    using Raven.Client;
    using Raven.Json.Linq;

    class SagaPersister : ISagaPersister
    {
        const string UniqueDocIdKey = "NServiceBus-UniqueDocId";

        public Task Save(IContainSagaData saga, SagaMetadata metadata, ContextBag context)
        {
            var session = context.Get<IDocumentSession>();

            session.Store(saga);

            var correlationProperty = metadata.CorrelationProperties.SingleOrDefault();

            if (correlationProperty == null)
            {
                return Task.FromResult(0);
            }

            var uniqueProperty = GetUniqueProperty(metadata, correlationProperty);

            var value = uniqueProperty.GetValue(saga);
            var id = SagaUniqueIdentity.FormatId(saga.GetType(), new KeyValuePair<string, object>(uniqueProperty.Name, value));


            var sagaDocId = session.Advanced.DocumentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier(saga.Id, saga.GetType(), false);

            session.Store(new SagaUniqueIdentity
            {
                Id = id,
                SagaId = saga.Id,
                UniqueValue = value,
                SagaDocId = sagaDocId
            });

            session.Advanced.GetMetadataFor(saga)[UniqueDocIdKey] = id;


            return Task.FromResult(0);
        }

        public Task Update(IContainSagaData saga, ContextBag context)
        {
            //np-op since the dirty tracking will handle the update for us
            return Task.FromResult(0);
        }

        public Task<T> Get<T>(Guid sagaId, ContextBag context) where T : IContainSagaData
        {
            var session = context.Get<IDocumentSession>();
            return Task.FromResult(session.Load<T>(sagaId));
        }

        public Task<T> Get<T>(string property, object value, ContextBag context) where T : IContainSagaData
        {
            var session = context.Get<IDocumentSession>();

            var lookupId = SagaUniqueIdentity.FormatId(typeof(T), new KeyValuePair<string, object>(property, value));

            //store it in the context to be able to optimize deletes for legacy sagas that don't have the id in metadata
            context.Set(UniqueDocIdKey, lookupId);

            var lookup = session
                .Include("SagaDocId") //tell raven to pull the saga doc as well to save us a round-trip
                .Load<SagaUniqueIdentity>(lookupId);

            if (lookup != null)
            {
                return lookup.SagaDocId != null
                    ? Task.FromResult(session.Load<T>(lookup.SagaDocId)) //if we have a saga id we can just load it
                    : Get<T>(lookup.SagaId, context); //if not this is a saga that was created pre 3.0.4 so we fallback to a get instead
            }

            return Task.FromResult(default(T));
        }

        public Task Complete(IContainSagaData saga, ContextBag context)
        {
            var session = context.Get<IDocumentSession>();
            session.Delete(saga);

            string uniqueDocumentId;
            RavenJToken uniqueDocumentIdMetadata;

            if (session.Advanced.GetMetadataFor(saga).TryGetValue(UniqueDocIdKey, out uniqueDocumentIdMetadata))
            {
                uniqueDocumentId = uniqueDocumentIdMetadata.Value<string>();
            }
            else
            {
                context.TryGet(UniqueDocIdKey, out uniqueDocumentId);
            }

            if (string.IsNullOrEmpty(uniqueDocumentId))
            {
                var uniqueDoc = session.Query<SagaUniqueIdentity>()
                    .SingleOrDefault(d => d.SagaId == saga.Id);

                if (uniqueDoc != null)
                {
                    session.Delete(uniqueDoc);
                }

                return Task.FromResult(0);
            }

            session.Advanced.Defer(new DeleteCommandData
            {
                Key = uniqueDocumentId
            });

            return Task.FromResult(0);
        }

        static PropertyInfo GetUniqueProperty(SagaMetadata metadata, CorrelationProperty correlationProperty)
        {
            return metadata.SagaEntityType.GetProperties().Single(p => p.CanRead && p.Name == correlationProperty.Name);
        }
    }
}