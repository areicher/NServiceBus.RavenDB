namespace NServiceBus.SagaPersisters.RavenDB
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using NServiceBus.Extensibility;
    using NServiceBus.RavenDB.Persistence.SagaPersister;
    using NServiceBus.Sagas;
    using Raven.Abstractions.Commands;
    using Raven.Client;

    class SagaPersister : ISagaPersister
    {
        internal const string UniqueValueMetadataKey = "NServiceBus-UniqueValue";
        static readonly ConcurrentDictionary<string, bool> PropertyCache = new ConcurrentDictionary<string, bool>();

        public bool AllowUnsafeLoads { get; set; }

        public Task Save(IContainSagaData saga, SagaMetadata metadata, ContextBag context)
        {
            var session = context.Get<IDocumentSession>();

            session.Store(saga);

            StoreUniqueProperty(saga, session, metadata);
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

            // TODO: Fix
            //var correlationProperty = options.Metadata.CorrelationProperties.Sinfg();

            //if (correlationProperty == null)
            //{
            //    return Task.FromResult(0);
            //}

            //var uniqueProperty = GetUniqueProperty(options.Metadata, correlationProperty);
            //DeleteUniqueProperty(saga, session, new KeyValuePair<string, object>(uniqueProperty.Name, uniqueProperty.GetValue(saga)));
            return Task.FromResult(0);
        }

     

        static PropertyInfo GetUniqueProperty(SagaMetadata metadata, CorrelationProperty correlationProperty)
        {
            // TODO: Check assumption
            return metadata.SagaEntityType.GetProperties().Single(p => p.CanRead && p.Name == correlationProperty.Name);
        }
        
        static void StoreUniqueProperty(IContainSagaData saga, IDocumentSession session, SagaMetadata sagaMetadata)
        {
            var correlationProperty = sagaMetadata.CorrelationProperties.Single();

            var uniqueProperty = GetUniqueProperty(sagaMetadata, correlationProperty);

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

            SetUniqueValueMetadata(saga, session, new KeyValuePair<string, object>(uniqueProperty.Name, value));
        }

        static void SetUniqueValueMetadata(IContainSagaData saga, IDocumentSession session, KeyValuePair<string, object> uniqueProperty)
        {
            session.Advanced.GetMetadataFor(saga)[UniqueValueMetadataKey] = uniqueProperty.Value.ToString();
        }

        static void DeleteUniqueProperty(IContainSagaData saga, IDocumentSession session, KeyValuePair<string, object> uniqueProperty)
        {
            var id = SagaUniqueIdentity.FormatId(saga.GetType(), uniqueProperty);

            session.Advanced.Defer(new DeleteCommandData
            {
                Key = id
            });
        }
    }
}