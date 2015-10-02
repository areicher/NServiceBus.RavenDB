namespace NServiceBus.RavenDB.Outbox
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using NServiceBus.Outbox;
    using Raven.Client;

    class OutboxPersister : IOutboxStorage
    {
        public IDocumentStore DocumentStore { get; set; }

        public Task<OutboxMessage> Get(string messageId, OutboxStorageOptions options)
        {
            OutboxRecord result;
            using (var session = DocumentStore.OpenSession())
            {
                // We use Load operation and not queries to avoid stale results
                result = session.Load<OutboxRecord>(GetOutboxRecordId(messageId));
            }

            if (result == null)
            {
                return Task.FromResult(default(OutboxMessage));
            }

            var operations = result.TransportOperations.Select(t => new TransportOperation(t.MessageId, t.Options, t.Message, t.Headers)).ToList();
            var message = new OutboxMessage(result.MessageId, operations);
  
            return Task.FromResult(message);
        }

        public Task Store(OutboxMessage outboxMessage, OutboxStorageOptions options)
        {
            var session = options.GetSession();
            session.Advanced.UseOptimisticConcurrency = true;

            session.Store(new OutboxRecord
            {
                MessageId = outboxMessage.MessageId,
                Dispatched = false,
                TransportOperations = outboxMessage.TransportOperations.Select(t => new OutboxRecord.OutboxOperation
                {
                    Message = t.Body,
                    Headers = t.Headers,
                    MessageId = t.MessageId,
                    Options = t.Options
                }).ToList()
            }, GetOutboxRecordId(outboxMessage.MessageId));

            return Task.FromResult(0);
        }

        public Task SetAsDispatched(string messageId, OutboxStorageOptions options)
        {
            using (var session = DocumentStore.OpenSession())
            {
                session.Advanced.UseOptimisticConcurrency = true;
                var outboxMessage = session.Load<OutboxRecord>(GetOutboxRecordId(messageId));
                if (outboxMessage == null || outboxMessage.Dispatched)
                {
                    return Task.FromResult(0);
                }

                outboxMessage.Dispatched = true;
                outboxMessage.DispatchedAt = DateTime.UtcNow;

                session.SaveChanges();
            }
            return Task.FromResult(0);
        }

        static string GetOutboxRecordId(string messageId)
        {
            return "Outbox/" + messageId;
        }
    }
}