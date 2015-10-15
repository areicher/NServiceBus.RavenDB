using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.RavenDB.Tests;
using NServiceBus.SagaPersisters.RavenDB;
using NUnit.Framework;
using Raven.Client;

[TestFixture]
public class When_updating_a_saga_property_on_a_existing_sagaInstance_that_just_got_a_unique_attribute_set : RavenDBPersistenceTestBase
{
    [Test]
    public async Task It_should_set_the_attribute_and_allow_the_update()
    {
        IDocumentSession session;
        var options = this.NewSagaPersistenceOptions(out session);
        var persister = new SagaPersister();
        var uniqueString = Guid.NewGuid().ToString();

        var anotherUniqueString = Guid.NewGuid().ToString();

        var saga1 = new SagaData
        {
            Id = Guid.NewGuid(),
            UniqueString = uniqueString,
            NonUniqueString = "notUnique"
        };

        await persister.Save(saga1, this.CreateMetadata<SomeSaga>(), options);
        session.SaveChanges();
        session.Dispose();

        using (session = store.OpenSession())
        {
            //fake that the attribute was just added by removing the metadata
            session.Advanced.GetMetadataFor(saga1).Remove(SagaPersister.UniqueValueMetadataKey);
            session.SaveChanges();
        }

        options = this.NewSagaPersistenceOptions(out session);
        var saga = await persister.Get<SagaData>(saga1.Id, options);
        saga.UniqueString = anotherUniqueString;
        await persister.Update(saga, options);
        session.SaveChanges();
        session.Dispose();

        using (session = store.OpenSession())
        {
            var value = session.Advanced.GetMetadataFor(saga1)[SagaPersister.UniqueValueMetadataKey].ToString();
            Assert.AreEqual(anotherUniqueString, value);
        }
    }

    class SomeSaga : Saga<SagaData>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SagaData> mapper)
        {
            mapper.ConfigureMapping<Message>(m => m.UniqueString).ToSaga(s => s.UniqueString);
        }

        class Message
        {
            public string UniqueString { get; set; }
        }
    }

    class SagaData : IContainSagaData
    {
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        public string UniqueString { get; set; }

        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        public string NonUniqueString { get; set; }
        public Guid Id { get; set; }
        public string Originator { get; set; }
        public string OriginalMessageId { get; set; }
    }
}