using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace DeadLetterPurge;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        const string connectionString = "";

        var serviceBusClient = new ServiceBusClient(connectionString);
        var serviceBusAdministrationClient = new ServiceBusAdministrationClient(connectionString);

        var queues = serviceBusAdministrationClient.GetQueuesAsync();

        var receiverOptions = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete,
            SubQueue = SubQueue.DeadLetter
        };

        await foreach (var queue in queues)
        {
            var queueName = queue.Name;

            Console.WriteLine("Purging dead-letters. Queue: " + queueName);

            var receiver = serviceBusClient.CreateReceiver(queueName, receiverOptions);

            var maxTimeToWaitForNextMessage = TimeSpan.FromSeconds(5);
            
            try
            {
                IReadOnlyList<ServiceBusReceivedMessage> serviceBusReceivedMessages;

                do
                {
                    serviceBusReceivedMessages = await receiver.ReceiveMessagesAsync(5000, maxTimeToWaitForNextMessage)
                        .ConfigureAwait(false);
                } while (serviceBusReceivedMessages?.Any() == true); // This list is said to never return null, but taking an extra precaution here.

                Console.WriteLine("Dead-letter purge complete. Queue: " + queueName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Dead-letter purge failed. Queue: " + queueName + " " + ex);
            }
        }

        Console.WriteLine("Dead-letter purge job complete. Next scheduled run will be in 3 days");
    }
}