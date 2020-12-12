using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace localstack_dotnet.Services
{
    public class SqsListener : IHostedService
    {
        private readonly ILogger<SqsListener> _logger;
        private readonly IConfiguration _configuration;

        public SqsListener(ILogger<SqsListener> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public async Task StartAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                //Reading configuration
                var aswSection = _configuration.GetSection("Aws");
                var accessKey = aswSection.GetSection("AccessKey").Value;
                var secretKey = aswSection.GetSection("SecretKey").Value;
                var sqsUrl = aswSection.GetSection("SQSUrl").Value;
                var localstackUrl = aswSection.GetSection("localstackUrl").Value;

                //Creating sqs client
                var credentials = new Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey);
                
                _logger.LogInformation($"Using SQS queue url {sqsUrl}", sqsUrl);
                _logger.LogInformation("Using credentials: {access}, {secret}", accessKey, secretKey);
                
                AmazonSQSClient amazonSQSClient = new AmazonSQSClient(new AmazonSQSConfig
                {
                    ServiceURL = localstackUrl
                });

                //Receive request
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl);
                var response = await amazonSQSClient.ReceiveMessageAsync(receiveMessageRequest, stoppingToken);

                if (response.Messages.Any())
                {
                    foreach (Message message in response.Messages)
                    {
                        Console.WriteLine($"Message received");
                        Console.WriteLine($"Message: {message.Body}");

                        //Deleting message
                        var deleteMessageRequest = new DeleteMessageRequest(sqsUrl, message.ReceiptHandle);
                        await amazonSQSClient.DeleteMessageAsync(deleteMessageRequest, stoppingToken);

                        Console.WriteLine($"Message deleted");
                    }
                }

                await Task.Delay(5000, stoppingToken);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine($"Message received");
            
            return Task.CompletedTask;
        }
    }
}