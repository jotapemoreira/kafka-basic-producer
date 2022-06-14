using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace kafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var continuar = true;
            List<string> msgs = new List<string>();
            while (continuar)
            {
                Console.WriteLine("Enviar uma mensagem para o kafka:");
                msgs.Add(Console.ReadLine());
                Console.WriteLine("Enviar outra mensagem? (S/N)");
                continuar = Console.ReadLine().ToUpper() == "S" ? true : false;
            }

            await ProduzirAsync(msgs);
        }

        private static async Task ProduzirAsync(List<string> mensagens)
        {
            string bootstrapServers = "localhost:9092";
            string nomeTopico = "topic.teste";

            var logger = new LoggerConfiguration().WriteTo.Console(theme: AnsiConsoleTheme.Literate).CreateLogger();
            logger.Information("Produzindo mensagem com Kafka");

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopico}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    foreach (var m in mensagens)
                    {
                        var result = await producer.ProduceAsync(nomeTopico, new() { Value = m });

                        logger.Information($"Mensagem: {m} | Status: { result.Status}");
                    }
                }

                logger.Information("Envio de mensagens finalizado.");
            }
            catch (Exception ex)
            {
                logger.Error($@"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}");
            }
        }
    }
}