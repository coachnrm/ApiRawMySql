using MySqlConnector;
using RealtimeDataApp.Models;


namespace RealtimeDataApp.Services
{
    public class OvstService
    {
        private readonly string _connectionString;
        private MySqlConnection _connection;
        private bool _listening;
        private List<Ovst> _latestData;

        public OvstService()
        {
            _connectionString = "Server=172.16.5.39;port=3306;Database=hos;User=hks;Password=Fi'rpk[k]@!#;";
            _latestData = new List<Ovst>();
            StartListening();
        }

        public async Task<List<Ovst>> GetAllOvst()
        {
            // Ensure we are listening for updates
            if (!_listening)
            {
                StartListening();
            }

            // Return the latest data fetched by PollDatabase
            return _latestData.Take(10).ToList();
        }

        private void StartListening()
        {
            if (_listening) return;

            _connection = new MySqlConnection(_connectionString);
            _connection.Open();

            _listening = true;

            // Start polling in a background task
            Task.Run(async () =>
            {
                while (_listening)
                {
                    await PollDatabase();
                    await Task.Delay(5000); // Poll every 5 seconds
                }
            });
        }

        private async Task PollDatabase()
        {
            try
            {
                using var connection = new MySqlConnection(_connectionString);
                await connection.OpenAsync();

                // // Query for the latest data (today's date, 10 rows max)
                // string query = "SELECT * FROM Ovst WHERE Vstdate = CURDATE() LIMIT 10;";
                // Query for the latest data for a specific hn, ordered by vstdate descending, limit 30 rows
                string query = @"
                    SELECT * 
                    FROM Ovst
                    WHERE Hn = '0020000'
                    ORDER BY Vstdate DESC
                    LIMIT 30;";
                using var command = new MySqlCommand(query, connection);
                using var reader = await command.ExecuteReaderAsync();

                var newData = new List<Ovst>();

                while (await reader.ReadAsync())
                {
                    var ovst = new Ovst
                    {
                        //HosGuid = reader["HosGuid"].ToString()!,
                        Vn = reader["Vn"]?.ToString(),
                        Hn = reader["Hn"]?.ToString(),
                        Hcode = reader["Hcode"]?.ToString(),
                        // Map other fields as needed...
                    };

                    newData.Add(ovst);
                }

                // Update the latest data
                _latestData = newData;

                Console.WriteLine("Real-time data updated.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error polling database: {ex.Message}");
            }
        }

        public void StopListening()
        {
            _listening = false;
            _connection?.Close();
        }
    }
}