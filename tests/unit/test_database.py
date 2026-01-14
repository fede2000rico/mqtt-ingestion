import unittest
from unittest.mock import patch, MagicMock
from ingestion_aggregated.database import InfluxBackend

class TestInfluxBackend(unittest.TestCase):
    @patch('ingestion_aggregated.database.InfluxDBClient')
    def test_init(self, mock_client):
        # Test initialization
        backend = InfluxBackend()
        mock_client.assert_called_once()
        self.assertIsNotNone(backend.client)

    @patch('ingestion_aggregated.database.InfluxDBClient')
    def test_write_point(self, mock_client):
        backend = InfluxBackend()
        mock_write_api = backend.write_api
        
        mock_point = MagicMock()
        backend.write_point(mock_point)
        
        mock_write_api.write.assert_called_once()

    @patch('ingestion_aggregated.database.InfluxDBClient')
    def test_close(self, mock_client):
        backend = InfluxBackend()
        backend.close()
        backend.client.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
