import unittest
from unittest.mock import MagicMock
from ingestion_aggregated.manager import IngestionManager, MachineBuffer

class TestMachineBuffer(unittest.TestCase):
    def test_buffer_logic(self):
        expected = ["recipe", "job"]
        buf = MachineBuffer("m1", expected)
        
        # Init state
        self.assertFalse(buf.is_row_ready())
        
        # Add recipe
        buf.add_message("recipe", {"recipe": "R1"})
        self.assertFalse(buf.is_row_ready())
        
        # Add job
        buf.add_message("job", {"job": "J1"})
        self.assertTrue(buf.is_row_ready())
        
        # Pop
        row = buf.pop_row()
        self.assertEqual(row["recipe"], "R1")
        self.assertEqual(row["job"], "J1")
        self.assertFalse(buf.is_row_ready())

    def test_buffer_overflow(self):
        expected = ["recipe"]
        buf = MachineBuffer("m1", expected)
        buf.MAX_SIZE = 2
        
        buf.add_message("recipe", {"val": 1})
        buf.add_message("recipe", {"val": 2})
        buf.add_message("recipe", {"val": 3})
        
        # Should drop val=1, so we have 2 and 3
        self.assertEqual(len(buf.queues["recipe"]), 2)
        self.assertEqual(buf.queues["recipe"][0]["val"], 2)

class TestIngestionManager(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.config = {
            "machines": [
                {
                    "machine_id": "m1",
                    "expected_messages": ["recipe_id", "temperature"]
                }
            ]
        }
        self.manager = IngestionManager(self.config, self.mock_db)

    def test_process_message_flow(self):
        # 1. Send Recipe
        self.manager.process_message("topic", {"machine_id": "m1", "recipe_id": "R1"})
        # Should not write yet
        self.mock_db.write_point.assert_not_called()
        
        # 2. Send Temperature
        self.manager.process_message("topic", {"machine_id": "m1", "temperature": 50.0})
        # Buffer ready -> Check and flush -> Write
        self.mock_db.write_point.assert_called_once()


if __name__ == '__main__':
    unittest.main()
