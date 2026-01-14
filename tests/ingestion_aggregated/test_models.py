import unittest
from ingestion_aggregated.models import MachineDataCombined

class TestModels(unittest.TestCase):
    def test_combined_model_valid(self):
        data = {
            "machine_id": "m1",
            "recipe_id": "r1",
            "temperature": 100.5
        }
        model = MachineDataCombined(**data)
        self.assertEqual(model.machine_id, "m1")
        self.assertEqual(model.recipe_id, "r1")
        self.assertEqual(model.temperature, 100.5)
        self.assertIsNone(model.job_id)

    def test_combined_model_missing_id(self):
        data = {
            "recipe_id": "r1"
        }
        # machine_id is required
        with self.assertRaises(ValueError):
            MachineDataCombined(**data)

    def test_combined_model_types(self):
        data = {
            "machine_id": "m1",
            "speed": "200.5" # String should be cast to float
        }
        model = MachineDataCombined(**data)
        self.assertEqual(model.speed, 200.5)

if __name__ == '__main__':
    unittest.main()
