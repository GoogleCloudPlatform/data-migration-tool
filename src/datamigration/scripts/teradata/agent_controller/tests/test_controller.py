import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock

import controller


class Test_Controller(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        controller._TRANSFER_RUN_BASE_DIR = cls.temp_dir.name
        cls.tests_data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data"
        )
        cls.sample_payload_file = os.path.join(
            cls.tests_data_dir, "sample_payload.json"
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test__setup_agent(self):
        with open(self.sample_payload_file) as f:
            data = json.load(f)
        c = controller.Controller(data)

        c.run_action()

        self.assertEqual(data["action"], "setup")
        self.assertTrue(os.path.exists(c.transfer_run_dir))
        self.assertTrue(os.path.exists(os.path.join(c.transfer_run_dir, "credentials")))
        self.assertTrue(
            os.path.exists(os.path.join(c.transfer_run_dir, f"{c.transfer_id}.json"))
        )

    def test__run_agent(self):
        with open(self.sample_payload_file) as f:
            data = json.load(f)
        data["action"] = "run"
        del data["params"]["agent_config"]
        c = controller.Controller(data)
        c._run_agent = MagicMock(name="_runagent")

        c.run_action()

        self.assertEqual(data["action"], "run")
        self.assertEqual(c._run_agent.call_count, 1)

    def test__kill_agent(self):
        with open(self.sample_payload_file) as f:
            data = json.load(f)
        data["action"] = "kill"
        del data["params"]["agent_config"]
        c = controller.Controller(data)
        c._kill_agent = MagicMock(name="_kill_agent")

        c.run_action()

        self.assertEqual(data["action"], "kill")
        self.assertEqual(c._kill_agent.call_count, 1)
