import base64
import json
import logging
import os
import subprocess

from config import config

_PROJECT_ID = config["project_id"]
_TRANSFER_RUN_BASE_DIR = config["transfer_run_base_dir"]
_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
_AGENT_START_SCRIPT = os.path.join(_ROOT_DIR, "scripts", "start_agent.sh")
_AGENT_KILL_SCRIPT = os.path.join(_ROOT_DIR, "scripts", "kill_agent.sh")

_LOGGER = logging.getLogger(__name__)

SECRET_PREFIX = "secret-"


class Controller:
    def __init__(self, data):
        """Instantiates controller instance for each PUB/SUB payload

        Args:
            data (dict): PUB/SUB payload parsed as a dictionary
        """
        self.data = data
        try:
            self.transfer_id = data["transfer_id"]
            self.action = data["action"]
        except KeyError as e:
            raise KeyError(f"Missing key in JSON payload: {e}")
        self.transfer_run_dir = os.path.join(_TRANSFER_RUN_BASE_DIR, self.transfer_id)

    def run_action(self):
        """
        Runs relevant controller action based on pub/sub payload

        Returns:
            None
        """
        _LOGGER.info(f"Action: {self.action}")
        if self.action == "setup":
            self._setup_agent()
        elif self.action == "run":
            self._run_agent()
        elif self.action == "kill":
            self._kill_agent()
        else:
            raise ValueError(f"Invalid action: {self.action}")

    def _setup_agent(self):
        """
        Setups directory and configurations for a new transfer run

        Returns:
            None
        """
        agent_config = base64.b64decode(self.data["params"]["agent_config"]).decode(
            "utf-8"
        )
        agent_config = json.loads(agent_config)

        # creates transfer run directory
        _LOGGER.info(f"Creating directory: {self.transfer_run_dir}")
        os.makedirs(self.transfer_run_dir, exist_ok=True)

        # creates credential file for transfer run
        db_username = agent_config["teradata-config"]["connection"]["username"]

        if "secret_resource_id" in agent_config["teradata-config"]["connection"]:
            secret_resource_id = agent_config["teradata-config"]["connection"][
                "secret_resource_id"
            ]
        else:
            password = agent_config["teradata-config"]["connection"]["password"]
            if password.startswith(SECRET_PREFIX):
                secret_resource_id = (
                    f"projects/{_PROJECT_ID}/secrets/{password}/versions/latest"
                )
            else:
                secret_resource_id = None

        if secret_resource_id:
            cred_file_content = (
                f"username={db_username}\nsecret_resource_id={secret_resource_id}"
            )
        else:
            cred_file_content = f"username={db_username}\npassword={password}"
        cred_file = os.path.join(self.transfer_run_dir, "credentials")
        _LOGGER.info(f"Creating credential file: {cred_file}")
        with open(cred_file, "w") as f:
            f.write(cred_file_content)

        # create agent config file for transfer run
        agent_config_file = os.path.join(
            self.transfer_run_dir, f"{self.transfer_id}.json"
        )
        agent_config["teradata-config"]["database-credentials-file-path"] = cred_file
        _LOGGER.info(f"Creating agent config file: {agent_config_file}")
        with open(agent_config_file, "w") as f:
            json.dump(agent_config, f)

    def _run_agent(self):
        """
        Starts migration agent process for relevant transfer run

        Returns:
            None
        """
        _LOGGER.info("Starting migration agent")
        cmd = f"sudo bash {_AGENT_START_SCRIPT} {self.transfer_id}"
        _LOGGER.info(f"Executing command: {cmd}")
        subprocess.Popen(cmd, shell=True)

    def _kill_agent(self):
        """
        Kills migration agent process for relevant transfer run

        Returns:
            None
        """
        _LOGGER.info("Killing migration agent")
        cmd = f"sudo bash {_AGENT_KILL_SCRIPT} {self.transfer_id}"
        _LOGGER.info(f"Executing command: {cmd}")
        subprocess.Popen(cmd, shell=True)
