import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class PinotConfig:
    """Configuration container for Pinot connection settings"""
    controller_url: str
    broker_host: str
    broker_port: int
    broker_scheme: str
    username: str | None
    password: str | None
    token: str | None
    database: str
    use_msqe: bool
    request_timeout: int = 60
    connection_timeout: int = 60
    query_timeout: int = 60


def load_pinot_config() -> PinotConfig:
    """Load and return Pinot configuration from environment variables"""
    load_dotenv(override=True)
    
    return PinotConfig(
        controller_url=os.getenv("PINOT_CONTROLLER_URL", ""),
        broker_host=os.getenv("PINOT_BROKER_HOST", ""),
        broker_port=int(os.getenv("PINOT_BROKER_PORT", "443")),
        broker_scheme=os.getenv("PINOT_BROKER_SCHEME", "https"),
        username=os.getenv("PINOT_USERNAME"),
        password=os.getenv("PINOT_PASSWORD"),
        token=os.getenv("PINOT_TOKEN"),
        database=os.getenv("PINOT_DATABASE", ""),
        use_msqe=os.getenv("PINOT_USE_MSQE", "false").lower() == "true",
        request_timeout=int(os.getenv("PINOT_REQUEST_TIMEOUT", "60")),
        connection_timeout=int(os.getenv("PINOT_CONNECTION_TIMEOUT", "60")),
        query_timeout=int(os.getenv("PINOT_QUERY_TIMEOUT", "60"))
    )