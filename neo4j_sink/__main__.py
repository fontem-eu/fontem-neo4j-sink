import logging

from .shutdown import install_stop_handlers
from .sink import Neo4jSink


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Before the loop starts: as PID 1 an unregistered SIGTERM is
    # ignored, so without this the pod only ever dies on SIGKILL and
    # stalls every node drain. See .shutdown.
    install_stop_handlers()
    Neo4jSink.from_env().run_forever()


if __name__ == "__main__":
    main()
