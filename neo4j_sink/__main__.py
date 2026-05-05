import logging

from .sink import Neo4jSink


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    Neo4jSink.from_env().run_forever()


if __name__ == "__main__":
    main()
