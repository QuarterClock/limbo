import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main() -> None:
    logger.info("Hello, World!")


if __name__ == "__main__":
    main()
