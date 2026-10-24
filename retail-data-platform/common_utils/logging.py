import logging

format = "%(asctime)s %(levelname)s %(message)s"


def get_logger(name, level = logging.INFO):

    """
    Return a logger that prints one line per message

    name: where the message comes from
    level: the lowest level we want to see (default will be INFO)
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format))
        logger.addHandler(handler)
    
    return logger