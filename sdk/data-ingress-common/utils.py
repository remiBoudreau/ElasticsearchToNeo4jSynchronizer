import logging, os, datetime

def loggerFunction():
    LOG_USE_STREAM = os.environ.get("LOG_USE_STREAM", True)
    LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')
    LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

    logging.basicConfig(level=LOGLEVEL)
    logger = logging.getLogger(__name__)
    if LOG_USE_STREAM:
        handler = logging.StreamHandler()
    else:
        now = datetime.now()
        handler = logging.FileHandler(
                    LOG_PATH 
                    + now.strftime("%Y-%m-%d") 
                    + '.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger