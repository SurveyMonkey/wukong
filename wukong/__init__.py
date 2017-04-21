# Set up a null roothandler for our logging system

import logging
from logging import NullHandler
logging.getLogger(__name__).addHandler(NullHandler())
