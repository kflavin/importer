import os
from lambdas.periods import MONTHLY, WEEKLY

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
USERDATA_DIR = os.path.join(ROOT_DIR, "resources/userdata")