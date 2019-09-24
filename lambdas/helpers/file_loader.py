import os
from lambdas import ROOT_DIR, USERDATA_DIR


def loader_user_data(fileName):
    return open(os.path.join(USERDATA_DIR, fileName + ".sh")).read()