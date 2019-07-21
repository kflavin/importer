import os
from lambdas import ROOT_DIR, USERDATA_DIR


def loader_user_data(fileName):
    print(f"shell dir is: {USERDATA_DIR}")
    print(f"root dir is: {USERDATA_DIR}")
    return open(os.path.join(USERDATA_DIR, fileName + ".sh")).read()