import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SHELL_DIR = os.path.join(ROOT_DIR, "lambdas/resources/product")

print(f"shell dir is: {SHELL_DIR}")
print(f"root dir is: {ROOT_DIR}")


def shell_loader(fileName):
    print(f"shell dir is: {SHELL_DIR}")
    print(f"root dir is: {ROOT_DIR}")
    return open(os.path.join(SHELL_DIR, fileName + ".sh")).read()