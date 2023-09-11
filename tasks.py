from pathlib import Path
from invoke import task
import sys

"""
    Pythonic task execution
        - Invoke: https://docs.pyinvoke.org/en/stable/concepts/invoking-tasks.html

"""

# thank you windows
pty = False if sys.platform == 'win32' else True

def _find_packages(path: Path) -> Path:
    for pkg in path.iterdir():
        if pkg.is_dir() and len(list(pkg.glob("**/*.py"))) >= 1:
            yield pkg

def _find_scripts(path: Path):
    return path.glob("**/*.py")

@task()
def lint(c, folder=None):
    if folder is not None:
        c.run(f'flake8 {folder}', echo=True, pty=pty)
    else:
        c.run("flake8 .", echo=True, pty=pty)

@task
def format(c, fix=False, diff=False):
    if fix and diff:
        print("Select either 'fix' or 'diff'.")
        sys.exit(1)
    if fix:
        arg = ""
    elif diff:
        arg = " --diff"
    else:
        arg = " --check"

    c.run(f"black{arg} . --line-length=99", echo=True, pty=pty)

@task
def test(c):
    c.run("pytest", echo=True, pty=pty)
