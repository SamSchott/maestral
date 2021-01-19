# -*- coding: utf-8 -*-

# system imports
from setuptools import setup, find_packages  # type: ignore


# proceed with actual install
install_requires = [
    "alembic>=1.3,<1.6",
    "click>=7.1.1,<8.0",
    "desktop-notifier",
    "dropbox>=10.9.0,<12.0",
    "dbus-next>=0.1.4;sys_platform=='linux'",
    "fasteners>=0.15",
    "importlib_metadata;python_version<'3.8'",
    "importlib_resources;python_version<'3.9'",
    "keyring>=22",
    "keyrings.alt>=3.1.0,<5.0",
    "packaging",
    "pathspec>=0.5.8",
    "Pyro5>=5.10",
    "requests>=2.16.2",
    "rubicon-objc>=0.3.1;sys_platform=='darwin'",
    "sdnotify>=0.3.2",
    "setuptools",
    "sqlalchemy>=1.3,<1.4",
    "survey>=3.2.2,<4.0",
    "watchdog@git+https://github.com/CCP-Aporia/watchdog",
]

gui_requires = [
    "maestral-qt>=1.3.1;sys_platform=='linux'",
    "maestral-cocoa>=1.3.0;sys_platform=='darwin'",
]

syslog_requires = ["systemd-python"]

dev_requires = [
    "black",
    "bump2version",
    "flake8",
    "mypy",
    "pre-commit",
    "pytest",
    "pytest-cov",
]

docs_require = [
    "sphinx",
    "m2r2",
    "sphinx-autoapi",
    "sphinx_rtd_theme",
]

setup(
    name="maestral",
    author="Sam Schott",
    author_email="ss2151@cam.ac.uk",
    version="1.3.2.dev0",
    url="https://github.com/SamSchott/maestral",
    description="Open-source Dropbox client for macOS and Linux.",
    license="MIT",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={
        "maestral": ["resources/*"],
    },
    setup_requires=["wheel"],
    install_requires=install_requires,
    extras_require={
        "gui": gui_requires,
        "syslog": syslog_requires,
        "dev": dev_requires,
        "docs": docs_require,
    },
    zip_safe=False,
    entry_points={
        "console_scripts": ["maestral=maestral.cli:main"],
        "pyinstaller40": ["hook-dirs=maestral.__pyinstaller:get_hook_dirs"],
    },
    python_requires=">=3.6",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
    data_files=[
        ("share/icons/hicolor/512x512/apps", ["src/maestral/resources/maestral.png"])
    ],
)
