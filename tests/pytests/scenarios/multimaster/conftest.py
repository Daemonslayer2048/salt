import logging
import os
import pathlib
import shutil

import pytest
from tests.support.runtests import RUNTIME_VARS

log = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def ext_pillar_file_tree():
    pillar_file_tree = {
        "root_dir": str(pathlib.Path(RUNTIME_VARS.PILLAR_DIR) / "base" / "file_tree"),
        "follow_dir_links": False,
        "keep_newline": True,
    }
    return {"file_tree": pillar_file_tree}


@pytest.fixture(scope="package")
def salt_mm_master_1(request, salt_factories, ext_pillar_file_tree):
    config_defaults = {
        "ext_pillar": [ext_pillar_file_tree],
        "open_mode": True,
        "transport": request.config.getoption("--transport"),
    }

    config_overrides = {
        "file_roots": {
            "base": [
                RUNTIME_VARS.TMP_STATE_TREE,
                os.path.join(RUNTIME_VARS.FILES, "file", "base"),
            ],
            # Alternate root to test __env__ choices
            "prod": [
                RUNTIME_VARS.TMP_PRODENV_STATE_TREE,
                os.path.join(RUNTIME_VARS.FILES, "file", "prod"),
            ],
        },
        "pillar_roots": {
            "base": [
                RUNTIME_VARS.TMP_PILLAR_TREE,
                os.path.join(RUNTIME_VARS.FILES, "pillar", "base"),
            ],
            "prod": [RUNTIME_VARS.TMP_PRODENV_PILLAR_TREE],
        },
    }
    factory = salt_factories.get_salt_master_daemon(
        "mm-master-1",
        config_defaults=config_defaults,
        config_overrides=config_overrides,
        extra_cli_arguments_after_first_start_failure=["--log-level=debug"],
    )
    with factory.started():
        yield factory


@pytest.fixture(scope="package")
def mm_master_1_salt_cli(salt_mm_master_1):
    return salt_mm_master_1.get_salt_cli()


@pytest.fixture(scope="package")
def salt_mm_master_2(salt_factories, salt_mm_master_1, ext_pillar_file_tree):

    config_defaults = {
        "ext_pillar": [ext_pillar_file_tree],
        "open_mode": True,
        "transport": salt_mm_master_1.config["transport"],
    }

    config_overrides = {
        "file_roots": {
            "base": [
                RUNTIME_VARS.TMP_STATE_TREE,
                os.path.join(RUNTIME_VARS.FILES, "file", "base"),
            ],
            # Alternate root to test __env__ choices
            "prod": [
                RUNTIME_VARS.TMP_PRODENV_STATE_TREE,
                os.path.join(RUNTIME_VARS.FILES, "file", "prod"),
            ],
        },
        "pillar_roots": {
            "base": [
                RUNTIME_VARS.TMP_PILLAR_TREE,
                os.path.join(RUNTIME_VARS.FILES, "pillar", "base"),
            ],
            "prod": [RUNTIME_VARS.TMP_PRODENV_PILLAR_TREE],
        },
    }

    factory = salt_factories.get_salt_master_daemon(
        "mm-master-2",
        config_defaults=config_defaults,
        config_overrides=config_overrides,
        extra_cli_arguments_after_first_start_failure=["--log-level=debug"],
    )

    # The secondary salt master depends on the primarily salt master fixture
    # because we need to clone the keys
    for keyfile in ("master.pem", "master.pub"):
        shutil.copyfile(
            os.path.join(salt_mm_master_1.config["pki_dir"], keyfile),
            os.path.join(factory.config["pki_dir"], keyfile),
        )
    with factory.started():
        yield factory


@pytest.fixture(scope="package")
def mm_master_2_salt_cli(salt_mm_master_2):
    return salt_mm_master_2.get_salt_cli()


@pytest.fixture(scope="package")
def salt_mm_minion_1(salt_mm_master_1, salt_mm_master_2):
    config_defaults = {
        "hosts.file": os.path.join(RUNTIME_VARS.TMP, "hosts"),
        "aliases.file": os.path.join(RUNTIME_VARS.TMP, "aliases"),
        "transport": salt_mm_master_1.config["transport"],
    }

    mm_master_1_port = salt_mm_master_1.config["ret_port"]
    mm_master_2_port = salt_mm_master_2.config["ret_port"]
    config_overrides = {
        "master": [
            "localhost:{}".format(mm_master_1_port),
            "localhost:{}".format(mm_master_2_port),
        ],
        "test.foo": "baz",
    }
    factory = salt_mm_master_1.get_salt_minion_daemon(
        "mm-minion-1",
        config_defaults=config_defaults,
        config_overrides=config_overrides,
        extra_cli_arguments_after_first_start_failure=["--log-level=debug"],
    )
    with factory.started():
        yield factory


@pytest.fixture(scope="package")
def salt_mm_minion_2(salt_mm_master_1, salt_mm_master_2):
    config_defaults = {
        "hosts.file": os.path.join(RUNTIME_VARS.TMP, "hosts"),
        "aliases.file": os.path.join(RUNTIME_VARS.TMP, "aliases"),
        "transport": salt_mm_master_1.config["transport"],
    }

    mm_master_1_port = salt_mm_master_1.config["ret_port"]
    mm_master_2_port = salt_mm_master_2.config["ret_port"]
    config_overrides = {
        "master": [
            "localhost:{}".format(mm_master_1_port),
            "localhost:{}".format(mm_master_2_port),
        ],
        "test.foo": "baz",
    }
    factory = salt_mm_master_2.get_salt_minion_daemon(
        "mm-minion-2",
        config_defaults=config_defaults,
        config_overrides=config_overrides,
        extra_cli_arguments_after_first_start_failure=["--log-level=debug"],
    )
    with factory.started():
        yield factory
