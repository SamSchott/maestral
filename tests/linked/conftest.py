# -*- coding: utf-8 -*-

import os
import logging
import time
from datetime import datetime
import uuid

import pytest
from watchdog.utils.dirsnapshot import DirectorySnapshot
from dropbox.files import WriteMode, FileMetadata

from maestral.main import Maestral, logger
from maestral.errors import NotFoundError, FileConflictError
from maestral.client import convert_api_errors
from maestral.config import remove_configuration
from maestral.utils.path import (
    generate_cc_name,
    delete,
    to_existing_cased_path,
    is_child,
)
from maestral.utils.appdirs import get_home_dir
from maestral.daemon import MaestralProxy
from maestral.daemon import start_maestral_daemon_process, stop_maestral_daemon_process


resources = os.path.dirname(__file__) + "/resources"

logger.setLevel(logging.DEBUG)


@pytest.fixture
def m():
    config_name = "test-config"

    m = Maestral(config_name)
    m.log_level = logging.DEBUG

    # link with given token
    access_token = os.environ.get("DROPBOX_ACCESS_TOKEN")
    refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")

    if access_token:
        m.client._init_sdk_with_token(access_token=access_token)
        m.client.auth._access_token = access_token
        m.client.auth._token_access_type = "legacy"
    elif refresh_token:
        m.client._init_sdk_with_token(refresh_token=refresh_token)
        m.client.auth._refresh_token = refresh_token
        m.client.auth._token_access_type = "offline"
    else:
        raise RuntimeError(
            "Either access token or refresh token must be given as environment "
            "variable DROPBOX_ACCESS_TOKEN or DROPBOX_REFRESH_TOKEN."
        )

    # get corresponding Dropbox ID and store in keyring for other processes
    res = m.client.get_account_info()
    m.client.auth._account_id = res.account_id
    m.client.auth.loaded = True
    m.client.auth.save_creds()

    # set local Dropbox directory
    home = get_home_dir()
    local_dropbox_dir = generate_cc_name(home + "/Dropbox", suffix="test runner")
    m.create_dropbox_directory(local_dropbox_dir)

    # acquire test lock and perform initial sync
    lock = DropboxTestLock(m)
    if not lock.acquire(timeout=60 * 60):
        raise TimeoutError("Could not acquire test lock")

    # create / clean our temporary test folder
    m.test_folder_dbx = "/sync_tests"
    m.test_folder_local = m.to_local_path(m.test_folder_dbx)

    try:
        m.client.remove(m.test_folder_dbx)
    except NotFoundError:
        pass
    m.client.make_dir(m.test_folder_dbx)

    # start syncing
    m.start_sync()
    wait_for_idle(m)

    # return synced and running instance
    yield m

    # stop syncing and clean up remote folder
    m.stop_sync()

    try:
        m.client.remove(m.test_folder_dbx)
    except NotFoundError:
        pass

    try:
        m.client.remove("/.mignore")
    except NotFoundError:
        pass

    # remove creds from system keyring
    m.client.auth.delete_creds()

    # remove local files and folders
    delete(m.dropbox_path)
    remove_configuration(m.config_name)

    # release lock
    lock.release()


@pytest.fixture
def proxy(m):
    m.stop_sync()
    start_maestral_daemon_process(m.config_name, timeout=20)
    yield MaestralProxy(m.config_name)

    stop_maestral_daemon_process(m.config_name)


# helper functions


def wait_for_idle(m: Maestral, minimum: int = 6):
    """Blocks until Maestral instance is idle for at least `minimum` sec."""

    t0 = time.time()
    while time.time() - t0 < minimum:
        if m.sync.busy():
            # Wait until we can acquire the sync lock => we are idle.
            m.sync.sync_lock.acquire()
            m.sync.sync_lock.release()
            t0 = time.time()
        else:
            time.sleep(0.1)


def assert_synced(m: Maestral):
    """Asserts that the `local_folder` and `remote_folder` are synced."""

    listing = m.client.list_folder("/", recursive=True)
    local_snapshot = DirectorySnapshot(m.dropbox_path)

    # assert that all items from server are present locally
    # with the same content hash
    for e in listing.entries:
        dbx_path = e.path_display
        local_path = to_existing_cased_path(str(dbx_path), root=m.dropbox_path)

        remote_hash = e.content_hash if isinstance(e, FileMetadata) else "folder"
        assert (
            m.sync.get_local_hash(local_path) == remote_hash
        ), f'different file content for "{dbx_path}"'

    # assert that all local items are present on server
    for path in local_snapshot.paths:
        if not m.sync.is_excluded(path) and is_child(path, m.dropbox_path):
            if not m.sync.is_excluded(path):
                dbx_path = m.sync.to_dbx_path(path).lower()
                matching_items = list(
                    e for e in listing.entries if e.path_lower == dbx_path
                )
                assert (
                    len(matching_items) == 1
                ), f'local item "{path}" does not exist on dbx'

    # check that our index is correct
    for index_entry in m.sync.get_index():

        if is_child(index_entry.dbx_path_lower, "/"):
            # check that there is a match on the server
            matching_items = list(
                e for e in listing.entries if e.path_lower == index_entry.dbx_path_lower
            )
            assert (
                len(matching_items) == 1
            ), f'indexed item "{index_entry.dbx_path_lower}" does not exist on dbx'

            e = matching_items[0]
            remote_rev = e.rev if isinstance(e, FileMetadata) else "folder"

            # check if revs are equal on server and locally
            assert (
                index_entry.rev == remote_rev
            ), f'different revs for "{index_entry.dbx_path_lower}"'

            # check if casing on drive is the same as in index
            local_path_expected_casing = m.dropbox_path + index_entry.dbx_path_cased
            local_path_actual_casing = to_existing_cased_path(
                local_path_expected_casing
            )

            assert (
                local_path_expected_casing == local_path_actual_casing
            ), "casing on drive does not match index"


# test lock


class DropboxTestLock:
    """
    A lock on a Dropbox account for running sync tests. The lock will be acquired by
    create a file at ``lock_path`` and released by deleting the file on the remote
    Dropbox. This can be used to synchronise tests running on the same Dropbox account.
    Lock files older than 1h are considered expired and will be discarded.

    :param m: Linked Maestral instance.
    :param lock_path: Path for the lock folder.
    :param expires_after: The lock will be considered as expired after the given time in
        seconds since the acquire call. Defaults to 15 min.
    """

    def __init__(
        self,
        m: Maestral,
        lock_path: str = "/test.lock",
        expires_after: float = 15 * 60,
    ) -> None:

        self.m = m
        self.lock_path = lock_path
        self.expires_after = expires_after
        self._rev = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """
        Acquires the lock. When invoked with the blocking argument set to True (the
        default), this blocks until the lock is unlocked, then sets it to locked and
        returns True. When invoked with the blocking argument set to False, this call
        does not block. If the lock cannot be acquired, returns False immediately;
        otherwise, sets the lock to locked and returns True.

        :param blocking: Whether to block until the lock can be acquired.
        :param timeout: Timeout in seconds. If negative, no timeout will be applied.
            If positive, blocking must be set to True.
        :returns: Whether the lock could be acquired (within timeout).
        """

        if not blocking and timeout > 0:
            raise ValueError("can't specify a timeout for a non-blocking call")

        t0 = time.time()

        # we encode the expiry time in the client_modified time stamp
        expiry_time = datetime.utcfromtimestamp(time.time() + self.expires_after)

        while True:
            try:
                with convert_api_errors(dbx_path=self.lock_path):
                    md = self.m.client.dbx.files_upload(
                        uuid.uuid4().bytes,
                        self.lock_path,
                        mode=WriteMode.add,
                        client_modified=expiry_time,
                    )
                    self._rev = md.rev
            except FileConflictError:
                if not self.locked():
                    continue
            else:
                return True

            if time.time() - t0 > timeout > 0:
                return False
            else:
                time.sleep(5)

    def locked(self):
        """
        Check if locked. Clean up any expired lock files.

        :returns: True if locked, False otherwise.
        """

        md = self.m.client.get_metadata(self.lock_path)

        if not md:
            return False

        elif isinstance(md, FileMetadata) and md.client_modified < datetime.utcnow():
            # lock has expired, remove
            try:
                self.m.client.remove(self.lock_path, parent_rev=md.rev)
            except NotFoundError:
                # protect against race
                pass

            return False
        else:
            return True

    def release(self) -> None:
        """
        Releases the lock.

        :raises: RuntimeError if the lock was not locked.
        """

        if not self._rev:
            raise RuntimeError("release unlocked lock")

        try:
            self.m.client.remove(self.lock_path, parent_rev=self._rev)
        except NotFoundError:
            raise RuntimeError("release unlocked lock")
        else:
            self._rev = None
