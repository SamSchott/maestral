# -*- coding: utf-8 -*-
"""This module contains the main syncing functionality."""

# system imports
import sys
import os
import os.path as osp
from stat import S_ISDIR
import socket
import resource
import logging
import time
import random
import uuid
import urllib.parse
import enum
import pprint
import gc
from threading import Thread, Event, RLock, current_thread
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from collections import abc
from contextlib import contextmanager
from functools import wraps
from tempfile import NamedTemporaryFile
from typing import (
    Optional,
    Any,
    Set,
    List,
    Dict,
    Tuple,
    Union,
    Iterator,
    Callable,
    Hashable,
    Type,
    cast,
    TypeVar,
)
from types import TracebackType

# external imports
import click
import sqlalchemy.exc  # type: ignore
import sqlalchemy.engine.url  # type: ignore
from sqlalchemy.sql import func  # type: ignore
from sqlalchemy import create_engine  # type: ignore
import pathspec  # type: ignore
import dropbox  # type: ignore
from dropbox.files import Metadata, DeletedMetadata, FileMetadata, FolderMetadata  # type: ignore
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler  # type: ignore
from watchdog.events import EVENT_TYPE_CREATED, EVENT_TYPE_DELETED, EVENT_TYPE_MOVED
from watchdog.events import (
    DirModifiedEvent,
    FileModifiedEvent,
    DirCreatedEvent,
    FileCreatedEvent,
    DirDeletedEvent,
    FileDeletedEvent,
    DirMovedEvent,
    FileMovedEvent,
    FileSystemEvent,
)
from watchdog.utils.dirsnapshot import DirectorySnapshot  # type: ignore

# local imports
from . import notify
from .config import MaestralConfig, MaestralState
from .notify import MaestralDesktopNotifier
from .constants import (
    IDLE,
    SYNCING,
    PAUSED,
    STOPPED,
    DISCONNECTED,
    EXCLUDED_FILE_NAMES,
    EXCLUDED_DIR_NAMES,
    MIGNORE_FILE,
    FILE_CACHE,
)
from .errors import (
    SyncError,
    NoDropboxDirError,
    CacheDirError,
    PathError,
    NotFoundError,
    DropboxServerError,
    FileConflictError,
    FolderConflictError,
    InvalidDbidError,
    DatabaseError,
)
from .client import (
    DropboxClient,
    os_to_maestral_error,
    convert_api_errors,
    fswatch_to_maestral_error,
)
from .database import (
    Base,
    Session,
    SyncEvent,
    HashCacheEntry,
    IndexEntry,
    SyncDirection,
    SyncStatus,
    ItemType,
    ChangeType,
)
from .utils import removeprefix, sanitize_string
from .utils.caches import LRUCache
from .utils.path import (
    generate_cc_name,
    cased_path_candidates,
    is_fs_case_sensitive,
    move,
    delete,
    is_child,
    is_equal_or_child,
    content_hash,
)
from .utils.appdirs import get_data_path, get_home_dir


__all__ = [
    "Conflict",
    "SyncDirection",
    "SyncStatus",
    "ItemType",
    "ChangeType",
    "FSEventHandler",
    "PersistentStateMutableSet",
    "SyncEvent",
    "IndexEntry",
    "HashCacheEntry",
    "SyncEngine",
    "SyncMonitor",
    "upload_worker",
    "download_worker",
    "download_worker_added_item",
    "startup_worker",
]


logger = logging.getLogger(__name__)
cpu_count = os.cpu_count() or 1  # os.cpu_count can return None
umask = os.umask(0o22)
os.umask(umask)

# type definitions
ExecInfoType = Tuple[Type[BaseException], BaseException, Optional[TracebackType]]
FT = TypeVar("FT", bound=Callable[..., Any])


# ======================================================================================
# Syncing functionality
# ======================================================================================


class Conflict(enum.Enum):
    """Enumeration of sync conflict types"""

    RemoteNewer = "remote newer"
    Conflict = "conflict"
    Identical = "identical"
    LocalNewerOrIdentical = "local newer or identical"


class _Ignore:
    def __init__(
        self,
        event: FileSystemEvent,
        start_time: float,
        ttl: Optional[float],
        recursive: bool,
    ) -> None:
        self.event = event
        self.start_time = start_time
        self.ttl = ttl
        self.recursive = recursive


class FSEventHandler(FileSystemEventHandler):
    """A local file event handler

    Handles captured file events and adds them to :class:`SyncEngine`'s file event queue
    to be uploaded by :meth:`upload_worker`. This acts as a translation layer between
    :class:`watchdog.Observer` and :class:`SyncEngine`.

    :param syncing: Set when syncing is running.
    :param startup: Set when startup is running.

    :cvar float ignore_timeout: Timeout in seconds after which ignored paths will be
        discarded.
    """

    _ignored_events: List[_Ignore]
    local_file_event_queue: "Queue[FileSystemEvent]"

    def __init__(self, syncing: Event, startup: Event) -> None:

        self.syncing = syncing
        self.startup = startup

        self._ignored_events = []
        self.ignore_timeout = 2.0
        self.local_file_event_queue = Queue()

    @contextmanager
    def ignore(
        self, *events: FileSystemEvent, recursive: bool = True
    ) -> Iterator[None]:
        """A context manager to ignore local file events

        Once a matching event has been registered, further matching events will no
        longer be ignored unless ``recursive`` is ``True``. If no matching event has
        occurred before leaving the context, the event will be ignored for
        :attr:`ignore_timeout` sec after leaving then context and then discarded. This
        accounts for possible delays in the emission of local file system events.

        This context manager is used to filter out file system events caused by maestral
        itself, for instance during a download or when moving a conflict.

        :param events: Local events to ignore.
        :param recursive: If ``True``, all child events of a directory event will be
            ignored as well. This parameter will be ignored for file events.
        """

        now = time.time()
        new_ignores = []
        for e in events:
            new_ignores.append(
                _Ignore(
                    event=e,
                    start_time=now,
                    ttl=None,
                    recursive=recursive and e.is_directory,
                )
            )
        self._ignored_events.extend(new_ignores)  # this is atomic

        try:
            yield
        finally:
            for ignore in new_ignores:
                ignore.ttl = time.time() + self.ignore_timeout

    def expire_ignored_events(self) -> None:
        """Removes all expired ignore entries."""

        now = time.time()
        for ignore in self._ignored_events.copy():
            ttl = ignore.ttl
            if ttl and ttl < now:
                try:
                    self._ignored_events.remove(ignore)
                except ValueError:
                    # someone else removed it in the meantime
                    pass

    def _is_ignored(self, event: FileSystemEvent) -> bool:
        """
        Checks if a file system event should been explicitly ignored because it was
        triggered by Maestral itself.

        :param event: Local file system event.
        :returns: Whether the event should be ignored.
        """

        self.expire_ignored_events()

        for ignore in self._ignored_events.copy():
            ignore_event = ignore.event
            recursive = ignore.recursive

            if event == ignore_event:

                if not recursive:
                    try:
                        self._ignored_events.remove(ignore)
                    except ValueError:
                        pass

                return True

            elif recursive:

                type_match = event.event_type == ignore_event.event_type
                src_match = is_equal_or_child(event.src_path, ignore_event.src_path)
                dest_match = is_equal_or_child(
                    get_dest_path(event), get_dest_path(ignore_event)
                )

                if type_match and src_match and dest_match:
                    return True

        return False

    def on_any_event(self, event: FileSystemEvent) -> None:
        """
        Checks if the system file event should be ignored. If not, adds it to the queue
        for events to upload. If syncing is paused or stopped, all events will be
        ignored.

        :param event: Watchdog file event.
        """

        # ignore events if we are not during startup or sync
        if not (self.syncing.is_set() or self.startup.is_set()):
            return

        # ignore all DirMovedEvents
        if isinstance(event, DirModifiedEvent):
            return

        # check if event should be ignored
        if self._is_ignored(event):
            return

        self.local_file_event_queue.put(event)


class PersistentStateMutableSet(abc.MutableSet):
    """Wraps a list in our state file as a MutableSet

    :param config_name: Name of config (determines name of state file).
    :param section: Section name in state file.
    :param option: Option name in state file.
    """

    def __init__(self, config_name: str, section: str, option: str) -> None:
        super().__init__()
        self.config_name = config_name
        self.section = section
        self.option = option
        self._state = MaestralState(config_name)
        self._lock = RLock()

    def __iter__(self) -> Iterator[Any]:
        with self._lock:
            return iter(self._state.get(self.section, self.option))

    def __contains__(self, entry: Any) -> bool:
        with self._lock:
            return entry in self._state.get(self.section, self.option)

    def __len__(self):
        with self._lock:
            return len(self._state.get(self.section, self.option))

    def add(self, entry: Any) -> None:
        with self._lock:
            state_list = self._state.get(self.section, self.option)
            state_list = set(state_list)
            state_list.add(entry)
            self._state.set(self.section, self.option, list(state_list))

    def discard(self, entry: Any) -> None:
        with self._lock:
            state_list = self._state.get(self.section, self.option)
            state_list = set(state_list)
            state_list.discard(entry)
            self._state.set(self.section, self.option, list(state_list))

    def update(self, *others: Any) -> None:
        with self._lock:
            state_list = self._state.get(self.section, self.option)
            state_list = set(state_list)
            state_list.update(*others)
            self._state.set(self.section, self.option, list(state_list))

    def difference_update(self, *others: Any) -> None:
        with self._lock:
            state_list = self._state.get(self.section, self.option)
            state_list = set(state_list)
            state_list.difference_update(*others)
            self._state.set(self.section, self.option, list(state_list))

    def clear(self) -> None:
        """Clears all elements."""
        with self._lock:
            self._state.set(self.section, self.option, [])

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__}(section='{self.section}',"
            f"option='{self.option}', entries={list(self)})>"
        )


class SyncEngine:
    """Class that handles syncing with Dropbox

    Provides methods to wait for local or remote changes and sync them, including
    conflict resolution and updates to our index.

    :param client: Dropbox API client instance.
    :param fs_events_handler: File system event handler to inform us of local events.
    """

    sync_errors: Set[SyncError]
    syncing: List[SyncEvent]
    _case_conversion_cache: LRUCache

    _max_history = 1000
    _num_threads = min(32, cpu_count * 3)

    def __init__(self, client: DropboxClient, fs_events_handler: FSEventHandler):

        self.client = client
        self.config_name = self.client.config_name
        self.fs_events = fs_events_handler

        self.sync_lock = RLock()
        self._db_lock = RLock()

        self._conf = MaestralConfig(self.config_name)
        self._state = MaestralState(self.config_name)

        self.notifier = notify.MaestralDesktopNotifier(self.config_name)

        # upload_errors / download_errors: contains failed uploads / downloads
        # (from sync errors) to retry later
        self.upload_errors = PersistentStateMutableSet(
            self.config_name, section="sync", option="upload_errors"
        )
        self.download_errors = PersistentStateMutableSet(
            self.config_name, section="sync", option="download_errors"
        )
        # pending_uploads / pending_downloads: contains interrupted uploads / downloads
        # to retry later. Running uploads / downloads can be stored in these lists to be
        # resumed if Maestral quits unexpectedly. This used for downloads which are not
        # part of the regular sync cycle and are therefore not restarted automatically.
        self.pending_downloads = PersistentStateMutableSet(
            self.config_name, section="sync", option="pending_downloads"
        )
        self.pending_uploads = PersistentStateMutableSet(
            self.config_name, section="sync", option="pending_uploads"
        )

        # data structures for internal communication
        self.sync_errors = set()
        self._cancel_requested = Event()

        # data structures for user information
        self.syncing = []

        # determine file paths
        self._dropbox_path = self._conf.get("main", "path")
        self._mignore_path = osp.join(self._dropbox_path, MIGNORE_FILE)
        self._file_cache_path = osp.join(self._dropbox_path, FILE_CACHE)
        self._db_path = get_data_path("maestral", f"{self.config_name}.db")

        # reset sync state if DB is missing
        if not osp.exists(self._db_path):
            self.remote_cursor = ""

        # initialize SQLite database
        url = sqlalchemy.engine.url.URL(
            drivername="sqlite",
            database=f"file:{self._db_path}",
            query={"check_same_thread": "false", "uri": "true"},
        )
        self._db_engine = create_engine(url)
        with self._database_access(log_errors=True):
            Base.metadata.create_all(self._db_engine)
            Session.configure(bind=self._db_engine)
        self._db_session = Session()

        # load cached properties
        self._is_case_sensitive = is_fs_case_sensitive(get_home_dir())
        self._mignore_rules = self._load_mignore_rules_form_file()
        self._excluded_items = self._conf.get("main", "excluded_items")
        self._max_cpu_percent = self._conf.get("sync", "max_cpu_percent") * cpu_count

        # caches
        self._case_conversion_cache = LRUCache(capacity=5000)

        # clean our file cache
        self.clean_cache_dir()

    # ==== config access ===============================================================

    @property
    def dropbox_path(self) -> str:
        """
        Path to local Dropbox folder, as loaded from the config file. Before changing
        :attr:`dropbox_path`, make sure that syncing is paused. Move the dropbox folder
        to the new location before resuming the sync. Changes are saved to the config
        file.
        """
        return self._dropbox_path

    @dropbox_path.setter
    def dropbox_path(self, path: str) -> None:
        """Setter: dropbox_path"""

        path = osp.realpath(path)

        with self.sync_lock:
            self._dropbox_path = path
            self._mignore_path = osp.join(self._dropbox_path, MIGNORE_FILE)
            self._file_cache_path = osp.join(self._dropbox_path, FILE_CACHE)
            self._is_case_sensitive = is_fs_case_sensitive(self._dropbox_path)
            self._conf.set("main", "path", path)

    @property
    def database_path(self) -> str:
        """Path SQLite database."""
        return self._db_path

    @property
    def file_cache_path(self) -> str:
        """Path to cache folder for temporary files (read only). The cache folder
        '.maestral.cache' is located inside the local Dropbox folder to prevent file
        transfer between different partitions or drives during sync."""
        return self._file_cache_path

    @property
    def excluded_items(self) -> List[str]:
        """List of all files and folders excluded from sync. Changes are saved to the
        config file. If a parent folder is excluded, its children will automatically be
        removed from the list. If only children are given but not the parent folder, any
        new items added to the parent will be synced. Change this property *before*
        downloading newly included items or deleting excluded items."""
        return self._excluded_items

    @excluded_items.setter
    def excluded_items(self, folder_list: List[str]) -> None:
        """Setter: excluded_items"""

        with self.sync_lock:
            clean_list = self.clean_excluded_items_list(folder_list)
            self._excluded_items = clean_list
            self._conf.set("main", "excluded_items", clean_list)

    @staticmethod
    def clean_excluded_items_list(folder_list: List[str]) -> List[str]:
        """
        Removes all duplicates and children of excluded items from the excluded items
        list.

        :param folder_list: Dropbox paths to exclude.
        :returns: Cleaned up items.
        """

        # remove duplicate entries by creating set, strip trailing '/'
        folder_set = set(f.lower().rstrip("/") for f in folder_list)

        # remove all children of excluded folders
        clean_list = list(folder_set)
        for folder in folder_set:
            clean_list = [f for f in clean_list if not is_child(f, folder)]

        return clean_list

    @property
    def max_cpu_percent(self) -> float:
        """Maximum CPU usage for parallel downloads or uploads in percent of the total
        available CPU time per core. Individual workers in a thread pool will pause
        until the usage drops below this value. Tasks in the main thread such as
        indexing file changes may still use more CPU time. Setting this to 200% means
        that two full logical CPU core can be used."""
        return self._max_cpu_percent

    @max_cpu_percent.setter
    def max_cpu_percent(self, percent: float) -> None:
        """Setter: max_cpu_percent."""
        self._max_cpu_percent = percent
        self._conf.set("app", "max_cpu_percent", percent // cpu_count)

    # ==== sync state ==================================================================

    @property
    def remote_cursor(self) -> str:
        """Cursor from last sync with remote Dropbox. The value is updated and saved to
        the config file on every successful download of remote changes."""
        return self._state.get("sync", "cursor")

    @remote_cursor.setter
    def remote_cursor(self, cursor: str) -> None:
        """Setter: last_cursor"""
        with self.sync_lock:
            self._state.set("sync", "cursor", cursor)
            logger.debug("Remote cursor saved: %s", cursor)

    @property
    def local_cursor(self) -> float:
        """Time stamp from last sync with remote Dropbox. The value is updated and saved
        to the config file on every successful upload of local changes."""
        return self._state.get("sync", "lastsync")

    @local_cursor.setter
    def local_cursor(self, last_sync: float) -> None:
        """Setter: local_cursor"""
        with self.sync_lock:
            logger.debug("Local cursor saved: %s", last_sync)
            self._state.set("sync", "lastsync", last_sync)

    @property
    def last_change(self) -> float:
        """The time stamp of the last file change or 0.0 if there are no file changes in
        our history."""

        with self._database_access():

            res = self._db_session.query(func.max(IndexEntry.last_sync)).first()

            if res:
                return res[0] or 0.0
            else:
                return 0.0

    @property
    def last_reindex(self) -> float:
        """Time stamp of last full indexing. This is used to determine when the next
        full indexing should take place."""
        return self._state.get("sync", "last_reindex")

    @property
    def history(self) -> List[SyncEvent]:
        """A list of the last SyncEvents in our history. History will be kept for the
        interval specified by the config value ``keep_history`` (defaults to two weeks)
        but at most 1,000 events will be kept."""
        with self._database_access():
            query = self._db_session.query(SyncEvent)
            ordered_query = query.order_by(SyncEvent.change_time_or_sync_time)
            return ordered_query.limit(self._max_history).all()

    def clear_sync_history(self) -> None:
        """Clears the sync history."""
        with self._database_access():
            SyncEvent.metadata.drop_all(self._db_engine)
            Base.metadata.create_all(self._db_engine)
            self._db_session.expunge_all()

    # ==== index management ============================================================

    def get_index(self) -> List[IndexEntry]:
        """
        Returns a copy of the local index of synced files and folders.

        :returns: List of index entries.
        """
        with self._database_access():
            return self._db_session.query(IndexEntry).all()

    def iter_index(self) -> Iterator[IndexEntry]:
        """
        Returns an iterator over the local index of synced files and folders.

        :returns: Iterator over index entries.
        """
        with self._database_access():
            for entry in self._db_session.query(IndexEntry).yield_per(1000):
                yield entry

    def get_local_rev(self, dbx_path: str) -> Optional[str]:
        """
        Gets revision number of local file.

        :param dbx_path: Dropbox path.
        :returns: Revision number as str or ``None`` if no local revision number has
            been saved.
        """

        with self._database_access():
            res = (
                self._db_session.query(IndexEntry.rev)
                .filter(IndexEntry.dbx_path_lower == dbx_path.lower())
                .first()
            )

        if res:
            return res[0]
        else:
            return None

    def get_last_sync(self, dbx_path: str) -> float:
        """
        Returns the timestamp of last sync for an individual path.

        :param dbx_path: Dropbox path.
        :returns: Time of last sync.
        """

        with self._database_access():
            res = self._db_session.query(IndexEntry).get(dbx_path.lower())

        if res:
            last_sync = res.last_sync or 0.0
        else:
            last_sync = 0.0

        return max(last_sync, self.local_cursor)

    def get_index_entry(self, dbx_path: str) -> Optional[IndexEntry]:
        """
        Gets the index entry for the given Dropbox path.

        :param dbx_path: Dropbox path.
        :returns: Index entry or ``None`` if no entry exists for the given path.
        """

        with self._database_access():
            return self._db_session.query(IndexEntry).get(dbx_path.lower())

    def get_local_hash(self, local_path: str) -> Optional[str]:
        """
        Computes content hash of a local file.

        :param local_path: Absolute path on local drive.
        :returns: Content hash to compare with Dropbox's content hash, or 'folder' if
            the path points to a directory. ``None`` if there is nothing at the path.
        """

        try:
            stat = os.stat(local_path)
        except (FileNotFoundError, NotADirectoryError):
            # remove any existing cache entries for path
            with self._database_access():
                cache_entry = self._db_session.query(HashCacheEntry).get(local_path)
                if cache_entry:
                    self._db_session.delete(cache_entry)
            return None
        except OSError as err:
            raise os_to_maestral_error(err, local_path=local_path)

        if S_ISDIR(stat.st_mode):
            # take shortcut: return 'folder'
            return "folder"

        mtime: Optional[float] = stat.st_mtime

        with self._database_access():
            # check cache for an up-to-date content hash and return if it exists
            cache_entry = self._db_session.query(HashCacheEntry).get(local_path)

            if cache_entry and cache_entry.mtime == mtime:
                return cache_entry.hash_str

        with convert_api_errors(local_path=local_path):
            hash_str, mtime = content_hash(local_path)

        self.save_local_hash(local_path, hash_str, mtime)

        return hash_str

    def save_local_hash(
        self, local_path: str, hash_str: Optional[str], mtime: Optional[float]
    ) -> None:
        """
        Save the content hash for a file in our cache.

        :param local_path: Absolute path on local drive.
        :param hash_str: Hash string to save. If None, the existing cache entry will be
            deleted.
        :param mtime: Mtime of the file when the hash was computed.
        """

        with self._database_access():

            cache_entry = self._db_session.query(HashCacheEntry).get(local_path)

            if hash_str:

                if cache_entry:
                    cache_entry.hash_str = hash_str
                    cache_entry.mtime = mtime

                else:
                    cache_entry = HashCacheEntry(
                        local_path=local_path, hash_str=hash_str, mtime=mtime
                    )
                    self._db_session.add(cache_entry)
            else:
                if cache_entry:
                    self._db_session.delete(cache_entry)
                else:
                    pass

            self._db_session.commit()

    def clear_hash_cache(self) -> None:
        """Clears the sync history."""
        with self._database_access():
            HashCacheEntry.metadata.drop_all(self._db_engine)
            Base.metadata.create_all(self._db_engine)
            self._db_session.expunge_all()

    def update_index_from_sync_event(self, event: SyncEvent) -> None:
        """
        Updates the local index from a SyncEvent.

        :param event: SyncEvent from download.
        """

        if event.change_type is not ChangeType.Removed and not event.rev:
            raise ValueError("Rev required to update index")

        dbx_path_lower = event.dbx_path.lower()

        with self._database_access():

            # remove any entries for deleted or moved items

            if event.change_type is ChangeType.Removed:
                self.remove_node_from_index(dbx_path_lower)
            elif event.change_type is ChangeType.Moved:
                self.remove_node_from_index(event.dbx_path_from.lower())

            # add or update entries for created or modified items

            if event.change_type is not ChangeType.Removed:

                entry = self.get_index_entry(dbx_path_lower)

                if entry:
                    # update existing entry
                    entry.dbx_id = event.dbx_id
                    entry.dbx_path_cased = event.dbx_path
                    entry.item_type = event.item_type
                    entry.last_sync = self._get_ctime(event.local_path)
                    entry.rev = event.rev
                    entry.content_hash = event.content_hash

                else:
                    # create new entry
                    entry = IndexEntry(
                        dbx_path_cased=event.dbx_path,
                        dbx_path_lower=dbx_path_lower,
                        dbx_id=event.dbx_id,
                        item_type=event.item_type,
                        last_sync=self._get_ctime(event.local_path),
                        rev=event.rev,
                        content_hash=event.content_hash,
                    )

                    self._db_session.add(entry)

            self._db_session.commit()

    def update_index_from_dbx_metadata(self, md: Metadata) -> None:
        """
        Updates the local index from Dropbox metadata.

        :param md: Dropbox metadata.
        """

        with self._database_access():

            if isinstance(md, DeletedMetadata):
                self.remove_node_from_index(md.path_lower)

            else:

                if isinstance(md, FileMetadata):
                    rev = md.rev
                    hash_str = md.content_hash
                    item_type = ItemType.File
                else:
                    rev = "folder"
                    hash_str = "folder"
                    item_type = ItemType.Folder

                # construct correct display path from ancestors

                entry = self.get_index_entry(md.path_lower)
                dbx_path_cased = self.correct_case(md.path_display)

                if entry:
                    entry.dbx_id = md.id
                    entry.dbx_path_cased = dbx_path_cased
                    entry.item_type = item_type
                    entry.last_sync = None
                    entry.rev = rev
                    entry.content_hash = hash_str

                else:
                    entry = IndexEntry(
                        dbx_path_cased=dbx_path_cased,
                        dbx_path_lower=md.path_lower,
                        dbx_id=md.id,
                        item_type=item_type,
                        last_sync=None,
                        rev=rev,
                        content_hash=hash_str,
                    )

                    self._db_session.add(entry)

            self._db_session.commit()

    def remove_node_from_index(self, dbx_path: str) -> None:
        """
        Removes any local index entries for the given path and all its children.

        :param dbx_path: Dropbox path.
        """

        with self._database_access():

            dbx_path_lower = dbx_path.lower().rstrip("/")
            match = f"{dbx_path_lower}/%"

            self._db_session.query(IndexEntry).filter(
                IndexEntry.dbx_path_lower == dbx_path_lower
            ).delete(synchronize_session="fetch")
            self._db_session.query(IndexEntry).filter(
                IndexEntry.dbx_path_lower.ilike(match)
            ).delete(synchronize_session="fetch")

            self._db_session.commit()

    def clear_index(self) -> None:
        """Clears the revision index."""
        with self._database_access():

            IndexEntry.metadata.drop_all(self._db_engine)
            Base.metadata.create_all(self._db_engine)
            self._db_session.expunge_all()

    # ==== mignore management ==========================================================

    @property
    def mignore_path(self) -> str:
        """Path to mignore file on local drive (read only)."""
        return self._mignore_path

    @property
    def mignore_rules(self) -> pathspec.PathSpec:
        """List of mignore rules following git wildmatch syntax (read only)."""
        if self._get_ctime(self.mignore_path) != self._mignore_ctime_loaded:
            self._mignore_rules = self._load_mignore_rules_form_file()
        return self._mignore_rules

    def _load_mignore_rules_form_file(self) -> pathspec.PathSpec:
        """Loads rules from mignore file. No rules are loaded if the file does
        not exist or cannot be read."""
        self._mignore_ctime_loaded = self._get_ctime(self.mignore_path)
        try:
            with open(self.mignore_path) as f:
                spec = f.read()
        except OSError as err:
            logger.debug(
                f"Could not load mignore rules from {self.mignore_path}: {err}"
            )
            spec = ""
        return pathspec.PathSpec.from_lines("gitwildmatch", spec.splitlines())

    # ==== helper functions ============================================================

    @property
    def is_case_sensitive(self) -> bool:
        """Returns ``True`` if the local Dropbox folder is located on a partition with a
        case-sensitive file system, ``False`` otherwise."""
        return self._is_case_sensitive

    def ensure_dropbox_folder_present(self) -> None:
        """
        Checks if the Dropbox folder still exists where we expect it to be.

        :raises DropboxDeletedError: When local Dropbox directory does not exist.
        """

        if not osp.isdir(self.dropbox_path):
            title = "Dropbox folder has been moved or deleted"
            msg = (
                "Please move the Dropbox folder back to its original location "
                "or restart Maestral to set up a new folder."
            )
            raise NoDropboxDirError(title, msg)

    def _ensure_cache_dir_present(self) -> None:
        """
        Checks for or creates a directory at :attr:`file_cache_path`.

        :raises CacheDirError: When local cache directory cannot be created.
        """

        retries = 0
        max_retries = 100

        while not osp.isdir(self.file_cache_path):
            try:
                # this will raise FileExistsError if file_cache_path
                # exists but is a file instead of a directory
                os.makedirs(self.file_cache_path, exist_ok=True)
            except FileExistsError:
                # remove the file that's in our way
                self.clean_cache_dir()
            except OSError as err:
                raise CacheDirError(
                    f"Cannot create cache directory: {os.strerror(err.errno)}",
                    "Please check if you have write permissions for "
                    f"{self._file_cache_path}.",
                )

            if retries > max_retries:
                raise CacheDirError(
                    "Cannot create cache directory",
                    "Exceeded maximum number of retries",
                )

            retries += 1

    def clean_cache_dir(self) -> None:
        """Removes all items in the cache directory."""

        with self.sync_lock:
            try:
                delete(self._file_cache_path, raise_error=True)
            except (FileNotFoundError, IsADirectoryError):
                pass
            except OSError as err:
                raise CacheDirError(
                    f"Cannot create cache directory: {os.strerror(err.errno)}",
                    "Please check if you have write permissions for "
                    f"{self._file_cache_path}.",
                )

    def _new_tmp_file(self) -> str:
        """Returns a new temporary file name in our cache directory."""
        self._ensure_cache_dir_present()
        try:
            with NamedTemporaryFile(dir=self.file_cache_path, delete=False) as f:
                try:
                    os.chmod(f.fileno(), 0o666 & ~umask)
                except OSError as exc:
                    # Can occur on file system's that don't support POSIX permissions
                    # such as NTFS mounted without the permissions option.
                    logger.debug("Cannot set permissions: errno %s", exc.errno)
                return f.name
        except OSError as err:
            raise CacheDirError(
                f"Cannot create cache directory: {os.strerror(err.errno)}",
                "Please check if you have write permissions for "
                f"{self._file_cache_path}.",
            )

    def correct_case(self, dbx_path: str) -> str:
        """
        Converts a Dropbox path with correctly cased basename to a fully cased path.
        This is because Dropbox metadata guarantees the correct casing for the basename
        only. In practice, casing of parent directories is often incorrect.
        This is done by retrieving the correct casing of the dirname, either from our
        cache, our database or from Dropbox servers.

        Performance may vary significantly with the number of parent folders:

        1) If the parent directory is already in our cache, performance is O(1).
        2) If the parent directory is already in our sync index, performance is O(1) but
           slower than the first case because it requires a SQLAlchemy query.
        3) If the parent directory is unknown to us, its metadata (including the correct
           casing of directory's basename) is queried from Dropbox. This is used to
           construct a correctly cased path by calling :meth:`correct_case` again. At
           best, performance will be of O(2) if the parent directory is known to us, at
           worst if will be of order O(n) involving queries to Dropbox servers for each
           parent directory.

        When running :meth:`correct_case` on a large tree of paths, it is therefore best
        to do so in hierarchical order.

        :param dbx_path: Dropbox path with correctly cased basename, as provided by
            :attr:`dropbox.files.Metadata.path_display` or
            :attr:`dropbox.files.Metadata.name`.
        :returns: Correctly cased Dropbox path.
        """

        dirname, basename = osp.split(dbx_path)

        dbx_path_lower = dbx_path.lower()
        dirname_lower = dirname.lower()

        if dirname == "/":
            path_cased = dbx_path

        else:

            # check in our conversion cache
            parent_path_cased = self._case_conversion_cache.get(dirname_lower)

            if not parent_path_cased:
                # try to get dirname casing from our index, this is slower
                with self._database_access():
                    parent_entry = self.get_index_entry(dirname_lower)

                if parent_entry:
                    parent_path_cased = parent_entry.dbx_path_cased

                else:
                    # fall back to querying from server
                    md_parent = self.client.get_metadata(dirname_lower)
                    if md_parent:
                        # recurse over parent directories
                        parent_path_cased = self.correct_case(md_parent.path_display)
                    else:
                        # give up
                        parent_path_cased = dirname

            path_cased = f"{parent_path_cased}/{basename}"

        # add our result to the cache
        self._case_conversion_cache.put(dbx_path_lower, path_cased)

        return path_cased

    def to_dbx_path(self, local_path: str) -> str:
        """
        Converts a local path to a path relative to the Dropbox folder. Casing of the
        given ``local_path`` will be preserved.

        :param local_path: Absolute path on local drive.
        :returns: Relative path with respect to Dropbox folder.
        :raises ValueError: When the path lies outside of the local Dropbox folder.
        """

        if is_equal_or_child(local_path, self.dropbox_path):
            dbx_path = osp.sep + removeprefix(local_path, self.dropbox_path).lstrip(
                osp.sep
            )
            return dbx_path.replace(osp.sep, "/")
        else:
            raise ValueError(
                f'Specified path "{local_path}" is outside of Dropbox '
                f'directory "{self.dropbox_path}"'
            )

    def to_local_path_from_cased(self, dbx_path_cased: str) -> str:
        """
        Converts a correctly cased Dropbox path to the corresponding local path. This is
        more efficient than :meth:`to_local_path` which accepts uncased paths.

        :param dbx_path_cased: Path relative to Dropbox folder, correctly cased.
        :returns: Corresponding local path on drive.
        """

        dbx_path_cased = dbx_path_cased.replace("/", osp.sep).lstrip(osp.sep)

        return osp.join(self.dropbox_path, dbx_path_cased)

    def to_local_path(self, dbx_path: str) -> str:
        """
        Converts a Dropbox path to the corresponding local path. Only the basename must
        be correctly cased. This is slower than :meth:`to_local_path_from_cased`.

        :param dbx_path: Path relative to Dropbox folder, must be correctly cased in its
            basename.
        :returns: Corresponding local path on drive.
        """
        dbx_path_cased = self.correct_case(dbx_path)
        dbx_path_cased = dbx_path_cased.replace("/", osp.sep).lstrip(osp.sep)

        return osp.join(self.dropbox_path, dbx_path_cased)

    def has_sync_errors(self) -> bool:
        """Returns ``True`` in case of sync errors, ``False`` otherwise."""
        return len(self.sync_errors) > 0

    def clear_sync_error(
        self, local_path: Optional[str] = None, dbx_path: Optional[str] = None
    ) -> None:
        """
        Clears all sync errors for ``local_path`` or ``dbx_path``.

        :param local_path: Absolute path on local drive.
        :param dbx_path: Path relative to Dropbox folder.
        """

        if local_path and not dbx_path:
            dbx_path = self.to_dbx_path(local_path)
        elif not dbx_path and not local_path:
            return

        dbx_path = cast(str, dbx_path)
        dbx_path_lower = dbx_path.lower()

        if self.has_sync_errors():
            for error in self.sync_errors.copy():
                assert isinstance(error.dbx_path, str)
                if is_equal_or_child(error.dbx_path.lower(), dbx_path_lower):
                    try:
                        self.sync_errors.remove(error)
                    except KeyError:
                        pass

        self.upload_errors.discard(dbx_path_lower)
        self.download_errors.discard(dbx_path_lower)

    def clear_sync_errors(self) -> None:
        """Clears all sync errors."""
        self.sync_errors.clear()
        self.upload_errors.clear()
        self.download_errors.clear()

    @staticmethod
    def is_excluded(path) -> bool:
        """
        Checks if a file is excluded from sync. Certain file names are always excluded
        from syncing, following the Dropbox support article:

        https://help.dropbox.com/installs-integrations/sync-uploads/files-not-syncing

        This includes file system files such as 'desktop.ini' and '.DS_Store' and some
        temporary files as well as caches used by Dropbox or Maestral. `is_excluded`
        accepts both local and Dropbox paths.

        :param path: Path of item. Can be both a local or Dropbox paths.
        :returns: Whether the path is excluded from syncing.
        """
        path = path.lower().replace(osp.sep, "/")

        # is root folder?
        if path in ("/", ""):
            return True

        dirname, basename = osp.split(path)
        # in excluded files?
        if basename in EXCLUDED_FILE_NAMES:
            return True

        # in excluded dirs?
        # TODO: check if this can be optimised
        dirnames = dirname.split("/")
        if any(excluded_dirname in dirnames for excluded_dirname in EXCLUDED_DIR_NAMES):
            return True

        if "~" in basename:  # is temporary file?
            # 1) office temporary files
            if basename.startswith("~$"):
                return True
            if basename.startswith(".~"):
                return True
            # 2) other temporary files
            if basename.startswith("~") and basename.endswith(".tmp"):
                return True

        return False

    def is_excluded_by_user(self, dbx_path: str) -> bool:
        """
        Check if file has been excluded through "selective sync" by the user.

        :param dbx_path: Path relative to Dropbox folder.
        :returns: Whether the path is excluded from download syncing by the user.
        """
        dbx_path = dbx_path.lower()

        return any(is_equal_or_child(dbx_path, path) for path in self.excluded_items)

    def is_mignore(self, event: SyncEvent) -> bool:
        """
        Check if local file change has been excluded by an mignore pattern.

        :param event: SyncEvent for local file event.
        :returns: Whether the path is excluded from upload syncing by the user.
        """
        if len(self.mignore_rules.patterns) == 0:
            return False

        return self._is_mignore_path(
            event.dbx_path, is_dir=event.is_directory
        ) and not self.get_local_rev(event.dbx_path)

    def _is_mignore_path(self, dbx_path: str, is_dir: bool = False) -> bool:

        relative_path = dbx_path.lstrip("/")

        if is_dir:
            relative_path += "/"

        return self.mignore_rules.match_file(relative_path)

    def _slow_down(self) -> None:
        """
        Pauses if CPU usage is too high if called from one of our thread pools.
        """

        if self._max_cpu_percent == 100:
            return

        if "pool" in current_thread().name:
            cpu_usage = cpu_usage_percent()
            while cpu_usage > self._max_cpu_percent:
                cpu_usage = cpu_usage_percent(0.5 + 2 * random.random())

    def cancel_sync(self):
        """
        Cancels all pending sync jobs and returns when idle.
        """

        self._cancel_requested.set()

        # Wait until we can acquire the sync lock => we are idle.
        self.sync_lock.acquire()
        self.sync_lock.release()

        self._cancel_requested.clear()

    def busy(self) -> bool:
        """
        Checks if we are currently syncing.

        :returns: ``True`` if :attr:`sync_lock` cannot be acquired, ``False`` otherwise.
        """

        idle = self.sync_lock.acquire(blocking=False)
        if idle:
            self.sync_lock.release()

        return not idle

    def _handle_sync_error(self, err: SyncError, direction: SyncDirection) -> None:
        """
        Handles a sync error. Fills out any missing path information and adds the error
        to the persistent state for later resync.

        :param err: The sync error to handle.
        :param direction: The sync direction (up or down) for which the error occurred.
        """

        # fill out missing dbx_path or local_path
        if err.dbx_path and not err.local_path:
            err.local_path = self.to_local_path_from_cased(err.dbx_path)
        if err.local_path and not err.dbx_path:
            err.dbx_path = self.to_dbx_path(err.local_path)

        # fill out missing dbx_path_dst or local_path_dst
        if err.dbx_path_dst and not err.local_path_dst:
            err.local_path_dst = self.to_local_path_from_cased(err.dbx_path_dst)
        if err.local_path_dst and not not err.dbx_path:
            err.dbx_path_dst = self.to_dbx_path(err.local_path_dst)

        if err.dbx_path:
            # we have a file / folder associated with the sync error
            # use sanitised path so that the error can be printed to the terminal, etc
            file_name = sanitize_string(osp.basename(err.dbx_path))

            logger.info("Could not sync %s", file_name, exc_info=True)

            def callback():
                if err.local_path:
                    click.launch(err.local_path, locate=True)
                else:
                    url_path = urllib.parse.quote(err.dbx_path)
                    click.launch(f"https://www.dropbox.com/preview{url_path}")

            self.notifier.notify(
                "Sync error",
                f"Could not sync {file_name}",
                level=notify.SYNCISSUE,
                buttons={"Show": callback},
            )
            self.sync_errors.add(err)

            # save download errors to retry later
            if direction == SyncDirection.Down:
                self.download_errors.add(err.dbx_path.lower())
            elif direction == SyncDirection.Up:
                self.upload_errors.add(err.dbx_path.lower())

    @contextmanager
    def _database_access(self, log_errors: bool = False) -> Iterator[None]:
        """
        Synchronises access to the SQLite database. Catches exceptions raised by
        SQLAlchemy and converts them to a MaestralApiError if we know how to handle
        them.

        :param log_errors: If ``True``, any resulting MaestralApiError is not raised but
            only logged.
        """

        title = ""
        msg = ""
        new_exc = None

        try:
            with self._db_lock:
                yield
        except (
            sqlalchemy.exc.DatabaseError,
            sqlalchemy.exc.DataError,
            sqlalchemy.exc.IntegrityError,
        ) as exc:
            title = "Database integrity error"
            msg = "Please rebuild the index to continue syncing."
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)
        except sqlalchemy.exc.OperationalError as exc:
            title = "Database transaction error"
            msg = (
                f'The index file at "{self._db_path}" cannot be read. '
                "Please check that you have sufficient permissions and "
                "rebuild the index if necessary."
            )
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)
        except sqlalchemy.exc.InternalError as exc:
            title = "Database transaction error"
            msg = "Please restart Maestral to continue syncing."
            new_exc = DatabaseError(title, msg).with_traceback(exc.__traceback__)

        if new_exc:
            if log_errors:
                logger.error(title, exc_info=exc_info_tuple(new_exc))
                self.notifier.notify(title, msg, level=notify.ERROR)
            else:
                raise new_exc

    def free_memory(self) -> None:
        """
        Frees memory by resetting our database session and the requests session,
        clearing out case-conversion cache and clearing all expired event ignores and.
        """

        with self._database_access():
            self._db_session.flush()
            self._db_session.close()
            self._db_session = Session()

        self.client.dbx.close()  # resets requests session
        self._case_conversion_cache.clear()
        self.fs_events.expire_ignored_events()
        gc.collect()

    # ==== Upload sync =================================================================

    def upload_local_changes_while_inactive(self) -> None:
        """
        Collects changes while sync has not been running and uploads them to Dropbox.
        Call this method when resuming sync.
        """

        with self.sync_lock:

            logger.info("Indexing local changes...")

            try:
                events, local_cursor = self._get_local_changes_while_inactive()
                logger.debug("Retrieved local changes:\n%s", pprint.pformat(events))
                events = self._clean_local_events(events)
                sync_events = [
                    SyncEvent.from_file_system_event(e, self) for e in events
                ]
            except (FileNotFoundError, NotADirectoryError):
                self.ensure_dropbox_folder_present()
                return

            if len(events) > 0:
                self.apply_local_changes(sync_events)
                logger.debug("Uploaded local changes while inactive")
            else:
                logger.debug("No local changes while inactive")

            if not self._cancel_requested.is_set():
                self.local_cursor = local_cursor

    def _get_local_changes_while_inactive(self) -> Tuple[List[FileSystemEvent], float]:
        """
        Retrieves all local changes since the last sync by performing a full scan of the
        local folder. Changes are detected by comparing the new directory snapshot to
        our index.

        Added items: Are present in the snapshot but not in our index.
        Deleted items: Are present in our index but not in the snapshot.
        Modified items: Are present in both but have a ctime newer than the last sync.

        :returns: Tuple containing local file system events and a cursor / timestamp
            for the changes.
        """

        changes = []
        snapshot_time = time.time()
        snapshot = self._dir_snapshot_with_mignore(self.dropbox_path)
        lowercase_snapshot_paths: Set[str] = set()

        # don't use iterator here but pre-fetch all entries
        # this significantly improves performance but can lead to high memory usage
        entries = self.get_index()

        # get modified or added items
        for path in snapshot.paths:

            # generate lower-case snapshot paths for later
            lowercase_snapshot_paths.add(path.lower())

            if path != self.dropbox_path:

                dbx_path_lower = self.to_dbx_path(path).lower()

                # check if item was created or modified since last sync
                # but before we started the FileEventHandler (~snapshot_time)
                stats = snapshot.stat_info(path)
                last_sync = self.get_last_sync(dbx_path_lower)
                ctime_check = snapshot_time > stats.st_ctime > last_sync

                # always upload untracked items, check ctime of tracked items
                index_entry = self.get_index_entry(dbx_path_lower)
                is_new = index_entry is None
                is_modified = ctime_check and not is_new

                if is_new:
                    if snapshot.isdir(path):
                        event = DirCreatedEvent(path)
                    else:
                        event = FileCreatedEvent(path)
                    changes.append(event)

                elif is_modified:
                    if snapshot.isdir(path) and index_entry.is_directory:  # type: ignore
                        event = DirModifiedEvent(path)
                        changes.append(event)
                    elif not snapshot.isdir(path) and not index_entry.is_directory:  # type: ignore
                        event = FileModifiedEvent(path)
                        changes.append(event)
                    elif snapshot.isdir(path):
                        event0 = FileDeletedEvent(path)
                        event1 = DirCreatedEvent(path)
                        changes += [event0, event1]
                    elif not snapshot.isdir(path):
                        event0 = DirDeletedEvent(path)
                        event1 = FileCreatedEvent(path)
                        changes += [event0, event1]

        # get deleted items
        dbx_root_lower = self.dropbox_path.lower()
        for entry in entries:
            local_path_uncased = f"{dbx_root_lower}{entry.dbx_path_lower}"
            if local_path_uncased not in lowercase_snapshot_paths:
                local_path = self.to_local_path_from_cased(entry.dbx_path_cased)
                if entry.is_directory:
                    event = DirDeletedEvent(local_path)
                else:
                    event = FileDeletedEvent(local_path)
                changes.append(event)

        # free memory
        del entries
        del snapshot
        del lowercase_snapshot_paths
        gc.collect()

        return changes, snapshot_time

    def wait_for_local_changes(self, timeout: float = 40) -> bool:

        logger.debug("Waiting for local changes since cursor: %s", self.local_cursor)

        try:
            event = self.fs_events.local_file_event_queue.get(timeout=timeout)
        except Empty:
            return False

        self.fs_events.local_file_event_queue.queue.append(event)
        return True

    def upload_sync_cycle(self):
        """
        Performs a full upload sync cycle by calling in order:

            1) :meth:`list_local_changes`
            2) :meth:`apply_local_changes`

        Handles updating the local cursor for you. If monitoring for local file events
        was interrupted, call :meth:`upload_local_changes_while_inactive` instead.
        """

        with self.sync_lock:

            changes, cursor = self.list_local_changes()
            self.apply_local_changes(changes)

            if not self._cancel_requested.is_set():
                # Save local cursor if not sync was not aborted by user.
                # Failed uploads will be tracked and retried individually.
                self.local_cursor = cursor

            del changes
            self.free_memory()

    def list_local_changes(self, delay: float = 1) -> Tuple[List[SyncEvent], float]:
        """
        Waits for local file changes. Returns a list of local changes with at most one
        entry per path.

        :param delay: Delay in sec to wait for subsequent changes before returning.
        :returns: (list of sync times events, time_stamp)
        """

        events = []
        local_cursor = time.time()

        # keep collecting events until idle for `delay`
        while True:
            try:
                event = self.fs_events.local_file_event_queue.get(timeout=delay)
                events.append(event)
                local_cursor = time.time()
            except Empty:
                break

        logger.debug("Retrieved local file events:\n%s", pprint.pformat(events))

        events = self._clean_local_events(events)
        sync_events = [SyncEvent.from_file_system_event(e, self) for e in events]

        return sync_events, local_cursor

    def apply_local_changes(self, sync_events: List[SyncEvent]) -> List[SyncEvent]:
        """
        Applies locally detected changes to the remote Dropbox. Changes which should be
        ignored (mignore or always ignored files) are skipped.

        :param sync_events: List of local file system events.
        """

        results = []

        if len(sync_events) > 0:

            sync_events, _ = self._filter_excluded_changes_local(sync_events)

            deleted: List[SyncEvent] = []
            dir_moved: List[SyncEvent] = []
            other: List[SyncEvent] = []  # file created + moved, dir created

            for event in sync_events:
                if event.is_deleted:
                    deleted.append(event)
                elif event.is_directory and event.is_moved:
                    dir_moved.append(event)
                else:
                    other.append(event)

                # housekeeping
                self.syncing.append(event)

            # apply deleted events first, folder moved events second
            # neither event type requires an actual upload
            if deleted:
                logger.info("Uploading deletions...")

            with ThreadPoolExecutor(
                max_workers=self._num_threads,
                thread_name_prefix="maestral-upload-pool",
            ) as executor:
                res = executor.map(self._create_remote_entry, deleted)

                n_items = len(deleted)
                for n, r in enumerate(res):
                    throttled_log(logger, f"Deleting {n + 1}/{n_items}...")
                    results.append(r)

            if dir_moved:
                logger.info("Moving folders...")

            for event in dir_moved:
                logger.info(f"Moving {event.dbx_path_from}...")
                res = self._create_remote_entry(event)
                results.append(res)

            # apply other events in parallel since order does not matter
            with ThreadPoolExecutor(
                max_workers=self._num_threads,
                thread_name_prefix="maestral-upload-pool",
            ) as executor:
                res = executor.map(self._create_remote_entry, other)

                n_items = len(other)
                for n, r in enumerate(res):
                    throttled_log(logger, f"Syncing ↑ {n + 1}/{n_items}")
                    results.append(r)

            self._clean_history()

        return results

    def _filter_excluded_changes_local(
        self, sync_events: List[SyncEvent]
    ) -> Tuple[List[SyncEvent], List[SyncEvent]]:
        """
        Checks for and removes file events referring to items which are excluded from
        syncing. Called by :meth:`apply_local_changes`.

        :param sync_events: List of file events.
        :returns: (``events_filtered``, ``events_excluded``)
        """

        events_filtered = []
        events_excluded = []

        for event in sync_events:

            if self.is_excluded(event.dbx_path):
                events_excluded.append(event)
            elif self.is_mignore(event):
                # moved events with an ignored path are
                # already split into deleted, created pairs
                events_excluded.append(event)
            else:
                events_filtered.append(event)

        logger.debug("Filtered local file events:\n%s", pprint.pformat(events_filtered))

        return events_filtered, events_excluded

    def _clean_local_events(
        self, events: List[FileSystemEvent]
    ) -> List[FileSystemEvent]:
        """
        Takes local file events within and cleans them up so that there is only a single
        event per path. Collapses moved and deleted events of folders with those of
        their children. Called by :meth:`wait_for_local_changes`.

        :param events: Iterable of :class:`watchdog.FileSystemEvent`.
        :returns: List of :class:`watchdog.FileSystemEvent`.
        """

        # COMBINE EVENTS TO ONE EVENT PER PATH

        # Move events are difficult to combine with other event types, we split them
        # into deleted and created events and recombine them later if neither the source
        # of the destination path of has other events associated with it or is excluded
        # from sync.

        histories: Dict[str, List[FileSystemEvent]] = dict()
        moved_events: Dict[str, List[FileSystemEvent]] = dict()
        unique_events: List[FileSystemEvent] = []

        for event in events:
            if isinstance(event, (FileMovedEvent, DirMovedEvent)):
                deleted, created = split_moved_event(event)
                add_to_bin(histories, deleted.src_path, deleted)
                add_to_bin(histories, created.src_path, created)
            else:
                add_to_bin(histories, event.src_path, event)

        # for every path, keep only a single event which represents all changes

        for path, events in histories.items():
            if len(events) == 1:
                event = events[0]
                unique_events.append(event)

                if hasattr(event, "move_id"):
                    # add to list "moved_events" to recombine line
                    add_to_bin(moved_events, event.move_id, event)

            else:

                n_created = len(
                    [e for e in events if e.event_type == EVENT_TYPE_CREATED]
                )
                n_deleted = len(
                    [e for e in events if e.event_type == EVENT_TYPE_DELETED]
                )

                if n_created > n_deleted:  # item was created
                    if events[-1].is_directory:
                        unique_events.append(DirCreatedEvent(path))
                    else:
                        unique_events.append(FileCreatedEvent(path))
                elif n_created < n_deleted:  # item was deleted
                    if events[0].is_directory:
                        unique_events.append(DirDeletedEvent(path))
                    else:
                        unique_events.append(FileDeletedEvent(path))
                else:

                    first_created_index = next(
                        iter(
                            i
                            for i, e in enumerate(events)
                            if e.event_type == EVENT_TYPE_CREATED
                        ),
                        -1,
                    )
                    first_deleted_index = next(
                        iter(
                            i
                            for i, e in enumerate(events)
                            if e.event_type == EVENT_TYPE_DELETED
                        ),
                        -1,
                    )

                    if n_created == 0 or first_deleted_index < first_created_index:
                        # item was modified
                        if events[0].is_directory and events[-1].is_directory:
                            unique_events.append(DirModifiedEvent(path))
                        elif not events[0].is_directory and not events[-1].is_directory:
                            unique_events.append(FileModifiedEvent(path))
                        elif events[0].is_directory:
                            unique_events.append(DirDeletedEvent(path))
                            unique_events.append(FileCreatedEvent(path))
                        elif events[-1].is_directory:
                            unique_events.append(FileDeletedEvent(path))
                            unique_events.append(DirCreatedEvent(path))
                    else:
                        # item was only temporary
                        pass

        # event order does not matter anymore from this point because we have already
        # consolidated events for every path

        cleaned_events = set(unique_events)

        # recombine moved events

        for split_events in moved_events.values():
            if len(split_events) == 2:
                src_path = next(
                    e.src_path
                    for e in split_events
                    if e.event_type == EVENT_TYPE_DELETED
                )
                dest_path = next(
                    e.src_path
                    for e in split_events
                    if e.event_type == EVENT_TYPE_CREATED
                )
                if split_events[0].is_directory:
                    new_event = DirMovedEvent(src_path, dest_path)
                else:
                    new_event = FileMovedEvent(src_path, dest_path)

                # Only recombine events if neither has an excluded path: We want to
                # treat renaming from / to an excluded path as a creation / deletion,
                # respectively.
                if not self._should_split_excluded(new_event):
                    cleaned_events.difference_update(split_events)
                    cleaned_events.add(new_event)

        # COMBINE MOVED AND DELETED EVENTS OF FOLDERS AND THEIR CHILDREN INTO ONE EVENT

        # Avoid nested iterations over all events here, they are on the order of O(n^2)
        # which becomes costly when the user moves or deletes folder with a large number
        # of children. Benchmark: aim to stay below 1 sec for 20,000 nested events on
        # representative laptops.

        # 1) combine moved events of folders and their children into one event
        dir_moved_paths = set(
            (e.src_path, e.dest_path)
            for e in cleaned_events
            if isinstance(e, DirMovedEvent)
        )

        if len(dir_moved_paths) > 0:
            child_moved_events: Dict[Tuple[str, str], List[FileSystemEvent]] = dict()
            for paths in dir_moved_paths:
                child_moved_events[paths] = []

            for event in cleaned_events:
                if event.event_type == EVENT_TYPE_MOVED:
                    try:
                        dirnames = (
                            osp.dirname(event.src_path),
                            osp.dirname(event.dest_path),
                        )
                        child_moved_events[dirnames].append(event)
                    except KeyError:
                        pass

            for split_events in child_moved_events.values():
                cleaned_events.difference_update(split_events)

        # 2) combine deleted events of folders and their children to one event
        dir_deleted_paths = set(
            e.src_path for e in cleaned_events if isinstance(e, DirDeletedEvent)
        )

        if len(dir_deleted_paths) > 0:
            child_deleted_events: Dict[str, List[FileSystemEvent]] = dict()
            for path in dir_deleted_paths:
                child_deleted_events[path] = []

            for event in cleaned_events:
                if event.event_type == EVENT_TYPE_DELETED:
                    try:
                        dirname = osp.dirname(event.src_path)
                        child_deleted_events[dirname].append(event)
                    except KeyError:
                        pass

            for split_events in child_deleted_events.values():
                cleaned_events.difference_update(split_events)

        logger.debug(
            "Cleaned up local file events:\n%s", pprint.pformat(cleaned_events)
        )

        del events
        del unique_events

        return list(cleaned_events)

    def _should_split_excluded(self, event: Union[FileMovedEvent, DirMovedEvent]):

        if event.event_type != EVENT_TYPE_MOVED:
            raise ValueError("Can only split moved events")

        dbx_src_path = self.to_dbx_path(event.src_path)
        dbx_dest_path = self.to_dbx_path(event.dest_path)

        if (
            self.is_excluded(event.src_path)
            or self.is_excluded(event.dest_path)
            or self.is_excluded_by_user(dbx_src_path)
            or self.is_excluded_by_user(dbx_dest_path)
        ):
            return True

        elif len(self.mignore_rules.patterns) == 0:
            return False
        else:
            dbx_src_path = self.to_dbx_path(event.src_path)
            dbx_dest_path = self.to_dbx_path(event.dest_path)

            return self._is_mignore_path(
                dbx_src_path, event.is_directory
            ) or self._is_mignore_path(dbx_dest_path, event.is_directory)

    def _handle_case_conflict(self, event: SyncEvent) -> bool:
        """
        Checks for other items in the same directory with same name but a different
        case. Renames items if necessary. Only needed for case sensitive file systems.

        :param event: SyncEvent for local created or moved event.
        :returns: Whether a case conflict was detected and handled.
        """

        if not self.is_case_sensitive:
            return False

        if not (event.is_added or event.is_moved):
            return False

        dirname, basename = osp.split(event.local_path)

        # check number of paths with the same case
        if len(cased_path_candidates(basename, root=dirname)) > 1:

            local_path_cc = generate_cc_name(
                event.local_path,
                suffix="case conflict",
                is_fs_case_sensitive=self.is_case_sensitive,
            )

            event_cls = DirMovedEvent if osp.isdir(event.local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(event.local_path, local_path_cc)):
                with convert_api_errors(local_path=local_path_cc):
                    move(event.local_path, local_path_cc, raise_error=True)

                self.rescan(local_path_cc)

            logger.info(
                'Case conflict: renamed "%s" to "%s"', event.local_path, local_path_cc
            )

            return True
        else:
            return False

    def _handle_selective_sync_conflict(self, event: SyncEvent) -> bool:
        """
        Checks for items in the local directory with same path as an item which is
        excluded by selective sync. Renames items if necessary.

        :param event: SyncEvent for local created or moved event.
        :returns: Whether a selective sync conflict was detected and handled.
        """

        if not (event.is_added or event.is_moved):
            return False

        if self.is_excluded_by_user(event.dbx_path):
            local_path_cc = generate_cc_name(
                event.local_path,
                suffix="selective sync conflict",
                is_fs_case_sensitive=self.is_case_sensitive,
            )

            event_cls = DirMovedEvent if osp.isdir(event.local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(event.local_path, local_path_cc)):
                with convert_api_errors(local_path=local_path_cc):
                    move(event.local_path, local_path_cc, raise_error=True)

                self.rescan(local_path_cc)

            logger.info(
                'Selective sync conflict: renamed "%s" to "%s"',
                event.local_path,
                local_path_cc,
            )
            return True
        else:
            return False

    def _create_remote_entry(self, event: SyncEvent) -> SyncEvent:
        """
        Applies a local file system event to the remote Dropbox and clears any existing
        sync errors belonging to that path. Any :class:`errors.SyncError` will be caught
        and logged as appropriate.

        :param event: SyncEvent for local file event.
        :returns: SyncEvent with updated status.
        """

        if self._cancel_requested.is_set():
            event.status = SyncStatus.Aborted
            self.syncing.remove(event)
            return event

        self._slow_down()

        self.clear_sync_error(local_path=event.local_path)
        self.clear_sync_error(local_path=event.local_path_from)
        event.status = SyncStatus.Syncing

        try:

            if event.is_added:
                res = self._on_local_created(event)
            elif event.is_moved:
                res = self._on_local_moved(event)
            elif event.is_changed:
                res = self._on_local_modified(event)
            elif event.is_deleted:
                res = self._on_local_deleted(event)
            else:
                res = None

            if res is not None:
                event.status = SyncStatus.Done
            else:
                event.status = SyncStatus.Skipped

        except SyncError as err:
            self._handle_sync_error(err, direction=SyncDirection.Up)
            event.status = SyncStatus.Failed
        finally:
            self.syncing.remove(event)

        # add to history database
        if event.status == SyncStatus.Done:
            with self._database_access():
                self._db_session.add(event)

        return event

    @staticmethod
    def _wait_for_creation(local_path: str) -> None:
        """
        Wait for a file at a path to be created or modified.

        :param local_path: Absolute path to file on drive.
        """
        try:
            while True:
                size1 = osp.getsize(local_path)
                time.sleep(0.2)
                size2 = osp.getsize(local_path)
                if size1 == size2:
                    return
        except OSError:
            return

    def _on_local_moved(self, event: SyncEvent) -> Optional[Metadata]:
        """
        Call when a local item is moved.

        Keep in mind that we may be moving a whole tree of items. But its better deal
        with the complexity than to delete and re-uploading everything. Thankfully, in
        case of directories, we always process the top-level first. Trying to move the
        children will then be delegated to `on_create` (because the old item no longer
        lives on Dropbox) and that won't upload anything because file contents have
        remained the same.

        :param event: SyncEvent for local moved event.
        :returns: Metadata for created remote item at destination.
        :raises MaestralApiError: For any issues when syncing the item.
        """

        # fail fast on badly decoded paths
        validate_encoding(event.local_path)

        if self._handle_selective_sync_conflict(event):
            return None
        if self._handle_case_conflict(event):
            return None

        dbx_path_from = cast(str, event.dbx_path_from)
        md_from_old = self.client.get_metadata(dbx_path_from)

        # If not on Dropbox, e.g., because its old name was invalid,
        # create it instead of moving it.
        if not md_from_old:
            if event.is_directory:
                new_event = DirCreatedEvent(event.local_path)
            else:
                new_event = FileCreatedEvent(event.local_path)

            new_sync_event = SyncEvent.from_file_system_event(new_event, self)

            return self._on_local_created(new_sync_event)

        md_to_new = self.client.move(dbx_path_from, event.dbx_path, autorename=True)

        self.remove_node_from_index(dbx_path_from)

        if md_to_new.path_lower != event.dbx_path.lower():
            # TODO: test this
            # conflicting copy created during upload, mirror remote changes locally
            local_path_cc = self.to_local_path(md_to_new.path_display)
            event_cls = DirMovedEvent if osp.isdir(event.local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(event.local_path, local_path_cc)):
                with convert_api_errors(
                    local_path=local_path_cc, dbx_path=md_to_new.path_display
                ):
                    move(event.local_path, local_path_cc, raise_error=True)

            # Delete entry of old path but don't update entry for new path here. This
            # will force conflict resolution on download in case of intermittent
            # changes.
            self.remove_node_from_index(event.dbx_path)
            logger.info(
                'Upload conflict: renamed "%s" to "%s"',
                event.dbx_path,
                md_to_new.path_display,
            )

        else:
            self._update_index_recursive(md_to_new)
            logger.debug('Moved "%s" to "%s" on Dropbox', dbx_path_from, event.dbx_path)

        return md_to_new

    def _update_index_recursive(self, md):

        self.update_index_from_dbx_metadata(md)

        if isinstance(md, FolderMetadata):
            result = self.client.list_folder(md.path_lower, recursive=True)
            for md in result.entries:
                self.update_index_from_dbx_metadata(md)

    def _on_local_created(self, event: SyncEvent) -> Optional[Metadata]:
        """
        Call when a local item is created.

        :param event: SyncEvent corresponding to local created event.
        :returns: Metadata for created item or None if no remote item is created.
        :raises MaestralApiError: For any issues when syncing the item.
        """

        # fail fast on badly decoded paths
        validate_encoding(event.local_path)

        if self._handle_selective_sync_conflict(event):
            return None
        if self._handle_case_conflict(event):
            return None

        self._wait_for_creation(event.local_path)

        if event.is_directory:
            try:
                md_new = self.client.make_dir(event.dbx_path, autorename=False)
            except FolderConflictError:
                logger.debug(
                    'No conflict for "%s": the folder already exists', event.local_path
                )
                try:
                    md = self.client.get_metadata(event.dbx_path)
                    if isinstance(md, FolderMetadata):
                        self.update_index_from_dbx_metadata(md)
                except NotFoundError:
                    pass

                return None
            except FileConflictError:
                md_new = self.client.make_dir(event.dbx_path, autorename=True)

        else:
            # check if file already exists with identical content
            md_old = self.client.get_metadata(event.dbx_path)
            if isinstance(md_old, FileMetadata):
                if event.content_hash == md_old.content_hash:
                    # file hashes are identical, do not upload
                    self.update_index_from_dbx_metadata(md_old)
                    return None

            local_entry = self.get_index_entry(event.dbx_path)

            if not local_entry:
                # file is new to us, let Dropbox rename it if something is in the way
                mode = dropbox.files.WriteMode.add
            elif local_entry.is_directory:
                # try to overwrite the destination, this will fail...
                mode = dropbox.files.WriteMode.overwrite
            else:
                # file has been modified, update remote if matching rev,
                # create conflict otherwise
                logger.debug(
                    '"%s" appears to have been created but we are '
                    "already tracking it",
                    event.dbx_path,
                )
                mode = dropbox.files.WriteMode.update(local_entry.rev)
            try:
                md_new = self.client.upload(
                    event.local_path,
                    event.dbx_path,
                    autorename=True,
                    mode=mode,
                    sync_event=event,
                )
            except NotFoundError:
                logger.debug(
                    'Could not upload "%s": the item does not exist', event.local_path
                )
                return None

        if md_new.path_lower != event.dbx_path.lower():
            # conflicting copy created during upload, mirror remote changes locally
            local_path_cc = self.to_local_path(md_new.path_display)
            event_cls = DirMovedEvent if osp.isdir(event.local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(event.local_path, local_path_cc)):
                with convert_api_errors(
                    local_path=local_path_cc, dbx_path=md_new.path_display
                ):
                    move(event.local_path, local_path_cc, raise_error=True)

            # Delete entry of old path but don't update entry for new path here. This
            # will force conflict resolution on download in case of intermittent
            # changes.
            self.remove_node_from_index(event.dbx_path)
            logger.debug(
                'Upload conflict: renamed "%s" to "%s"',
                event.dbx_path,
                md_new.path_lower,
            )
        else:
            # everything went well, update index
            self.update_index_from_dbx_metadata(md_new)
            logger.debug('Created "%s" on Dropbox', event.dbx_path)

        return md_new

    def _on_local_modified(self, event: SyncEvent) -> Optional[Metadata]:
        """
        Call when local item is modified.

        :param event: SyncEvent for local modified event.
        :returns: Metadata corresponding to modified remote item or None if no remote
            item is modified.
        :raises MaestralApiError: For any issues when syncing the item.
        """

        if event.is_directory:  # ignore directory modified events
            return None

        self._wait_for_creation(event.local_path)

        # check if item already exists with identical content
        md_old = self.client.get_metadata(event.dbx_path)
        if isinstance(md_old, FileMetadata):
            if event.content_hash == md_old.content_hash:
                # file hashes are identical, do not upload
                self.update_index_from_dbx_metadata(md_old)
                logger.debug(
                    'Modification of "%s" detected but file content is '
                    "the same as on Dropbox",
                    event.dbx_path,
                )
                return None

        local_entry = self.get_index_entry(event.dbx_path)

        if not local_entry:
            logger.debug(
                '"%s" appears to have been modified but cannot ' "find old revision",
                event.dbx_path,
            )
            mode = dropbox.files.WriteMode.add
        elif local_entry.is_directory:
            mode = dropbox.files.WriteMode.overwrite
        else:
            mode = dropbox.files.WriteMode.update(local_entry.rev)

        try:
            md_new = self.client.upload(
                event.local_path,
                event.dbx_path,
                autorename=True,
                mode=mode,
                sync_event=event,
            )
        except NotFoundError:
            logger.debug(
                'Could not upload "%s": the item does not exist', event.dbx_path
            )
            return None

        if md_new.path_lower != event.dbx_path.lower():
            # conflicting copy created during upload, mirror remote changes locally
            local_path_cc = self.to_local_path(md_new.path_display)
            with self.fs_events.ignore(FileMovedEvent(event.local_path, local_path_cc)):
                try:
                    os.rename(event.local_path, local_path_cc)
                except OSError:
                    with self.fs_events.ignore(FileDeletedEvent(event.local_path)):
                        delete(event.local_path)

            # Delete revs of old path but don't set revs for new path here. This will
            # force conflict resolution on download in case of intermittent changes.
            self.remove_node_from_index(event.dbx_path)
            logger.debug(
                'Upload conflict: renamed "%s" to "%s"',
                event.dbx_path,
                md_new.path_lower,
            )
        else:
            # everything went well, save new revs
            self.update_index_from_dbx_metadata(md_new)
            logger.debug('Uploaded modified "%s" to Dropbox', md_new.path_lower)

        return md_new

    def _on_local_deleted(self, event: SyncEvent) -> Optional[Metadata]:
        """
        Call when local item is deleted. We try not to delete remote items which have
        been modified since the last sync.

        :param event: SyncEvent for local deletion.
        :returns: Metadata for deleted item or None if no remote item is deleted.
        :raises MaestralApiError: For any issues when syncing the item.
        """

        if self.is_excluded_by_user(event.dbx_path):
            logger.debug(
                'Not deleting "%s": is excluded by selective sync', event.dbx_path
            )
            return None

        local_rev = self.get_local_rev(event.dbx_path)

        md = self.client.get_metadata(event.dbx_path, include_deleted=True)

        if event.is_directory and isinstance(md, FileMetadata):
            logger.debug(
                'Expected folder at "%s" but found a file instead, checking '
                "which one is newer",
                md.path_display,
            )
            # don't delete a remote file if it was modified since last sync
            if md.server_modified.timestamp() >= self.get_last_sync(event.dbx_path):
                logger.debug(
                    'Skipping deletion: remote item "%s" has been modified '
                    "since last sync",
                    md.path_display,
                )
                # mark local folder as untracked
                self.remove_node_from_index(event.dbx_path)
                return None

        if event.is_file and isinstance(md, FolderMetadata):
            # don't delete a remote folder if we were expecting a file
            # TODO: Delete the folder if its children did not change since last sync.
            #   Is there a way of achieving this without listing the folder or listing
            #   all changes and checking when they occurred?
            logger.debug(
                'Skipping deletion: expected file at "%s" but found a '
                "folder instead",
                md.path_display,
            )
            # mark local file as untracked
            self.remove_node_from_index(event.dbx_path)
            return None

        try:
            # will only perform delete if Dropbox remote rev matches `local_rev`
            md_deleted = self.client.remove(
                event.dbx_path, parent_rev=local_rev if event.is_file else None
            )
        except NotFoundError:
            logger.debug(
                'Could not delete "%s": the item no longer exists on Dropbox',
                event.dbx_path,
            )
            md_deleted = None
        except PathError:
            logger.debug(
                'Could not delete "%s": the item has been changed ' "since last sync",
                event.dbx_path,
            )
            md_deleted = None

        # remove revision metadata
        self.remove_node_from_index(event.dbx_path)

        return md_deleted

    # ==== Download sync ===============================================================

    def get_remote_folder(self, dbx_path: str) -> bool:
        """
        Gets all files/folders from a Dropbox folder and writes them to the local folder
        :attr:`dropbox_path`.

        :param dbx_path: Path relative to Dropbox folder.
        :returns: Whether download was successful.
        """

        with self.sync_lock:

            logger.info(f"Syncing ↓ {dbx_path}")

            try:

                # iterate over index and download results
                list_iter = self.client.list_folder_iterator(dbx_path, recursive=True)

                for res in list_iter:
                    res.entries.sort(key=lambda x: x.path_lower.count("/"))

                    # convert metadata to sync_events
                    sync_events = [
                        SyncEvent.from_dbx_metadata(md, self) for md in res.entries
                    ]
                    download_res = self.apply_remote_changes(sync_events)

                    success = all(
                        e.status in (SyncStatus.Done, SyncStatus.Skipped)
                        for e in download_res
                    )

            except SyncError as e:
                self._handle_sync_error(e, direction=SyncDirection.Down)
                return False

            return success

    def get_remote_item(self, dbx_path: str) -> bool:
        """
        Downloads a remote file or folder and updates its local rev. If the remote item
        does not exist, any corresponding local items will be deleted. If ``dbx_path``
        refers to a folder, the download will be handled by :meth:`get_remote_folder`.
        If it refers to a single file, the download will be performed by
        :meth:`_create_local_entry`.

        This method can be used to fetch individual items outside of the regular sync
        cycle, for instance when including a previously excluded file or folder.

        :param dbx_path: Path relative to Dropbox folder.
        :returns: Whether download was successful.
        """

        with self.sync_lock:

            md = self.client.get_metadata(dbx_path, include_deleted=True)

            if md is None:
                # create a fake deleted event
                index_entry = self.get_index_entry(dbx_path)
                cased_path = index_entry.dbx_path_cased if index_entry else dbx_path

                md = DeletedMetadata(
                    name=osp.basename(dbx_path),
                    path_lower=dbx_path.lower(),
                    path_display=cased_path,
                )

            event = SyncEvent.from_dbx_metadata(md, self)

            if event.is_directory:
                success = self.get_remote_folder(dbx_path)
            else:
                self.syncing.append(event)
                e = self._create_local_entry(event)
                success = e.status in (SyncStatus.Done, SyncStatus.Skipped)

            return success

    def wait_for_remote_changes(self, last_cursor: str, timeout: int = 40) -> bool:
        """
        Blocks until changes to the remote Dropbox are available.

        :param last_cursor: Cursor form last sync.
        :param timeout: Timeout in seconds before returning even if there are no
            changes. Dropbox adds random jitter of up to 90 sec to this value.
        """
        logger.debug("Waiting for remote changes since cursor:\n%s", last_cursor)
        has_changes = self.client.wait_for_remote_changes(last_cursor, timeout=timeout)

        # For for 2 sec. This delay is typically only necessary folders are shared /
        # un-shared with other Dropbox accounts.
        time.sleep(2)

        logger.debug("Detected remote changes: %s", has_changes)
        return has_changes

    def download_sync_cycle(self) -> None:
        """
        Performs a full download sync cycle by calling in order:

            1) :meth:`list_remote_changes_iterator`
            2) :meth:`apply_remote_changes`

        Handles updating the remote cursor and resuming interrupted syncs for you.
        Calling this method will perform a full indexing if this is the first download.
        """

        with self.sync_lock:

            is_indexing = self.remote_cursor == ""

            changes_iter = self.list_remote_changes_iterator(self.remote_cursor)

            # Download changes in chunks to reduce memory usage.
            for changes, cursor in changes_iter:
                downloaded = self.apply_remote_changes(changes)

                if not is_indexing:
                    # Don't send desktop notifications during indexing.
                    self.notify_user(downloaded)

                if self._cancel_requested.is_set():
                    break
                else:
                    # Save (incremental) remote cursor.
                    self.remote_cursor = cursor

                del changes
                del downloaded

            self.free_memory()

    def list_remote_changes_iterator(
        self, last_cursor: str
    ) -> Iterator[Tuple[List[SyncEvent], str]]:
        """
        Get remote changes since the last download sync, as specified by
        ``last_cursor``. If the ``last_cursor`` is from paginating through a previous
        set of changes, continue where we left off. If ``last_cursor`` is an emtpy
        string, tart a full indexing of the Dropbox folder.

        :param last_cursor: Cursor from last download sync.
        :returns: Iterator yielding tuples with remote changes and corresponding cursor.
        """

        if last_cursor == "":
            # We are starting from the beginning, do a full indexing.
            logger.info("Fetching remote Dropbox")
            changes_iter = self.client.list_folder_iterator("/", recursive=True)
        else:
            # Pick up where we left off. This may be an interrupted indexing /
            # pagination through changes or a completely new set of changes.
            logger.info("Fetching remote changes...")
            changes_iter = self.client.list_remote_changes_iterator(last_cursor)

        for changes in changes_iter:

            logger.debug("Listed remote changes:\n%s", entries_repr(changes.entries))

            clean_changes = self._clean_remote_changes(changes)
            logger.debug(
                "Cleaned remote changes:\n%s", entries_repr(clean_changes.entries)
            )

            clean_changes.entries.sort(key=lambda x: x.path_lower.count("/"))
            sync_events = [
                SyncEvent.from_dbx_metadata(md, self) for md in clean_changes.entries
            ]

            logger.debug("Converted remote changes to SyncEvents")

            yield sync_events, changes.cursor

    def apply_remote_changes(self, sync_events: List[SyncEvent]) -> List[SyncEvent]:
        """
        Applies remote changes to local folder. Call this on the result of
        :meth:`list_remote_changes`. The saved cursor is updated after a set of changes
        has been successfully applied. Entries in the local index are created after
        successful completion.

        :param sync_events: List of remote changes.
        :returns: List of changes that were made to local files and bool indicating if
            all download syncs were successful.
        """

        # filter out excluded changes
        changes_included, changes_excluded = self._filter_excluded_changes_remote(
            sync_events
        )

        # remove deleted item and its children from the excluded list
        for event in changes_excluded:
            if event.is_deleted:
                new_excluded = [
                    path
                    for path in self.excluded_items
                    if not is_equal_or_child(path, event.dbx_path.lower())
                ]

                self.excluded_items = new_excluded

        # sort changes into folders, files and deleted
        # sort according to path hierarchy:
        # do not create sub-folder / file before parent exists
        # delete parents before deleting children to save some work
        files: List[SyncEvent] = list()
        folders: Dict[int, List[SyncEvent]] = dict()
        deleted: Dict[int, List[SyncEvent]] = dict()

        for event in changes_included:

            level = event.dbx_path.count("/")

            if event.is_deleted:
                add_to_bin(deleted, level, event)
            elif event.is_file:
                files.append(event)
            elif event.is_directory:
                add_to_bin(folders, level, event)

            # housekeeping
            self.syncing.append(event)

        results = []  # local list of all changes

        # apply deleted items
        if deleted:
            logger.info("Applying deletions...")
        for level in sorted(deleted):
            items = deleted[level]
            with ThreadPoolExecutor(
                max_workers=self._num_threads,
                thread_name_prefix="maestral-download-pool",
            ) as executor:
                res = executor.map(self._create_local_entry, items)

                n_items = len(items)
                for n, r in enumerate(res):
                    throttled_log(logger, f"Deleting {n + 1}/{n_items}...")
                    results.append(r)

        # create local folders, start with top-level and work your way down
        if folders:
            logger.info("Creating folders...")
        for level in sorted(folders):
            items = folders[level]
            with ThreadPoolExecutor(
                max_workers=self._num_threads,
                thread_name_prefix="maestral-download-pool",
            ) as executor:
                res = executor.map(self._create_local_entry, items)

                n_items = len(items)
                for n, r in enumerate(res):
                    throttled_log(logger, f"Creating folder {n + 1}/{n_items}...")
                    results.append(r)

        # apply created files
        with ThreadPoolExecutor(
            max_workers=self._num_threads, thread_name_prefix="maestral-download-pool"
        ) as executor:
            res = executor.map(self._create_local_entry, files)

            n_items = len(files)
            for n, r in enumerate(res):
                throttled_log(logger, f"Syncing ↓ {n + 1}/{n_items}")
                results.append(r)

        self._clean_history()

        return results

    def notify_user(self, sync_events: List[SyncEvent]) -> None:
        """
        Shows a desktop notification for the given file changes.

        :param sync_events: List of SyncEvents from download sync.
        """

        buttons: Dict[str, Callable]

        changes = [e for e in sync_events if e.status != SyncStatus.Skipped]

        # get number of remote changes
        n_changed = len(changes)

        if n_changed == 0:
            return

        # find out who changed the item(s), show the user name if its only a single user
        user_name: Optional[str]
        dbid_list = set(e.change_dbid for e in changes if e.change_dbid is not None)
        if len(dbid_list) == 1:
            # all files have been modified by the same user
            dbid = dbid_list.pop()
            if dbid == self.client.account_id:
                user_name = "You"
            else:
                try:
                    account_info = self.client.get_account_info(dbid)
                except InvalidDbidError:
                    user_name = None
                else:
                    user_name = account_info.name.display_name
        else:
            user_name = None

        if n_changed == 1:
            # display user name, file name, and type of change
            event = changes[0]
            file_name = osp.basename(event.dbx_path)
            change_type = event.change_type.value

            def callback():
                click.launch(event.local_path, locate=True)

            buttons = {"Show": callback}

        else:

            if all(e.change_type == sync_events[0].change_type for e in sync_events):
                change_type = sync_events[0].change_type.value
            else:
                change_type = "changed"

            if all(e.is_file for e in sync_events):
                file_name = f"{n_changed} files"
            elif all(e.is_directory for e in sync_events):
                file_name = f"{n_changed} folders"
            else:
                file_name = f"{n_changed} items"

            buttons = {}

        if change_type == ChangeType.Removed.value:

            def callback():
                # show dropbox website with deleted files
                click.launch("https://www.dropbox.com/deleted_files")

            buttons = {"Show": callback}

        if user_name:
            msg = f"{user_name} {change_type} {file_name}"
        else:
            msg = f"{file_name} {change_type}"

        self.notifier.notify("Items synced", msg, buttons=buttons)

    def _filter_excluded_changes_remote(
        self, changes: List[SyncEvent]
    ) -> Tuple[List[SyncEvent], List[SyncEvent]]:
        """
        Removes all excluded items from the given list of changes.

        :param changes: List of SyncEvents.
        :returns: Tuple with items to keep and items to discard.
        """
        items_to_keep = []
        items_to_discard = []

        for item in changes:
            if self.is_excluded_by_user(item.dbx_path) or self.is_excluded(
                item.dbx_path
            ):
                items_to_discard.append(item)
            else:
                items_to_keep.append(item)

        return items_to_keep, items_to_discard

    def _check_download_conflict(self, event: SyncEvent) -> Conflict:
        """
        Check if a local item is conflicting with remote change. The equivalent check
        when uploading and a change will be carried out by Dropbox itself.

        Checks are carried out against our index, reflecting the latest sync state.
        We compare the following values:

        1) Local vs remote rev: 'folder' for folders, actual revision for files and None
           for deleted or not present items.
        2) Local vs remote content hash: 'folder' for folders, actual hash for files and
           None for deletions.
        3) Local ctime vs last sync time: This is calculated recursively for folders.

        :param event: Download SyncEvent.
        :returns: Conflict check result.
        :raises MaestralApiError: For any issues when syncing the item.
        """

        local_rev = self.get_local_rev(event.dbx_path)

        if event.rev == local_rev:
            # Local change has the same rev. The local item (or deletion) must be newer
            # and not yet synced or identical to the remote state. Don't overwrite.
            logger.debug(
                'Equal revs for "%s": local item is the same or newer '
                "than on Dropbox",
                event.dbx_path,
            )
            return Conflict.LocalNewerOrIdentical

        elif event.content_hash == self.get_local_hash(event.local_path):
            # Content hashes are equal, therefore items are identical. Folders will
            # have a content hash of 'folder'.
            logger.debug('Equal content hashes for "%s": no conflict', event.dbx_path)
            return Conflict.Identical
        elif any(
            is_equal_or_child(p, event.dbx_path.lower()) for p in self.upload_errors
        ):
            # Local version could not be uploaded due to a sync error. Do not over-
            # write unsynced changes but declare a conflict.
            logger.debug('Unresolved upload error for "%s": conflict', event.dbx_path)
            return Conflict.Conflict
        elif not self._ctime_newer_than_last_sync(event.local_path):
            # Last change time of local item (recursive for folders) is older than
            # the last time the item was synced. Remote must be newer.
            logger.debug(
                'Local item "%s" has no unsynced changes: remote item is newer',
                event.dbx_path,
            )
            return Conflict.RemoteNewer
        elif event.is_deleted:
            # Remote item was deleted but local item has been modified since then.
            logger.debug(
                'Local item "%s" has unsynced changes and remote was '
                "deleted: local item is newer",
                event.dbx_path,
            )
            return Conflict.LocalNewerOrIdentical
        else:
            # Both remote and local items have unsynced changes: conflict.
            logger.debug(
                'Local item "%s" has unsynced local changes: conflict',
                event.dbx_path,
            )
            return Conflict.Conflict

    def _ctime_newer_than_last_sync(self, local_path: str) -> bool:
        """
        Checks if a local item has any unsynced changes. This is by comparing its ctime
        to the ``last_sync`` time saved in our index. In case of folders, we recursively
        check the ctime of children.

        :param local_path: Local path of item to check.
        :returns: Whether the local item has unsynced changes.
        """

        if self.is_excluded(local_path):
            # excluded names such as .DS_Store etc never count as unsynced changes
            return False

        dbx_path = self.to_dbx_path(local_path)
        index_entry = self.get_index_entry(dbx_path)

        try:
            stat = os.stat(local_path)
        except (FileNotFoundError, NotADirectoryError):
            # don't check ctime for deleted items (os won't give stat info)
            # but confirm absence from index
            return index_entry is not None

        if S_ISDIR(stat.st_mode):

            # don't check ctime for folders but to index entry type
            if index_entry is None or index_entry.is_file:
                return True

            # recurse over children
            with os.scandir(local_path) as it:
                for entry in it:
                    if entry.is_dir():
                        if self._ctime_newer_than_last_sync(entry.path):
                            return True
                    elif not self.is_excluded(entry.name):
                        child_dbx_path = self.to_dbx_path(entry.path)
                        if entry.stat().st_ctime > self.get_last_sync(child_dbx_path):
                            return True

            return False

        else:
            # check our ctime against index
            return os.stat(local_path).st_ctime > self.get_last_sync(dbx_path)

    def _get_ctime(self, local_path: str) -> float:
        """
        Returns the ctime of a local item or -1.0 if there is nothing at the path. If
        the item is a directory, return the largest ctime of it and its children. Items
        which are excluded from syncing (eg., .DS_Store files) are ignored.

        :param local_path: Absolute path on local drive.
        :returns: Ctime or -1.0.
        """
        try:
            stat = os.stat(local_path)
            if S_ISDIR(stat.st_mode):
                ctime = stat.st_ctime
                with os.scandir(local_path) as it:
                    for entry in it:
                        if entry.is_dir():
                            child_ctime = self._get_ctime(entry.path)
                        elif not self.is_excluded(entry.name):
                            child_ctime = entry.stat().st_ctime
                        else:
                            child_ctime = -1.0

                        ctime = max(ctime, child_ctime)

                return ctime
            else:
                return os.stat(local_path).st_ctime
        except (FileNotFoundError, NotADirectoryError):
            return -1.0

    def _clean_remote_changes(
        self, changes: dropbox.files.ListFolderResult
    ) -> dropbox.files.ListFolderResult:
        """
        Takes remote file events since last sync and cleans them up so that there is
        only a single event per path.

        Dropbox will sometimes report multiple changes per path. Once such instance is
        when sharing a folder: ``files/list_folder/continue`` will report the shared
        folder and its children as deleted and then created because the folder *is*
        actually deleted from the user's Dropbox and recreated as a shared folder which
        then gets mounted to the user's Dropbox. Ideally, we want to deal with this
        without re-downloading all its contents.

        :param changes: Result from Dropbox API call to retrieve remote changes.
        :returns: Cleaned up changes with a single Metadata entry per path.
        """

        # Note: we won't have to deal with modified or moved events,
        # Dropbox only reports DeletedMetadata or FileMetadata / FolderMetadata

        histories: Dict[str, List[Metadata]] = dict()
        for entry in changes.entries:
            add_to_bin(histories, entry.path_lower, entry)

        new_entries = []

        for h in histories.values():
            if len(h) == 1:
                new_entries.extend(h)
            else:
                last_event = h[-1]
                local_entry = self.get_index_entry(last_event.path_lower)
                was_dir = local_entry and local_entry.is_directory

                # Dropbox guarantees that applying events in the provided order will
                # reproduce the state in the cloud. We therefore keep only the last
                # event, unless there is a change in item type.
                if (
                    was_dir
                    and isinstance(last_event, FileMetadata)
                    or not was_dir
                    and isinstance(last_event, FolderMetadata)
                ):
                    deleted_event = DeletedMetadata(
                        name=last_event.name,
                        path_lower=last_event.path_lower,
                        path_display=last_event.path_display,
                        parent_shared_folder_id=last_event.parent_shared_folder_id,
                    )
                    new_entries.append(deleted_event)
                    new_entries.append(last_event)
                else:
                    new_entries.append(last_event)

        changes.entries = new_entries

        return changes

    def _create_local_entry(self, event: SyncEvent) -> SyncEvent:
        """
        Applies a file / folder change from Dropbox servers to the local Dropbox folder.
        Any :class:`errors.MaestralApiError` will be caught and logged as appropriate.
        Entries in the local index are created after successful completion.

        :param event: Dropbox metadata.
        :returns: Copy of the Dropbox metadata if the change was applied successfully,
            ``True`` if the change already existed, ``False`` in case of a
            :class:`errors.SyncError` and ``None`` if cancelled.
        """

        if self._cancel_requested.is_set():
            event.status = SyncStatus.Aborted
            self.syncing.remove(event)
            return event

        self._slow_down()

        self.clear_sync_error(dbx_path=event.dbx_path)
        event.status = SyncStatus.Syncing

        try:
            if event.is_deleted:
                res = self._on_remote_deleted(event)
            elif event.is_file:
                res = self._on_remote_file(event)
            elif event.is_directory:
                res = self._on_remote_folder(event)
            else:
                res = None

            if res is not None:
                event.status = SyncStatus.Done
            else:
                event.status = SyncStatus.Skipped

        except SyncError as e:
            self._handle_sync_error(e, direction=SyncDirection.Down)
            event.status = SyncStatus.Failed
        finally:
            self.syncing.remove(event)

        # add to history database
        if event.status == SyncStatus.Done:
            with self._database_access():
                self._db_session.add(event)

        return event

    def _on_remote_file(self, event: SyncEvent) -> Optional[SyncEvent]:
        """
        Applies a remote file change or creation locally.

        :param event: SyncEvent for file download.
        :returns: SyncEvent corresponding to local item or None if no local changes
            are made.
        """

        self._apply_case_change(event)

        # Store the new entry at the given path in your local state. If the required
        # parent folders don’t exist yet, create them. If there’s already something else
        # at the given path, replace it and remove all its children.

        conflict_check = self._check_download_conflict(event)

        if conflict_check is Conflict.Identical:
            self.update_index_from_sync_event(event)
            return None
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return None

        local_path = event.local_path

        # we download to a temporary file first (this may take some time)
        tmp_fname = self._new_tmp_file()

        try:
            md = self.client.download(f"rev:{event.rev}", tmp_fname, sync_event=event)
            event = SyncEvent.from_dbx_metadata(md, self)
        except SyncError as err:
            # replace rev number with path
            err.dbx_path = event.dbx_path
            raise err

        # re-check for conflict and move the conflict
        # out of the way if anything has changed
        if self._check_download_conflict(event) == Conflict.Conflict:
            new_local_path = generate_cc_name(
                local_path, is_fs_case_sensitive=self.is_case_sensitive
            )
            event_cls = DirMovedEvent if osp.isdir(local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(local_path, new_local_path)):
                with convert_api_errors(local_path=new_local_path):
                    move(local_path, new_local_path, raise_error=True)

            logger.debug(
                'Download conflict: renamed "%s" to "%s"', local_path, new_local_path
            )
            self.rescan(new_local_path)

        if osp.isdir(local_path):
            with self.fs_events.ignore(DirDeletedEvent(local_path)):
                delete(local_path)

        # check if we should preserve permissions of destination file
        old_entry = self.get_index_entry(event.dbx_path)

        if old_entry and event.dbx_id == old_entry.dbx_id:
            preserve_permissions = True
        else:
            preserve_permissions = False

        ignore_events = [
            FileMovedEvent(tmp_fname, local_path),
            FileCreatedEvent(local_path),  # sometimes emitted on macOS
        ]

        if preserve_permissions:
            # ignore FileModifiedEvent when changing permissions
            ignore_events.append(FileModifiedEvent(local_path))

        if osp.isfile(local_path):
            # ignore FileDeletedEvent when replacing old file
            ignore_events.append(FileDeletedEvent(local_path))

        # move the downloaded file to its destination
        with self.fs_events.ignore(*ignore_events):

            mtime = os.stat(tmp_fname).st_mtime

            with convert_api_errors(dbx_path=event.dbx_path, local_path=local_path):
                move(
                    tmp_fname,
                    local_path,
                    preserve_dest_permissions=preserve_permissions,
                    raise_error=True,
                )

        self.update_index_from_sync_event(event)
        self.save_local_hash(event.local_path, event.content_hash, mtime)

        logger.debug('Created local file "%s"', event.dbx_path)

        return event

    def _on_remote_folder(self, event: SyncEvent) -> Optional[SyncEvent]:
        """
        Applies a remote folder creation locally.

        :param event: SyncEvent for folder download.
        :returns: SyncEvent corresponding to local item or None if no local changes
            are made.
        """

        self._apply_case_change(event)

        # Store the new entry at the given path in your local state. If the required
        # parent folders don’t exist yet, create them. If there’s already something else
        # at the given path, replace it but leave the children as they are.

        conflict_check = self._check_download_conflict(event)

        if conflict_check is Conflict.Identical:
            self.update_index_from_sync_event(event)
            return None
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return None

        if conflict_check == Conflict.Conflict:
            new_local_path = generate_cc_name(
                event.local_path, is_fs_case_sensitive=self.is_case_sensitive
            )
            event_cls = DirMovedEvent if osp.isdir(event.local_path) else FileMovedEvent
            with self.fs_events.ignore(event_cls(event.local_path, new_local_path)):
                with convert_api_errors(local_path=new_local_path):
                    move(event.local_path, new_local_path, raise_error=True)

            logger.debug(
                'Download conflict: renamed "%s" to "%s"',
                event.local_path,
                new_local_path,
            )
            self.rescan(new_local_path)

        if osp.isfile(event.local_path):
            event_cls = (
                DirDeletedEvent if osp.isdir(event.local_path) else FileDeletedEvent
            )
            with self.fs_events.ignore(event_cls(event.local_path)):
                delete(event.local_path)

        try:
            with self.fs_events.ignore(
                DirCreatedEvent(event.local_path), recursive=False
            ):
                os.makedirs(event.local_path)
        except FileExistsError:
            pass
        except OSError as err:
            raise os_to_maestral_error(
                err, dbx_path=event.dbx_path, local_path=event.local_path
            )

        self.update_index_from_sync_event(event)

        logger.debug('Created local folder "%s"', event.dbx_path)

        return event

    def _on_remote_deleted(self, event: SyncEvent) -> Optional[SyncEvent]:
        """
        Applies a remote deletion locally.

        :param event: Dropbox deleted metadata.
        :returns: SyncEvent corresponding to local deletion or None if no local changes
            are made.
        """

        self._apply_case_change(event)

        # If your local state has something at the given path, remove it and all its
        # children. If there’s nothing at the given path, ignore this entry.

        conflict_check = self._check_download_conflict(event)

        if conflict_check is Conflict.Identical:
            self.update_index_from_sync_event(event)
            return None
        elif conflict_check is Conflict.LocalNewerOrIdentical:
            return None

        event_cls = DirDeletedEvent if osp.isdir(event.local_path) else FileDeletedEvent
        with self.fs_events.ignore(event_cls(event.local_path)):
            exc = delete(event.local_path)

        if not exc:
            self.update_index_from_sync_event(event)
            logger.debug('Deleted local item "%s"', event.dbx_path)
            return event
        elif isinstance(exc, (FileNotFoundError, NotADirectoryError)):
            self.update_index_from_sync_event(event)
            logger.debug('Deletion failed: "%s" not found', event.dbx_path)
            return None
        else:
            raise os_to_maestral_error(exc)

    def _apply_case_change(self, event: SyncEvent) -> None:
        """
        Applies any changes in casing of the remote item locally. This should be called
        before any system calls using ``local_path`` because the actual path on the file
        system may have a different casing on case-sensitive file systems. On case-
        insensitive file systems, this causes only a cosmetic change.

        :param event: Download SyncEvent.
        """

        with self._database_access():
            old_entry = self.get_index_entry(event.dbx_path.lower())

        if old_entry and old_entry.dbx_path_cased != event.dbx_path:

            local_path_old = self.to_local_path_from_cased(old_entry.dbx_path_cased)

            event_cls = DirMovedEvent if osp.isdir(local_path_old) else FileMovedEvent
            with self.fs_events.ignore(event_cls(local_path_old, event.local_path)):
                move(local_path_old, event.local_path)

            with self._database_access():
                old_entry.dbx_path_cased = event.dbx_path
                self._db_session.commit()

            logger.debug('Renamed "%s" to "%s"', local_path_old, event.local_path)

    def rescan(self, local_path: str) -> None:
        """
        Forces a rescan of a local path: schedules created events for every folder,
        modified events for every file and deleted events for every deleted item
        (compared to our index).

        :param local_path: Path to rescan.
        """

        logger.debug('Rescanning "%s"', local_path)

        if osp.isfile(local_path):
            self.fs_events.local_file_event_queue.put(FileModifiedEvent(local_path))
        elif osp.isdir(local_path):

            # add created and deleted events of children as appropriate

            snapshot = self._dir_snapshot_with_mignore(local_path)
            lowercase_snapshot_paths = {x.lower() for x in snapshot.paths}
            local_path_lower = local_path.lower()

            for path in snapshot.paths:
                if snapshot.isdir(path):
                    self.fs_events.local_file_event_queue.put(DirCreatedEvent(path))
                else:
                    self.fs_events.local_file_event_queue.put(FileModifiedEvent(path))

            # add deleted events

            with self._database_access():
                entries = (
                    self._db_session.query(IndexEntry)
                    .filter(IndexEntry.dbx_path_lower.like(f"{local_path_lower}%"))
                    .all()
                )

            for entry in entries:
                child_path_uncased = (
                    f"{self.dropbox_path}{entry.dbx_path_lower}".lower()
                )
                if child_path_uncased not in lowercase_snapshot_paths:
                    local_child_path = self.to_local_path_from_cased(
                        entry.dbx_path_cased
                    )
                    if entry.is_directory:
                        self.fs_events.local_file_event_queue.put(
                            DirDeletedEvent(local_child_path)
                        )
                    else:
                        self.fs_events.local_file_event_queue.put(
                            FileDeletedEvent(local_child_path)
                        )

        elif not osp.exists(local_path):
            dbx_path = self.to_dbx_path(local_path)

            local_entry = self.get_index_entry(dbx_path)

            if local_entry:
                if local_entry.is_directory:
                    self.fs_events.local_file_event_queue.put(
                        DirDeletedEvent(local_path)
                    )
                else:
                    self.fs_events.local_file_event_queue.put(
                        FileDeletedEvent(local_path)
                    )

    def _clean_history(self):
        """Commits new events and removes all events older than ``_keep_history`` from
        history."""

        with self._database_access():

            # commit previous
            self._db_session.commit()

            # drop all entries older than keep_history
            now = time.time()
            keep_history = self._conf.get("sync", "keep_history")
            query = self._db_session.query(SyncEvent)
            subquery = query.filter(
                SyncEvent.change_time_or_sync_time < now - keep_history
            )
            subquery.delete(synchronize_session="fetch")

            # commit to drive
            self._db_session.commit()

    def _scandir_with_mignore(self, path: str) -> List:
        return [
            f
            for f in os.scandir(path)
            if not self._is_mignore_path(self.to_dbx_path(f.path), f.is_dir())
        ]

    def _dir_snapshot_with_mignore(self, path: str) -> DirectorySnapshot:
        return DirectorySnapshot(
            path,
            listdir=self._scandir_with_mignore,
        )


# ======================================================================================
# Workers for upload, download and connection monitoring threads
# ======================================================================================


@contextmanager
def handle_sync_thread_errors(
    syncing: Event,
    running: Event,
    connected: Event,
    notifier: notify.MaestralDesktopNotifier,
) -> Iterator[None]:

    try:
        yield
    except DropboxServerError:
        logger.info("Dropbox server error", exc_info=True)
    except ConnectionError:
        syncing.clear()
        connected.clear()
        logger.debug("Connection error", exc_info=True)
        logger.info(DISCONNECTED)
    except Exception as err:
        running.clear()
        syncing.clear()
        title = getattr(err, "title", "Unexpected error")
        message = getattr(err, "message", "Please restart to continue syncing")
        logger.error(title, exc_info=True)
        notifier.notify(title, message, level=notify.ERROR)


def download_worker(
    sync: SyncEngine, syncing: Event, running: Event, connected: Event
) -> None:
    """
    Worker to sync changes of remote Dropbox with local folder.

    :param sync: Instance of :class:`SyncEngine`.
    :param syncing: Event that indicates if workers are running or paused.
    :param running: Event to shutdown local file event handler and worker threads.
    :param connected: Event that indicates if we can connect to Dropbox.
    """

    while running.is_set():

        syncing.wait()

        with handle_sync_thread_errors(syncing, running, connected, sync.notifier):

            has_changes = sync.wait_for_remote_changes(sync.remote_cursor)

            if not (running.is_set() and syncing.is_set()):
                continue

            sync.ensure_dropbox_folder_present()

            if has_changes:
                logger.info(SYNCING)
                sync.download_sync_cycle()
                logger.info(IDLE)

                sync.client.get_space_usage()  # update space usage


def download_worker_added_item(
    sync: SyncEngine,
    syncing: Event,
    running: Event,
    connected: Event,
    added_item_queue: "Queue[str]",
) -> None:
    """
    Worker to download items which have been newly included in sync.

    :param sync: Instance of :class:`SyncEngine`.
    :param syncing: Event that indicates if workers are running or paused.
    :param running: Event to shutdown local file event handler and worker threads.
    :param connected: Event that indicates if we can connect to Dropbox.
    :param added_item_queue: Queue with newly added items to download. Entries are
        Dropbox paths.
    """

    while running.is_set():

        syncing.wait()

        with handle_sync_thread_errors(syncing, running, connected, sync.notifier):

            dbx_path = added_item_queue.get()
            sync.pending_downloads.add(dbx_path.lower())  # protect against crashes

            with sync.sync_lock:

                if not (running.is_set() and syncing.is_set()):
                    # try again later
                    continue

                sync.get_remote_item(dbx_path)
                sync.pending_downloads.discard(dbx_path)

                logger.info(IDLE)

                # free some memory
                sync.free_memory()


def upload_worker(
    sync: SyncEngine, syncing: Event, running: Event, connected: Event
) -> None:
    """
    Worker to sync local changes to remote Dropbox.

    :param sync: Instance of :class:`SyncEngine`.
    :param syncing: Event that indicates if workers are running or paused.
    :param running: Event to shutdown local file event handler and worker threads.
    :param connected: Event that indicates if we can connect to Dropbox.
    """

    while running.is_set():

        syncing.wait()

        with handle_sync_thread_errors(syncing, running, connected, sync.notifier):

            has_changes = sync.wait_for_local_changes()

            if not (running.is_set() and syncing.is_set()):
                continue

            sync.ensure_dropbox_folder_present()

            if has_changes:
                logger.info(SYNCING)
                sync.upload_sync_cycle()
                logger.info(IDLE)


def startup_worker(
    sync: SyncEngine,
    syncing: Event,
    running: Event,
    connected: Event,
    startup: Event,
    paused_by_user: Event,
) -> None:
    """
    Worker to sync local changes to remote Dropbox.

    :param sync: Instance of :class:`SyncEngine`.
    :param syncing: Event that indicates if workers are running or paused.
    :param running: Event to shutdown local file event handler and worker threads.
    :param connected: Event that indicates if we can connect to Dropbox.
    :param startup: Set when we should run startup routines.
    :param paused_by_user: Set when syncing has been paused by the user.
    """

    while running.is_set():

        startup.wait()

        with handle_sync_thread_errors(syncing, running, connected, sync.notifier):

            # Retry failed downloads.
            if len(sync.download_errors) > 0:
                logger.info("Retrying failed syncs...")

            for dbx_path in list(sync.download_errors):
                logger.info(f"Syncing ↓ {dbx_path}")
                sync.get_remote_item(dbx_path)

            # Resume interrupted downloads.
            if len(sync.pending_downloads) > 0:
                logger.info("Resuming interrupted syncs...")

            for dbx_path in list(sync.pending_downloads):
                logger.info(f"Syncing ↓ {dbx_path}")
                sync.get_remote_item(dbx_path)
                sync.pending_downloads.discard(dbx_path)

            # retry failed / interrupted uploads by scheduling additional events
            # if len(sync.upload_errors) > 0:
            #     logger.debug('Retrying failed uploads...')
            #
            # for dbx_path in list(sync.upload_errors):
            #     sync.rescan(sync.to_local_path(dbx_path))

            sync.download_sync_cycle()
            sync.upload_local_changes_while_inactive()

            if not running.is_set():
                continue

            if not running.is_set():
                continue

            if not paused_by_user.is_set():
                syncing.set()

            startup.clear()
            logger.info(IDLE)


# ======================================================================================
# Main Monitor class to start, stop and coordinate threads
# ======================================================================================


class SyncMonitor:
    """Class to manage sync threads

    :param client: The Dropbox API client, a wrapper around the Dropbox Python SDK.
    """

    added_item_queue: "Queue[str]"
    """Queue of dropbox paths which have been newly included in syncing."""

    def __init__(self, client: DropboxClient):

        self.client = client
        self.config_name = self.client.config_name

        self._conf = MaestralConfig(self.config_name)

        self.startup = Event()
        self.connected = Event()
        self.syncing = Event()
        self.running = Event()
        self.paused_by_user = Event()
        self.paused_by_user.set()

        self.added_item_queue = Queue()  # entries are dbx_paths

        self._lock = RLock()

        self.fs_event_handler = FSEventHandler(self.syncing, self.startup)
        self.sync = SyncEngine(self.client, self.fs_event_handler)

        self._startup_time = -1.0

        self.connection_check_interval = 10
        self.connection_helper = Thread(
            target=self.connection_monitor,
            name="maestral-connection-helper",
            daemon=True,
        )
        self.connection_helper.start()

    def _with_lock(fn: FT) -> FT:  # type: ignore
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            with self._lock:
                return fn(self, *args, **kwargs)

        return cast(FT, wrapper)

    @property
    def reindex_interval(self) -> float:
        """
        Interval in sec for period reindexing. Changes will be saved to state file.
        """
        return self._conf.get("sync", "reindex_interval")

    @reindex_interval.setter
    def reindex_interval(self, interval: float) -> None:
        """Setter: reindex_interval"""
        self._conf.set("sync", "reindex_interval", interval)

    @property
    def activity(self) -> List[SyncEvent]:
        """Returns a list all items queued for or currently syncing."""
        return list(self.sync.syncing)

    @property
    def history(self) -> List[SyncEvent]:
        """A list of the last SyncEvents in our history. History will be kept for the
        interval specified by the config value``keep_history`` (defaults to two weeks)
        but at most 1,000 events will kept."""
        return self.sync.history

    @property
    def idle_time(self) -> float:
        """
        Returns the idle time in seconds since the last file change or since startup if
        there haven't been any changes in our current session.
        """

        now = time.time()
        time_since_startup = now - self._startup_time
        time_since_last_sync = now - self.sync.last_change

        return min(time_since_startup, time_since_last_sync)

    @_with_lock
    def start(self) -> None:
        """Creates observer threads and starts syncing."""

        if self.running.is_set() or self.startup.is_set():
            # do nothing if already started
            return

        self.running = Event()  # create new event to let old threads shut down

        self.local_observer_thread = Observer(timeout=40)
        self.local_observer_thread.setName("maestral-fsobserver")
        self._watch = self.local_observer_thread.schedule(
            self.fs_event_handler, self.sync.dropbox_path, recursive=True
        )
        for i, emitter in enumerate(self.local_observer_thread.emitters):
            emitter.setName(f"maestral-fsemitter-{i}")

        self.startup_thread = Thread(
            target=startup_worker,
            daemon=True,
            args=(
                self.sync,
                self.syncing,
                self.running,
                self.connected,
                self.startup,
                self.paused_by_user,
            ),
            name="maestral-sync-startup",
        )

        self.download_thread = Thread(
            target=download_worker,
            daemon=True,
            args=(
                self.sync,
                self.syncing,
                self.running,
                self.connected,
            ),
            name="maestral-download",
        )

        self.download_thread_added_folder = Thread(
            target=download_worker_added_item,
            daemon=True,
            args=(
                self.sync,
                self.syncing,
                self.running,
                self.connected,
                self.added_item_queue,
            ),
            name="maestral-folder-download",
        )

        self.upload_thread = Thread(
            target=upload_worker,
            daemon=True,
            args=(
                self.sync,
                self.syncing,
                self.running,
                self.connected,
            ),
            name="maestral-upload",
        )

        try:
            self.local_observer_thread.start()
        except OSError as err:
            new_err = fswatch_to_maestral_error(err)
            title = getattr(err, "title", "Unexpected error")
            message = getattr(err, "message", "Please restart to continue syncing")
            logger.error(f"{title}: {message}", exc_info=exc_info_tuple(new_err))
            self.sync.notifier.notify(title, message, level=notify.ERROR)

        self.running.set()
        self.syncing.clear()
        self.connected.set()
        self.startup.set()

        self.startup_thread.start()
        self.upload_thread.start()
        self.download_thread.start()
        self.download_thread_added_folder.start()

        self.paused_by_user.clear()

        self._startup_time = time.time()

    @_with_lock
    def pause(self) -> None:
        """Pauses syncing."""

        self.paused_by_user.set()
        self.syncing.clear()

        self.sync.cancel_sync()

        logger.info(PAUSED)

    @_with_lock
    def resume(self) -> None:
        """Checks for changes while idle and starts syncing."""

        if not self.paused_by_user.is_set():
            return

        self.startup.set()
        self.paused_by_user.clear()

    @_with_lock
    def stop(self) -> None:
        """Stops syncing and destroys worker threads."""

        if not self.running.is_set():
            return

        logger.info("Shutting down threads...")

        self.running.clear()
        self.syncing.clear()
        self.paused_by_user.clear()
        self.startup.clear()

        self.sync.cancel_sync()

        self.local_observer_thread.stop()
        # self.local_observer_thread.join()
        # self.upload_thread.join()

        logger.info(STOPPED)

    def connection_monitor(self) -> None:
        """
        Monitors the connection to Dropbox servers. Pauses syncing when the connection
        is lost and resumes syncing when reconnected and syncing has not been paused by
        the user.
        """

        while True:

            if check_connection("www.dropbox.com"):
                self.on_connect()
            else:
                self.on_disconnect()

            time.sleep(self.connection_check_interval)

    @_with_lock
    def on_connect(self) -> None:
        """Callback to run when we have lost the connection to Dropbox"""

        if self.running.is_set():
            if not self.connected.is_set() and not self.paused_by_user.is_set():
                self.startup.set()
            self.connected.set()

    @_with_lock
    def on_disconnect(self) -> None:
        """Callback to run when we have reestablished the connection to Dropbox"""

        if self.running.is_set():
            if self.connected.is_set():
                logger.info(DISCONNECTED)
            self.syncing.clear()
            self.connected.clear()
            self.startup.clear()

    def reset_sync_state(self) -> None:
        """Resets all saved sync state. Settings are not affected."""

        if self.syncing.is_set() or self.startup.is_set() or self.sync.busy():
            raise RuntimeError("Cannot reset sync state while syncing.")

        self.sync.remote_cursor = ""
        self.sync.local_cursor = 0.0
        self.sync.clear_index()
        self.sync.clear_sync_history()

        logger.debug("Sync state reset")

    def rebuild_index(self) -> None:
        """
        Rebuilds the rev file by comparing remote with local files and updating rev
        numbers from the Dropbox server. Files are compared by their content hashes and
        conflicting copies are created if the contents differ. File changes during the
        rebuild process will be queued and uploaded once rebuilding has completed.

        Rebuilding will be performed asynchronously.
        """

        logger.info("Rebuilding index...")

        self.pause()

        self.sync.remote_cursor = ""
        self.sync.clear_index()

        if not self.running.is_set():
            self.start()
        else:
            self.resume()


# ======================================================================================
# Helper functions
# ======================================================================================


def add_to_bin(d: Dict[Any, List], key: Hashable, value: Any):

    try:
        d[key].append(value)
    except KeyError:
        d[key] = [value]


def exc_info_tuple(exc: BaseException) -> ExecInfoType:
    """Creates an exc-info tuple from an exception."""
    return type(exc), exc, exc.__traceback__


def get_dest_path(event: FileSystemEvent) -> str:
    """
    Returns the dest_path of a file system event if present (moved events only)
    otherwise returns the src_path (which is also the "destination").

    :param event: Watchdog file system event.
    :returns: Destination path for moved event, source path otherwise.
    """
    return getattr(event, "dest_path", event.src_path)


def split_moved_event(
    event: Union[FileMovedEvent, DirMovedEvent]
) -> Tuple[FileSystemEvent, FileSystemEvent]:
    """
    Splits a FileMovedEvent or DirMovedEvent into "deleted" and "created" events of the
    same type. A new attribute ``move_id`` is added to both instances.

    :param event: Original event.
    :returns: Tuple of deleted and created events.
    """

    if event.is_directory:
        created_event_cls = DirCreatedEvent
        deleted_event_cls = DirDeletedEvent
    else:
        created_event_cls = FileCreatedEvent
        deleted_event_cls = FileDeletedEvent

    deleted_event = deleted_event_cls(event.src_path)
    created_event = created_event_cls(event.dest_path)

    move_id = uuid.uuid4()

    deleted_event.move_id = move_id
    created_event.move_id = move_id

    return deleted_event, created_event


def entries_repr(entries: List[Metadata]) -> str:
    """
    Generates a nicely formatted string repr from a list of Dropbox metadata.

    :param entries: List of Dropbox metadata.
    :returns: String representation of the list.
    """
    str_reps = [
        f"<{e.__class__.__name__}(path_display={e.path_display})>" for e in entries
    ]
    return "[" + ",\n ".join(str_reps) + "]"


_last_emit = time.time()


def throttled_log(
    log: logging.Logger, msg: str, level: int = logging.INFO, limit: int = 2
) -> None:
    """
    Emits the given log message only if the previous message was emitted more than
    ``limit`` seconds ago. This can be used to prevent spamming a log with frequent
    updates.

    :param log: Logger used to emit the message.
    :param msg: Log message.
    :param level: Log level.
    :param limit: Minimum time between log messages.
    """

    global _last_emit

    if time.time() - _last_emit > limit:
        log.log(level=level, msg=msg)
        _last_emit = time.time()


def cpu_usage_percent(interval: float = 0.1) -> float:
    """
    Returns a float representing the CPU utilization of the current process as a
    percentage. This duplicates the similar method from psutil to avoid the psutil
    dependency.

    Compares process times to system CPU times elapsed before and after the interval
    (blocking). It is recommended for accuracy that this function be called with an
    interval of at least 0.1 sec.

    A value > 100.0 can be returned in case of processes running multiple threads on
    different CPU cores. The returned value is explicitly NOT split evenly between all
    available logical CPUs. This means that a busy loop process running on a system with
    2 logical CPUs will be reported as having 100% CPU utilization instead of 50%.

    :param interval: Interval in sec between comparisons of CPU times.
    :returns: CPU usage during interval in percent.
    """

    if not interval > 0:
        raise ValueError(f"interval is not positive (got {interval!r})")

    def timer():
        return time.monotonic() * cpu_count

    st1 = timer()
    rt1 = resource.getrusage(resource.RUSAGE_SELF)
    time.sleep(interval)
    st2 = timer()
    rt2 = resource.getrusage(resource.RUSAGE_SELF)

    delta_proc = (rt2.ru_utime - rt1.ru_utime) + (rt2.ru_stime - rt1.ru_stime)
    delta_time = st2 - st1

    try:
        overall_cpus_percent = (delta_proc / delta_time) * 100
    except ZeroDivisionError:
        return 0.0
    else:
        single_cpu_percent = overall_cpus_percent * cpu_count
        return round(single_cpu_percent, 1)


def check_connection(hostname: str) -> bool:
    """
    A low latency check for an internet connection.

    :param hostname: Hostname to use for connection check.
    :returns: Connection availability.
    """
    try:
        host = socket.gethostbyname(hostname)
        s = socket.create_connection((host, 80), 2)
        s.close()
        return True
    except Exception:
        logger.debug("Connection error", exc_info=True)
        return False


def validate_encoding(local_path: str) -> None:
    """
    Validate that the path contains only characters in the reported file system
    encoding. On Unix, paths are fundamentally bytes and some platforms do not enforce
    a uniform encoding of file names despite reporting a file system encoding. Such
    paths will be handed to us in Python with "surrogate escapes" in place of the
    unknown characters.

    Since the Dropbox API and our database both require utf-8 encoded paths, we use this
    method to check and fail early on unknown characters.

    :param local_path: Path to check.
    :raises PathError: if the path contains characters with an unknown encoding.
    """

    try:
        local_path.encode()
    except UnicodeEncodeError:

        fs_encoding = sys.getfilesystemencoding()

        error = PathError(
            "Could not upload item",
            f"The file name contains characters outside of the "
            f"{fs_encoding} encoding of your file system",
        )

        error.local_path = local_path

        raise error
