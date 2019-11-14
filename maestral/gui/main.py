# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 16:23:13 2018

@author: samschott
"""

# system imports
import sys
import os
import logging
import platform
import time
from subprocess import Popen

# external packages
import click
import keyring
from keyring.errors import KeyringLocked
from PyQt5 import QtCore, QtWidgets

# maestral modules
from maestral.config.main import CONF
from maestral.sync.constants import (
    IDLE, SYNCING, PAUSED, STOPPED, DISCONNECTED, SYNC_ERROR,
    IS_MACOS_BUNDLE
)
from maestral.sync.daemon import (
    start_maestral_daemon_process,
    start_maestral_daemon_thread,
    stop_maestral_daemon_process,
    get_maestral_pid,
    get_maestral_daemon_proxy,
)
from maestral.gui.settings_window import SettingsWindow
from maestral.gui.sync_issues_window import SyncIssueWindow
from maestral.gui.rebuild_index_dialog import RebuildIndexDialog
from maestral.gui.resources import get_system_tray_icon
from maestral.gui.autostart import AutoStart
from maestral.gui.utils import (
    UserDialog,
    MaestralBackgroundTask,
    MaestralBackgroundTaskProgressDialog,
    elide_string,
)


logger = logging.getLogger(__name__)

CONFIG_NAME = os.getenv("MAESTRAL_CONFIG", "maestral")


# noinspection PyTypeChecker
class MaestralGuiApp(QtWidgets.QSystemTrayIcon):
    """A Qt GUI for the Maestral daemon."""

    mdbx = None
    _started = False

    _context_menu_visible = False

    PAUSE_TEXT = "Pause Syncing"
    RESUME_TEXT = "Resume Syncing"

    def __init__(self):
        QtWidgets.QSystemTrayIcon.__init__(self)

        self.icons = self.load_tray_icons()
        self.setIcon(DISCONNECTED)
        self.show_when_systray_available()

        self.menu = QtWidgets.QMenu()
        self.menu.aboutToShow.connect(self._onContextMenuAboutToShow)
        self.menu.aboutToHide.connect(self._onContextMenuAboutToHide)
        self.setContextMenu(self.menu)

        self.setup_ui_unlinked()

        self._n_errors = None
        self._status = None

        self.update_ui_timer = QtCore.QTimer()
        self.update_ui_timer.timeout.connect(self.update_ui)
        self.update_ui_timer.start(500)  # every 500 ms

    def setIcon(self, icon_name):
        icon = self.icons.get(icon_name, self.icons[SYNCING])
        self._current_icon = icon_name
        QtWidgets.QSystemTrayIcon.setIcon(self, icon)

    def update_ui(self):
        if self.mdbx:
            self.update_status()
            self.update_error()

    def show_when_systray_available(self):
        # If available, show icon, otherwise, set a timer to check back later.
        # This is a workaround for https://bugreports.qt.io/browse/QTBUG-61898
        if self.isSystemTrayAvailable():
            self.setIcon(self._current_icon)  # reload icon
            self.show()
        else:
            QtCore.QTimer.singleShot(1000, self.show_when_systray_available)

    def load_tray_icons(self):

        icons = dict()
        icon_mapping = {
            IDLE: "idle",
            SYNCING: "syncing",
            PAUSED: "paused",
            STOPPED: "error",
            DISCONNECTED: "disconnected",
            SYNC_ERROR: "error",
        }

        if self.contextMenuVisible() and platform.system() == "Darwin":
            color = "light"
        else:
            color = None

        for key in icon_mapping:
            icons[key] = get_system_tray_icon(icon_mapping[key], color=color)

        return icons

    def load_maestral(self):

        pending_link = not _is_linked()
        pending_dbx_folder = not os.path.isdir(CONF.get("main", "path"))

        if pending_link or pending_dbx_folder:
            from maestral.gui.setup_dialog import SetupDialog
            logger.info("Setting up Maestral...")
            done = SetupDialog.configureMaestral(pending_link)
            if done:
                logger.info("Successfully set up Maestral")
            else:
                logger.info("Setup aborted. Quitting.")
                self.quit()

        self.mdbx = self._get_or_start_maestral_daemon()
        self.setup_ui_linked()

    def _get_or_start_maestral_daemon(self):

        pid = get_maestral_pid(CONFIG_NAME)
        if pid:
            self._started = False
        else:
            if IS_MACOS_BUNDLE:
                res = start_maestral_daemon_thread(CONFIG_NAME)
            else:
                res = start_maestral_daemon_process(CONFIG_NAME)
            self._started = True

            if res is False:
                error_dialog = UserDialog(
                    "Could not start Maestral",
                    "Could not start or connect to sync daemon. Please try again and " +
                    "contact the developer if this issue persists."
                )
                error_dialog.exec_()
                self.quit()

        return get_maestral_daemon_proxy(CONFIG_NAME)

    def setup_ui_unlinked(self):

        self.setToolTip("Not linked.")

        self.autostart = AutoStart()

        # ------------- populate context menu -------------------

        self.menu.clear()

        self.openDropboxFolderAction = self.menu.addAction("Open Dropbox Folder")
        self.openDropboxFolderAction.setEnabled(False)
        self.openWebsiteAction = self.menu.addAction("Launch Dropbox Website")

        self.separator1 = self.menu.addSeparator()

        self.statusAction = self.menu.addAction("Setting up...")
        self.statusAction.setEnabled(False)

        self.separator2 = self.menu.addSeparator()

        self.loginAction = self.menu.addAction("Start on login")
        self.loginAction.setCheckable(True)
        self.loginAction.triggered.connect(self.autostart.toggle)
        self.helpAction = self.menu.addAction("Help Center")

        self.separator5 = self.menu.addSeparator()

        self.quitAction = self.menu.addAction("Quit Maestral")

        # ------------- connect callbacks for menu items -------------------
        self.openDropboxFolderAction.triggered.connect(
            lambda: click.launch(self.mdbx.dropbox_path))
        self.openWebsiteAction.triggered.connect(self.on_website_clicked)
        self.loginAction.setChecked(self.autostart.enabled)
        self.helpAction.triggered.connect(self.on_help_clicked)
        self.quitAction.triggered.connect(self.quit)

    def setup_ui_linked(self):

        if not self.mdbx:
            return

        self.setToolTip(IDLE)

        # ----------------- create windows ----------------------
        self.settings = SettingsWindow(self, self.mdbx)
        self.sync_issues_window = SyncIssueWindow(self.mdbx)

        # ------------- populate context menu -------------------

        self.menu.clear()

        self.openDropboxFolderAction = self.menu.addAction("Open Dropbox Folder")
        self.openWebsiteAction = self.menu.addAction("Launch Dropbox Website")

        self.separator1 = self.menu.addSeparator()

        self.accountEmailAction = self.menu.addAction(self.mdbx.get_conf("account", "email"))
        self.accountEmailAction.setEnabled(False)

        self.accountUsageAction = self.menu.addAction(self.mdbx.get_conf("account", "usage"))
        self.accountUsageAction.setEnabled(False)

        self.separator2 = self.menu.addSeparator()

        self.statusAction = self.menu.addAction(IDLE)
        self.statusAction.setEnabled(False)
        if self.mdbx.syncing:
            self.pauseAction = self.menu.addAction(self.PAUSE_TEXT)
        else:
            self.pauseAction = self.menu.addAction(self.RESUME_TEXT)
        self.recentFilesMenu = self.menu.addMenu("Recently Changed Files")

        self.separator3 = self.menu.addSeparator()

        self.preferencesAction = self.menu.addAction("Preferences...")
        self.updatesAction = self.menu.addAction("Check for Updates...")
        self.helpAction = self.menu.addAction("Help Center")

        self.separator4 = self.menu.addSeparator()

        self.syncIssuesAction = self.menu.addAction("Show Sync Issues...")
        self.rebuiltAction = self.menu.addAction("Rebuild index...")

        self.separator5 = self.menu.addSeparator()

        if self._started:
            self.quitAction = self.menu.addAction("Quit Maestral")
        else:
            self.quitAction = self.menu.addAction("Quit Maestral GUI")

        # --------- connect callbacks for menu items ------------
        self.openDropboxFolderAction.triggered.connect(
            lambda: click.launch(self.mdbx.dropbox_path))
        self.openWebsiteAction.triggered.connect(self.on_website_clicked)
        self.pauseAction.triggered.connect(self.on_start_stop_clicked)
        self.preferencesAction.triggered.connect(self.settings.populate_gui)
        self.preferencesAction.triggered.connect(self.settings.show)
        self.preferencesAction.triggered.connect(self.settings.raise_)
        self.preferencesAction.triggered.connect(self.settings.activateWindow)
        self.updatesAction.triggered.connect(self.on_check_for_updates)
        self.syncIssuesAction.triggered.connect(self.sync_issues_window.show)
        self.syncIssuesAction.triggered.connect(self.sync_issues_window.raise_)
        self.syncIssuesAction.triggered.connect(self.sync_issues_window.activateWindow)
        self.rebuiltAction.triggered.connect(self.on_rebuild)
        self.helpAction.triggered.connect(self.on_help_clicked)
        self.quitAction.triggered.connect(self.quit)

        if platform.system() == "Linux":
            # on linux, submenu.aboutToShow may not be emitted
            # (see https://bugreports.qt.io/browse/QTBUG-55911)
            # therefore, we update the recent files list when the main menu is about to show
            self.menu.aboutToShow.connect(self.update_recent_files)
        else:
            self.recentFilesMenu.aboutToShow.connect(self.update_recent_files)

        # --------------- switch to idle icon -------------------
        self.setIcon(IDLE)

        # ----------- check for updates and notify user ---------
        self._update_timer = QtCore.QTimer()
        self._update_timer.timeout.connect(self.auto_check_for_updates)
        self._update_timer.start(30*60*1000)  # every 30 min

    # callbacks for user interaction

    def auto_check_for_updates(self):

        last_update_check = self.mdbx.get_conf("app", "update_notification_last")
        interval = self.mdbx.get_conf("app", "update_notification_interval")
        if interval == 0:  # checks disabled
            return
        elif time.time() - last_update_check > interval:
            checker = MaestralBackgroundTask(self, "check_for_updates")
            checker.sig_done.connect(
                lambda res: self._notify_updates(res, user_requested=False))

    def on_check_for_updates(self):

        checker = MaestralBackgroundTask(self, "check_for_updates")
        self._pd = MaestralBackgroundTaskProgressDialog("Checking for Updates")
        self._pd.show()
        self._pd.rejected.connect(lambda: checker.sig_done.disconnect(self._notify_updates))

        checker.sig_done.connect(self._pd.accept)
        checker.sig_done.connect(self._notify_updates)

    def _notify_updates(self, res, user_requested=True):

        if user_requested and res["error"]:
            update_dialog = UserDialog("Could not check for updates", res["error"])
            update_dialog.exec_()

        elif res["update_available"]:
            if not user_requested:  # save last update time
                self.mdbx.set_conf("app", "update_notification_last", time.time())
            url_r = "https://github.com/samschott/maestral-dropbox/releases"
            message = (
                'Maestral v{0} is available. Please use your package manager to '
                'update Maestral or go to the <a href=\"{1}\"><span '
                'style="text-decoration: underline; color:#2874e1;">releases</span></a> '
                'page to download the new version. '
                '<div style="height:5px;font-size:5px;">&nbsp;<br></div>'
                '<b>Release notes:</b>'
            ).format(res["latest_release"], url_r)
            list_style = '<ul style="margin-top: 0px; margin-bottom: 0px; margin-left: -20px; margin-right: 0px; -qt-list-indent: 1;">'
            styled_release_notes = res["release_notes"].replace('<ul>', list_style)
            update_dialog = UserDialog("Update available", message, styled_release_notes)
            update_dialog.exec_()

        elif user_requested and not res["update_available"]:
            message = 'Maestral v{} is the newest version available.'.format(res["latest_release"])
            update_dialog = UserDialog("You’re up-to-date!", message)
            update_dialog.exec_()

    @staticmethod
    def on_website_clicked():
        """Open the Dropbox website."""
        click.launch("https://www.dropbox.com/")

    @staticmethod
    def on_help_clicked():
        """Open the Dropbox help website."""
        click.launch("https://dropbox.com/help")

    def on_start_stop_clicked(self):
        """Pause / resume syncing on menu item clicked."""
        if self.pauseAction.text() == self.PAUSE_TEXT:
            self.mdbx.pause_sync()
            self.pauseAction.setText(self.RESUME_TEXT)
        elif self.pauseAction.text() == self.RESUME_TEXT:
            self.mdbx.resume_sync()
            self.pauseAction.setText(self.PAUSE_TEXT)
        elif self.pauseAction.text() == "Start Syncing":
            self.mdbx.start_sync()
            self.pauseAction.setText(self.PAUSE_TEXT)

    def update_error(self):
        errs = self.mdbx.get_maestral_errors()

        if not errs:
            return
        else:
            self.mdbx.clear_maestral_errors()

        err = errs[-1]

        if err["type"] in ("RevFileError", "BadInputError"):
            title = err["title"]
            message = err["message"]
            self._stop_and_exec_error_dialog(title, message)
        elif err["type"] == "CursorResetError":
            title = "Dropbox has reset its sync state."
            message = 'Please go to "Rebuild index..." to re-sync your Dropbox.'
            self._stop_and_exec_error_dialog(title, message)
        elif err["type"] == "DropboxDeletedError":
            self.mdbx.stop_sync()
            self.restart()
        elif err["type"] == "DropboxAuthError":
            from maestral.gui.relink_dialog import RelinkDialog
            self._stop_and_exec_relink_dialog(RelinkDialog.REVOKED)
        elif err["type"] == "TokenExpiredError":
            from maestral.gui.relink_dialog import RelinkDialog
            self._stop_and_exec_relink_dialog(RelinkDialog.EXPIRED)
        else:
            title = "An unexpected error occurred."
            message = ("Please restart Maestral to continue syncing and contact "
                       "the developer with the information below.")
            self._stop_and_exec_error_dialog(title, message, err["traceback"])
            self.mdbx.start_sync()  # resume sync again

    def on_rebuild(self):

        self.rebuild_dialog = RebuildIndexDialog(self.mdbx)
        self.rebuild_dialog.show()
        self.rebuild_dialog.activateWindow()
        self.rebuild_dialog.raise_()

    def _stop_and_exec_relink_dialog(self, reason):
        from maestral.gui.relink_dialog import RelinkDialog

        self.setIcon(SYNC_ERROR)

        if self.mdbx:
            self.mdbx.stop_sync()
        if hasattr(self, "pauseAction"):
            self.pauseAction.setText("Start Syncing")
            self.pauseAction.setEnabled(False)

        relink_dialog = RelinkDialog(self, reason)
        # Will either just return (Cancel), relink the account (Link) or unlink it and
        # delete the old creds (Unlink). In the first case

        relink_dialog.exec_()  # this will perform quit actions as appropriate

    def _stop_and_exec_error_dialog(self, title, message, exc_info=None):
        self.setIcon(SYNC_ERROR)

        if self.mdbx:
            self.mdbx.stop_sync()
        if hasattr(self, "pauseAction"):
            self.pauseAction.setText("Start Syncing")

        error_dialog = UserDialog(title, message, exc_info)
        error_dialog.exec_()

    # callbacks to update GUI

    def update_recent_files(self):
        """Update menu with list of recently changed files."""
        self.recentFilesMenu.clear()
        for dbx_path in reversed(self.mdbx.get_conf("internal", "recent_changes")):
            file_name = os.path.basename(dbx_path)
            truncated_name = elide_string(file_name, font=self.menu.font(), side="right")
            local_path = self.mdbx.to_local_path(dbx_path)
            action = self.recentFilesMenu.addAction(truncated_name)
            action.triggered.connect(
                lambda _, lp=local_path: click.launch(lp, locate=True))

    def update_status(self):
        """Change icon according to status."""

        n_errors = len(self.mdbx.sync_errors)
        status = self.mdbx.status
        is_paused = self.mdbx.paused

        if status == self._status and n_errors == self._n_errors:
            return

        # update icon
        if is_paused:
            new_icon = PAUSED
        else:
            new_icon = status

        self.setIcon(new_icon)

        # update action texts
        if n_errors > 0:
            self.syncIssuesAction.setText("Show Sync Issues ({0})...".format(n_errors))
        else:
            self.syncIssuesAction.setText("Show Sync Issues...")

        self.pauseAction.setText(self.RESUME_TEXT if is_paused else self.PAUSE_TEXT)
        self.accountUsageAction.setText(self.mdbx.get_conf("account", "usage"))

        status_short = elide_string(status)
        self.statusAction.setText(status_short)

        # update sync issues window
        if n_errors != self._n_errors:
            self.sync_issues_window.reload()

        # update tooltip
        self.setToolTip(status_short)

        # cache status
        self._n_errors = n_errors
        self._status = status

    def _onContextMenuAboutToShow(self):
        self._context_menu_visible = True

        if platform.system() == "Darwin":
            self.reload_icons()

    def _onContextMenuAboutToHide(self):
        self._context_menu_visible = False

        if platform.system() == "Darwin":
            self.reload_icons()

    def reload_icons(self):
        self.icons = self.load_tray_icons()
        self.setIcon(self._current_icon)

    def contextMenuVisible(self):
        return self._context_menu_visible

    def setToolTip(self, text):
        if not platform.system() == "Darwin":
            # tray icons in macOS should not have tooltips
            QtWidgets.QSystemTrayIcon.setToolTip(self, text)

    def quit(self, *args, stop_daemon=None):
        """Quits Maestral.

        :param bool stop_daemon: If ``True``, the sync daemon will be stopped when
            quitting the GUI, if ``False``, it will be kept alive. If ``None``, the daemon
            will only be stopped if it was started by the GUI (default).
        """

        if stop_daemon is None:
            stop_daemon = self._started

        # stop update timer to stop communication with daemon
        self.update_ui_timer.stop()

        # stop sync daemon if we started it or ``stop_daemon==True``
        if stop_daemon and self.mdbx and not IS_MACOS_BUNDLE:
            self.mdbx._pyroRelease()
            stop_maestral_daemon_process(CONFIG_NAME)

        # quit
        self.deleteLater()
        QtCore.QCoreApplication.quit()
        sys.exit(0)

    def restart(self):
        """Restarts the Maestral GUI and sync daemon."""

        # schedule restart after current process has quit
        pid = os.getpid()  # get ID of current process
        config_name = os.getenv("MAESTRAL_CONFIG", "maestral")
        if IS_MACOS_BUNDLE:
            launch_command = os.path.join(sys._MEIPASS, "main")
            Popen("lsof -p {0} +r 1 &>/dev/null; {0}".format(launch_command), shell=True)
        if platform.system() == "Darwin":
            Popen("lsof -p {0} +r 1 &>/dev/null; maestral gui --config-name='{1}'".format(
                pid, config_name), shell=True)
        elif platform.system() == "Linux":
            Popen("tail --pid={0} -f /dev/null; maestral gui --config-name='{1}'".format(
                pid, config_name), shell=True)

        # quit Maestral
        self.quit(stop_daemon=True)


def _is_linked():
    """
    Checks if auth key has been saved.

    :raises: ``KeyringLocked`` if the system keyring cannot be accessed.
    """
    account_id = CONF.get("account", "account_id")
    try:
        if account_id == "":
            access_token = None
        else:
            access_token = keyring.get_password("Maestral", account_id)
        return access_token
    except KeyringLocked:
        info = "Please make sure that your keyring is unlocked and restart Maestral."
        raise KeyringLocked(info)


def run():
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_UseHighDpiPixmaps)

    app = QtWidgets.QApplication(["Maestral GUI"])
    app.setQuitOnLastWindowClosed(False)

    maestral_gui = MaestralGuiApp()
    app.processEvents()  # refresh ui before loading the Maestral daemon
    maestral_gui.load_maestral()
    sys.exit(app.exec_())


if __name__ == "__main__":
    run()
