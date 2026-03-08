"""
QB Connection Manager — COM connection to QuickBooks Desktop Enterprise.

Uses QBXMLRP2.RequestProcessor via pywin32 COM automation.
QB Desktop must be running with the company file open.
"""

import logging
import win32com.client
import pythoncom

logger = logging.getLogger("qb_poller")

# Connection modes for BeginSession
QB_OPEN_DO_NOT_CARE = 2     # Open regardless of single/multi-user mode
QB_OPEN_SINGLE_USER = 0     # Require single-user mode
QB_OPEN_MULTI_USER  = 1     # Require multi-user mode


class QBConnection:
    """Manages COM connection to QuickBooks Desktop via QBXMLRP2."""

    def __init__(self, company_file: str, app_name: str = "AAE ERP Poller",
                 app_id: str = ""):
        self.company_file = company_file
        self.app_name = app_name
        self.app_id = app_id
        self._session_manager = None
        self._ticket = None

    def connect(self) -> None:
        """Open connection and begin session with QB Desktop."""
        # Initialize COM for this thread
        pythoncom.CoInitialize()

        try:
            logger.info("Connecting to QuickBooks Desktop...")
            self._session_manager = win32com.client.Dispatch(
                "QBXMLRP2.RequestProcessor"
            )

            # OpenConnection2(appID, appName, connType)
            # connType 1 = localQBD (QuickBooks Desktop on this machine)
            self._session_manager.OpenConnection2(
                self.app_id, self.app_name, 1
            )
            logger.info("Connection opened to QB Desktop")

            # BeginSession(companyFile, openMode)
            self._ticket = self._session_manager.BeginSession(
                self.company_file, QB_OPEN_DO_NOT_CARE
            )
            logger.info(f"Session started with: {self.company_file}")

        except Exception as e:
            self._cleanup()
            raise ConnectionError(
                f"Failed to connect to QuickBooks Desktop. "
                f"Ensure QB is running with the company file open. "
                f"Error: {e}"
            ) from e

    def execute_request(self, qbxml_request: str) -> str:
        """Send a QBXML request and return the XML response string."""
        if not self._ticket or not self._session_manager:
            raise ConnectionError("Not connected to QuickBooks. Call connect() first.")

        try:
            response = self._session_manager.ProcessRequest(
                self._ticket, qbxml_request
            )
            return response
        except Exception as e:
            logger.error(f"QBXML request failed: {e}")
            raise

    def disconnect(self) -> None:
        """End session and close connection."""
        try:
            if self._ticket and self._session_manager:
                self._session_manager.EndSession(self._ticket)
                logger.info("QB session ended")
        except Exception as e:
            logger.warning(f"Error ending session: {e}")

        try:
            if self._session_manager:
                self._session_manager.CloseConnection()
                logger.info("QB connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

        self._cleanup()

    def _cleanup(self):
        """Release COM resources."""
        self._ticket = None
        self._session_manager = None
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False


def test_connection(company_file: str) -> bool:
    """Quick test to verify QB Desktop connection works."""
    try:
        with QBConnection(company_file) as conn:
            # Simple company query to verify connection
            request = """<?xml version="1.0" encoding="utf-8"?>
<?qbxml version="16.0"?>
<QBXML>
  <QBXMLMsgsRq onError="stopOnError">
    <CompanyQueryRq>
    </CompanyQueryRq>
  </QBXMLMsgsRq>
</QBXML>"""
            response = conn.execute_request(request)
            logger.info(f"Connection test successful. Response length: {len(response)}")
            return True
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
