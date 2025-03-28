import threading
import time
from typing import Optional
from msal import ConfidentialClientApplication
from pfg_app import settings
from pfg_app.logger_module import logger

class MSGraphAPITokenManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(MSGraphAPITokenManager, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.client_id = settings.graph_client_id # TODO:FLAG_GRAPH
        self.client_secret = settings.graph_client_secret # TODO:FLAG_GRAPH
        self.tenant_id = settings.graph_tenant_id # TODO:FLAG_GRAPH
        
        self._access_token = None
        self._token_expiration = 0
        
        self._app = ConfidentialClientApplication(
            client_id=self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )
        # Threading lock for thread-safe token operations
        self._token_lock = threading.Lock()

    def get_access_token(self) -> Optional[str]:
        """
        Retrieve a valid access token, generating a new one if necessary
        """
        with self._token_lock:
            # Check if we have a valid token
            current_time = int(time.time())
            if (self._access_token and current_time < self._token_expiration - 300):  # 5 minute buffer
                return self._access_token
            
            # Define scopes
            scopes = ['https://graph.microsoft.com/.default'] # TODO:FLAG_GRAPH
            try:
                result = self._app.acquire_token_for_client(scopes=scopes)
                if "access_token" in result:
                    self._access_token = result['access_token'] 
                    self._token_expiration = current_time + result.get('expires_in', 3600)
                    return self._access_token
                else:
                    logger.info(f"Token acquisition failed: {result.get('error')}")
                    return None
            
            except Exception as e:
                logger.info(f"Error obtaining access token: {e}")
                return None