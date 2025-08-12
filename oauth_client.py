import requests
import time
from flask import session

class OAuthClient:
    def __init__(self, token_url, client_id, client_secret, code):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        #self.token = None
        self.code = code 
        self.acess_token = None
        self.token_expiry = 0
        
    def get_access_token(self):
        """Get a fresh access token if expired or missing"""
        if self.acess_token and time.time() < self.token_expiry:
            return self.acess_token

        payload = {'grant_type': 'authorization_code',
                   'response_type' : 'token',
                   'code' : self.code}
        #payload = {'grant_type': 'authorization_code'}
        response = requests.post(
            self.token_url,
            data=payload,
            auth=(self.client_id, self.client_secret)
        )

        if response.status_code != 200:
            raise Exception(f"Token request failed: {response.status_code} {response.text}")

        token_data = response.json()
        self.acess_token = token_data["access_token"]
        print(session)
        self.token_expiry = time.time() + token_data.get("expires_in", 3600) - 60
        return self.acess_token

    def call_api(self, url, method="GET", headers=None, **kwargs):
        """Call an OAuth-protected API endpoint with Bearer token"""
        token = self.get_access_token()
        auth_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        if headers:
            auth_headers.update(headers)

        response = requests.request(method, url, headers=auth_headers, **kwargs)
        response.raise_for_status()
        return response.json()

    def call_api_session(self, url, method="GET", headers=None, **kwargs):
        """Call an OAuth-protected API endpoint with Bearer token"""
        token = session['access_token']
        auth_headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        if headers:
            auth_headers.update(headers)

        response = requests.request(method, url, headers=auth_headers, **kwargs)
        response.raise_for_status()
        return response.json()