from azure.identity import ClientSecretCredential
import requests
import rdata
import os

from dotenv import load_dotenv


class XmartExtractor:
    def __init__(self):
        # key = rdata.read_rda("./WIISEMART_OData_key.RData")
        # authn = key["authn"]
        # authn_resource = authn["resource"][0]
        # authn_tenant = authn["tenant"][0]

        # load_dotenv()
        authn_app = os.getenv("AUTHN_APP")
        authn_password = os.getenv("AUTHN_PASSWORD")
        authn_resource = os.getenv("AUTHN_RESOURCE")
        authn_tenant = os.getenv("AUTHN_TENANT")

        credential = ClientSecretCredential(
            tenant_id=authn_tenant, client_id=authn_app, client_secret=authn_password
        )

        self.access_token = credential.get_token(authn_resource + "/.default")
        self.headers = {"Authorization": f"Bearer {self.access_token.token}"}
        self.base_url = "https://extranet.who.int/xmart-api/odata/WIISE/"

    def get(self, path):
        print(path)
        response = requests.get(self.base_url + path, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            return response.status_code
