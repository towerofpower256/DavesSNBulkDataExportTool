import logging
import argparse
import csv
from typing import TYPE_CHECKING
import requests # For making web calls
from requests.auth import HTTPBasicAuth
from requests.models import HTTPError
import time # For execution timing
from datetime import timedelta

class SNDataExport:
    HEADER_COUNT="X-Total-Count"
    HEADER_CONTENTTYPE="Content-Type"
    HEADER_CONTENTTYPE_DEFAULT="application/json"
    HEADER_ACCEPT="Accept"
    HEADER_ACCEPT_DEFAULT="application/json"
    INSTANCEURL_DEFAULT="{name}.service-now.com"
    PAGESIZE_DEFAULT=500
    MARK_DISPLAYVALUE="$DISPLAY"
    MARK_PLAINVALUE="$VALUE"
    AUTH_NONE="none"
    AUTH_BASIC="basic"

    def __init__(self):
        self.log = logging.getLogger("SNDataExport")
        self.options = {}

    def setupOptions(self, opts={}):
        self.log.debug("Setting options")
        self.options = opts

        #Debug
        self.log.debug("OPTIONS")
        for k,v in self.options.items():
            self.log.debug("{k} : {v}".format(k=k, v=v))

    def setOption(self, name, value):
        self.options[name] = value

    def getOption(self, name, default=""):
        if (not name in self.options):
            return default
        else:
            return self.options[name]

    def openOutputFile(self):
        filename = self.getOption("outputName")
        self.log.info("Output file: {0}".format(filename))
        try:
            self.csvFile = open(file=filename, mode='w')
        except Exception as ex:
            self.log.error("Failed to open the output file for writing.")
            raise ex

    def closeOutputFile(self):
        self.log.debug("Closing output file")
        if (self.csvFile):
            self.csvFile.close()
            self.csvFile = False

    def loadAndValidate(self):
        # Set the target URL
        # If the instance URL is empty, auto set it using the instanceName
        self.instanceUrl = self.getOption("instanceUrl")
        if (not self.instanceUrl):
            self.instanceName = self.getOption("instanceName")
            if (not self.instanceName):
                raise ValueError("Both instance name and instance URL cannot be empty")

            self.instanceUrl = self.INSTANCEURL_DEFAULT.format(name=self.getOption("instanceName"))

        self.log.info("Instance URL: {0}".format(self.instanceUrl))

        # Validate authentication
        authType = self.getOption("authType")
        if (authType == self.AUTH_NONE):
            # Nothing to validate here
            pass
        if (authType == self.AUTH_BASIC):
            if (not self.getOption("basic_auth_username")):
                raise ValueError("Username cannot be empty when using basic authentication")
            if (not self.getOption("basic_auth_password")):
                raise ValueError("Password cannot be empty when using basic authentication")

        self.log.info("Exporting from table: {0}".format(self.getOption("table")))
        self.log.info("Query: {0}".format(self.getOption("query")))
        
        rowLimit = int(self.getOption("row_limit"))
        if (rowLimit > 0): self.log.info("Row limit: {0}".format(rowLimit))

    def run(self):
        self.log.info("Running")
        
        try:
            self.loadAndValidate()

            self.openOutputFile()

        
            self.pageSizeInt = int(self.getOption("pageSize"))
            self.pageIdx = 0
            self.pageOffset = 0
            self.rowLimit = int(self.getOption("row_limit"))
            self.rowCount = 0

            commonParams = {}
            commonParams["sysparm_limit"] = self.getOption("pageSize", self.pageSizeInt)
            commonParams["sysparm_exclude_reference_link"] = "true" # Always exclude reference links
            commonParams["sysparm_display_value"] = bool(self.getOption("display_value"))
            commonParams["sysparm_query"] = self.getOption("query")
            if (self.getOption("fields")): commonParams["sysparm_fields"] = self.getOption("fields")

            self.log.info("Page 1 offset 0 - {max}".format(max=self.pageSizeInt))
            firstParams = commonParams.copy()
            firstResponse = self.makeRequest(self.getOption("table"), firstParams)
            firstResponseJson = firstResponse.json()
            firstResponseCount = self.getResultCountFromJson(firstResponseJson)

            if (firstResponseCount == 0):
                self.log.info("No data returned, nothing to do here")
                return
            
            self.log.info("Results: {0}".format(firstResponseCount))

            # Write the first line
            self.log.debug("Initializing DictWriter")
            self.csvWriter = csv.DictWriter(
                self.csvFile,
                dialect='unix',
                fieldnames=self.getHeaderNamesFromJson(firstResponseJson["result"][0])
                )
            
            self.log.debug("Writing headers")
            self.csvWriter.writeheader()
            

            # Write the other rows
            self.log.debug("Writing results")
            for row in firstResponseJson["result"]:
                if (self.rowLimit > 0 and self.rowCount > self.rowLimit):
                    self.log.info("Row limit reached: {0}".format(self.rowLimit))
                    break
                self.csvWriter.writerow(row)
                self.rowCount = self.rowCount + 1

            
            if (firstResponseCount < self.pageSizeInt):
                self.log.info("No more pages")
            else:
                while (True):
                    # Pagenate
                    self.pageIdx = self.pageIdx + 1
                    self.pageOffset = self.pageIdx * self.pageSizeInt
                    self.log.info("Getting page {pageIdx}, offset {min} - {max}".format(
                        pageIdx=self.pageIdx+1, min=self.pageOffset, max=self.pageOffset+self.pageSizeInt
                        )
                        )
                    
                    pageParams = commonParams.copy()
                    pageParams["sysparm_offset"] = self.pageOffset
                    pageResponse = self.makeRequest(self.getOption("table"), pageParams)
                    pageResponseJson = pageResponse.json()
                    pageResponseCount = self.getResultCountFromJson(pageResponseJson)

                    self.log.info("Results: {0}".format(pageResponseCount))

                    self.log.debug("Writing results")
                    for row in pageResponseJson["result"]:
                        if (self.rowLimit > 0 and self.rowCount > self.rowLimit):
                            self.log.info("Row limit reached: {0}".format(self.rowLimit))
                            break

                        self.csvWriter.writerow(row)
                        self.rowCount = self.rowCount + 1
                        
                    if (pageResponseCount < self.pageSizeInt):
                        self.log.info("No more pages")
                        return

        except Exception as ex:
            self.log.error("Unhandled exception occurred! {0}".format(ex.__class__.__name__)) 
            raise ex
        finally:
            self.closeOutputFile()


    def getResultCountFromJson(self,json):
        c = json["result"]
        if (not isinstance(c, list)):
            raise TypeError("'result' in response body is either missing or is not an array")

        return len(c)

    def getHeaderNamesFromJson(self, json):
        r = []
        for attribute, value in json.items():
            r.append(attribute)

        self.log.debug("Headers: "+(", ".join(r)))
        return r

    def makeRequest(self, table, params):
        """Make the request to ServiceNow for data"""

        url = "https://{instanceUrl}/api/now/table/{table}".format(instanceUrl=self.instanceUrl,table=table)
        headers = {self.HEADER_CONTENTTYPE: self.HEADER_CONTENTTYPE_DEFAULT, self.HEADER_ACCEPT: self.HEADER_ACCEPT_DEFAULT}
        auth=False
        authType = self.getOption("authType") # No auth
        if (authType == self.AUTH_BASIC):
            auth=(self.getOption("basic_auth_username"), self.getOption("basic_auth_password")) # Basic auth

        response = requests.get(url, headers=headers, params=params, auth=auth)
        self.log.debug("Response: "+str(response.status_code)+"\n"+str(response.text))
        try:
            response.raise_for_status() # throw error, if bad response
        except HTTPError as ex:
            self.log.error("HTTPError\n{m}\n{r}".format(m=ex.__str__(), r=ex.response.text))
            raise ex

        return response


# ========================
# MAIN CODE
# ========================

# Argument parsing
parser = argparse.ArgumentParser(description="David McDonald's ServiceNow Bulk Data Export tool 2021.\nFor exporting bulk amounts of data out of ServiceNow.")
parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging.")
parser.add_argument("-p", "--pagesize", dest="pageSize", type=int, default=SNDataExport.PAGESIZE_DEFAULT, help="Rows per page / request. Default: {0}".format(SNDataExport.PAGESIZE_DEFAULT))
parser.add_argument("-t", "--table", type=str, required=True, help="The ServiceNow table name to export from. E.g. 'incident'")
parser.add_argument("-n", "--instance-name", dest="instanceName", type=str, help="The name of the ServiceNow instance. E.g. 'dev71826'")
parser.add_argument("-N", "--instance-url", dest="instanceUrl", type=str, help="The hostname URL of the ServiceNow instance, if the instance has a custom URL. E.g. 'dev71826.custom-url.co.uk")
parser.add_argument("-d", "--display-value", action="store_true", help="Fetch displayvalues, instead of system values")
parser.add_argument("-o", "--output", dest="outputName", type=str, required=True, help="Name of the file to save the data to.")
parser.add_argument("-a", "--auth-mode", dest="authType", choices=["none", "basic"], default="none", help="Type of authentication. Default: none")
parser.add_argument("-q", "--query", type=str, default="ORDERBYsys_id", help="Query to use on the table when fetching data.")
parser.add_argument("-l", "--row-limit", type=int, default=0, help="Limit how many rows to export")
parser.add_argument("-f", "--fields", type=str, help="A comma-separated list of fields to include. Can dot-walk through reference fields (e.g. caller_id.email). Leave blank for all fields.")
parser.add_argument("--basic-username", dest="basic_auth_username", type=str, help="Basic authentication username.")
parser.add_argument("--basic-password", dest="basic_auth_password", type=str, help="Basic authentication password.")

args = vars(parser.parse_args())

# Setup logging
logLevel = logging.INFO
if (args["verbose"]):
    logLevel = logging.DEBUG

logging.basicConfig(level=logLevel)

# Run
startTime = time.perf_counter()
snde = SNDataExport()
snde.setupOptions(args)
snde.run()
snde.log.info("Finished!")
snde.log.info("Execution time: {0}s".format(round(time.perf_counter() - startTime, 2)))