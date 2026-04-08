"""SOAP client for target-sherpaan."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import xmltodict
from requests import Session
from tenacity import retry, stop_after_attempt, wait_exponential

from target_sherpaan.auth import SherpaAuth

# Set up logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)


class SherpaClient:
    """SOAP client for Sherpa API."""

    def __init__(
        self,
        auth: SherpaAuth,
        timeout: int = 300,
    ) -> None:
        """Initialize the Sherpa SOAP client.

        Args:
            auth: Authentication handler
            timeout: Request timeout in seconds
        """
        self.auth = auth
        self.timeout = timeout
        self.session = Session()
        self.session.headers.update({
            "Content-Type": "application/soap+xml; charset=utf-8",
            "User-Agent": "PostmanRuntime/7.32.3",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        self.logger = logging.getLogger(__name__)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def call_soap_service(
        self,
        service_name: str,
        soap_envelope: str
    ) -> Dict[str, Any]:
        """Call a SOAP service with a custom envelope.

        Args:
            service_name: Name of the SOAP service (for SOAPAction header)
            soap_envelope: The complete SOAP envelope XML

        Returns:
            Parsed response dictionary
        """
        self.session.headers.update({
            "SOAPAction": f'"http://sherpa.sherpaan.nl/{service_name}"'
        })

        # Clean up URL - remove query params and ensure proper format
        url = self.auth.base_url.replace("?wsdl", "").split("?")[0]
        if not url.endswith(".asmx"):
            url = f"{url}/Sherpa.asmx"

        try:
            self.logger.info(f"Calling {service_name} at {url}")
            self.logger.info(f"SOAPAction header: http://sherpa.sherpaan.nl/{service_name}")
            # Log the XML being sent for debugging (truncate if too long)
            if len(soap_envelope) > 2000:
                self.logger.debug(f"SOAP envelope (first 2000 chars): {soap_envelope[:2000]}")
            else:
                self.logger.debug(f"SOAP envelope: {soap_envelope}")
            
            response = self.session.post(
                url,
                data=soap_envelope.encode('utf-8'),
                timeout=self.timeout
            )
            
            self.logger.info(f"Response status code: {response.status_code}")
            
            if response.status_code != 200:
                self.logger.error(f"HTTP {response.status_code} error for {service_name}")
                self.logger.error(f"Response headers: {dict(response.headers)}")
                self.logger.error(f"Response body (first 1000 chars): {response.text[:1000]}")
            response.raise_for_status()
            
            parsed_response = self._parse_soap_response(response.text, service_name)
            self.logger.debug(f"Parsed response: {parsed_response}")
            return parsed_response
        except Exception as e:
            self.logger.error(f"Error in call_soap_service for {service_name}: {e}")
            self.logger.error(f"URL attempted: {url}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response headers: {dict(e.response.headers)}")
                self.logger.error(f"Response body (first 1000 chars): {e.response.text[:1000]}")
            raise

    def _parse_soap_response(
        self,
        xml_response: str,
        service_name: str
    ) -> Dict[str, Any]:
        """Parse SOAP XML response to dictionary.

        Args:
            xml_response: Raw XML response string
            service_name: Name of the service (for logging)

        Returns:
            Parsed response dictionary
        """
        try:
            self.logger.debug(f"Raw XML response (first 500 chars): {xml_response[:500]}")
            xml_dict = xmltodict.parse(xml_response)
            
            # Handle different SOAP namespaces
            soap_body = None
            for key in ["soap:Envelope", "soap12:Envelope", "Envelope"]:
                if key in xml_dict:
                    envelope = xml_dict[key]
                    for body_key in ["soap:Body", "soap12:Body", "Body"]:
                        if body_key in envelope:
                            soap_body = envelope[body_key]
                            break
                    if soap_body:
                        break

            if not soap_body:
                self.logger.warning(f"Could not find SOAP body in response for {service_name}")
                self.logger.warning(f"Available keys in parsed XML: {list(xml_dict.keys())}")
                return {"raw_response": xml_response}

            self.logger.debug(f"SOAP body keys: {list(soap_body.keys()) if isinstance(soap_body, dict) else 'Not a dict'}")

            # Find the response data dynamically
            response_data = None
            for key, value in soap_body.items():
                if isinstance(value, dict):
                    # Look for Result or ResponseValue
                    if "Result" in value:
                        response_data = value["Result"]
                        break
                    elif "ResponseValue" in value:
                        response_data = value["ResponseValue"]
                        break
                    # Some responses might have the data directly
                    elif key.endswith("Response") or "Response" in key:
                        response_data = value
                        break

            if response_data:
                self.logger.debug(f"Found response data: {response_data}")
                return response_data

            # Fallback: return the entire body
            self.logger.debug(f"Returning entire SOAP body as response")
            return soap_body if isinstance(soap_body, dict) else {"raw_response": xml_response}
        except Exception as e:
            self.logger.error(f"Failed to parse SOAP response for {service_name}: {e}")
            self.logger.error(f"XML response (first 1000 chars): {xml_response[:1000]}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {"raw_response": xml_response}
