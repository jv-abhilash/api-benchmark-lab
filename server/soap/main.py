"""
server/soap/main.py
SOAP service — port 8002
Duplex:    Half  |  Stateless: Optional  |  Auth: WS-Security header
Payload:   XML envelope — 3-10x larger than JSON (benchmark will show this)
"""
import time, os, sys
from collections import defaultdict
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from lxml import etree
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from server.shared.iot_feed import next_reading, reading_batch, PAD_SIZES, next_reading_padded, SENSOR_IDS

app = FastAPI(title="SOAP Sensor Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

NS_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"
NS_WSSEC    = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
NS_SERVICE  = "http://api-benchmark-lab/sensor"
NS_XSD      = "http://www.w3.org/2001/XMLSchema"

VALID_USER  = os.getenv("SOAP_USER", "benchmark")
VALID_PASS  = os.getenv("SOAP_PASS", "api-bench-secret")

_counters   = defaultdict(int)
_start_ts   = time.time()

def soap_envelope(body_content: etree._Element) -> bytes:
    env = etree.Element(f"{{{NS_ENVELOPE}}}Envelope",
                        nsmap={"soapenv": NS_ENVELOPE, "sen": NS_SERVICE})
    etree.SubElement(env, f"{{{NS_ENVELOPE}}}Header")
    body = etree.SubElement(env, f"{{{NS_ENVELOPE}}}Body")
    body.append(body_content)
    return etree.tostring(env, pretty_print=True,
                          xml_declaration=True, encoding="UTF-8")

def soap_fault(code: str, message: str) -> bytes:
    fault_el = etree.Element(f"{{{NS_ENVELOPE}}}Fault")
    etree.SubElement(fault_el, "faultcode").text  = code
    etree.SubElement(fault_el, "faultstring").text = message
    return soap_envelope(fault_el)

def reading_to_xml(r) -> etree._Element:
    el = etree.Element("reading")
    etree.SubElement(el, "sensor_id").text = r.sensor_id
    etree.SubElement(el, "temp").text      = str(r.temp)
    etree.SubElement(el, "humidity").text  = str(r.humidity)
    etree.SubElement(el, "timestamp").text = str(r.timestamp)
    etree.SubElement(el, "seq").text       = str(r.seq)
    etree.SubElement(el, "location").text  = r.location
    etree.SubElement(el, "status").text    = r.status
    return el

def verify_wssec(tree: etree._Element) -> bool:
    header   = tree.find(f"{{{NS_ENVELOPE}}}Header")
    if header is None: return False
    security = header.find(f"{{{NS_WSSEC}}}Security")
    if security is None: return False
    token    = security.find(f"{{{NS_WSSEC}}}UsernameToken")
    if token is None: return False
    username = token.findtext(f"{{{NS_WSSEC}}}Username", "")
    password = token.findtext(f"{{{NS_WSSEC}}}Password", "")
    return username == VALID_USER and password == VALID_PASS

# ── Fixed WSDL — xsd namespace added ─────────────────────────
WSDL = """<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
             xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/"
             xmlns:tns="http://api-benchmark-lab/sensor"
             xmlns:xsd="http://www.w3.org/2001/XMLSchema"
             targetNamespace="http://api-benchmark-lab/sensor"
             name="SensorService">

  <types>
    <xsd:schema targetNamespace="http://api-benchmark-lab/sensor">
      <xsd:complexType name="SensorReading">
        <xsd:sequence>
          <xsd:element name="sensor_id" type="xsd:string"/>
          <xsd:element name="temp"      type="xsd:float"/>
          <xsd:element name="humidity"  type="xsd:float"/>
          <xsd:element name="timestamp" type="xsd:double"/>
          <xsd:element name="seq"       type="xsd:int"/>
          <xsd:element name="location"  type="xsd:string"/>
          <xsd:element name="status"    type="xsd:string"/>
        </xsd:sequence>
      </xsd:complexType>
      <xsd:element name="GetLatestReadingRequest">
        <xsd:complexType>
          <xsd:sequence>
            <xsd:element name="sensor_id" type="xsd:string"/>
          </xsd:sequence>
        </xsd:complexType>
      </xsd:element>
      <xsd:element name="GetLatestReadingResponse">
        <xsd:complexType>
          <xsd:sequence>
            <xsd:element name="reading" type="tns:SensorReading"/>
          </xsd:sequence>
        </xsd:complexType>
      </xsd:element>
      <xsd:element name="GetAllReadingsRequest">
        <xsd:complexType><xsd:sequence/></xsd:complexType>
      </xsd:element>
      <xsd:element name="GetAllReadingsResponse">
        <xsd:complexType>
          <xsd:sequence>
            <xsd:element name="reading" type="tns:SensorReading"
                         minOccurs="0" maxOccurs="unbounded"/>
          </xsd:sequence>
        </xsd:complexType>
      </xsd:element>
    </xsd:schema>
  </types>

  <message name="GetLatestReadingInput">
    <part name="parameters" element="tns:GetLatestReadingRequest"/>
  </message>
  <message name="GetLatestReadingOutput">
    <part name="parameters" element="tns:GetLatestReadingResponse"/>
  </message>
  <message name="GetAllReadingsInput">
    <part name="parameters" element="tns:GetAllReadingsRequest"/>
  </message>
  <message name="GetAllReadingsOutput">
    <part name="parameters" element="tns:GetAllReadingsResponse"/>
  </message>

  <portType name="SensorPortType">
    <operation name="GetLatestReading">
      <input  message="tns:GetLatestReadingInput"/>
      <output message="tns:GetLatestReadingOutput"/>
    </operation>
    <operation name="GetAllReadings">
      <input  message="tns:GetAllReadingsInput"/>
      <output message="tns:GetAllReadingsOutput"/>
    </operation>
  </portType>

  <binding name="SensorBinding" type="tns:SensorPortType">
    <soap:binding style="document"
                  transport="http://schemas.xmlsoap.org/soap/http"/>
    <operation name="GetLatestReading">
      <soap:operation soapAction="GetLatestReading"/>
      <input><soap:body use="literal"/></input>
      <output><soap:body use="literal"/></output>
    </operation>
    <operation name="GetAllReadings">
      <soap:operation soapAction="GetAllReadings"/>
      <input><soap:body use="literal"/></input>
      <output><soap:body use="literal"/></output>
    </operation>
  </binding>

  <service name="SensorService">
    <port name="SensorPort" binding="tns:SensorBinding">
      <soap:address location="http://localhost:8002/soap"/>
    </port>
  </service>
</definitions>"""

@app.get("/health")
def health():
    return {"status": "ok", "service": "soap", "port": 8002}

@app.get("/soap")
def get_wsdl(wsdl: str = None):
    return Response(content=WSDL, media_type="text/xml")

@app.get("/credentials")
def get_credentials():
    return {"username": VALID_USER, "password": VALID_PASS}

@app.post("/soap")
async def soap_endpoint(request: Request):
    _counters["requests"] += 1
    body_bytes = await request.body()
    action     = request.headers.get("SOAPAction", "").strip('"')
    try:
        tree = etree.fromstring(body_bytes)
    except etree.XMLSyntaxError as e:
        return Response(content=soap_fault("Client", f"Invalid XML: {e}"),
                        media_type="text/xml", status_code=400)
    if not verify_wssec(tree):
        _counters["auth_failures"] += 1
        return Response(content=soap_fault("Client",
                        "WS-Security authentication failed"),
                        media_type="text/xml", status_code=401)
    if action == "GetLatestReading":
        body   = tree.find(f"{{{NS_ENVELOPE}}}Body")
        req_el = body.find(f"{{{NS_SERVICE}}}GetLatestReadingRequest") if body is not None else None
        sid    = req_el.findtext("sensor_id") if req_el is not None else None
        r      = next_reading(sid)
        resp   = etree.Element(f"{{{NS_SERVICE}}}GetLatestReadingResponse")
        resp.append(reading_to_xml(r))
        return Response(content=soap_envelope(resp), media_type="text/xml")
    elif action == "GetAllReadings":
        _counters["get_all"] += 1
        resp = etree.Element(f"{{{NS_SERVICE}}}GetAllReadingsResponse")
        for sid in SENSOR_IDS:
            resp.append(reading_to_xml(next_reading(sid)))
        return Response(content=soap_envelope(resp), media_type="text/xml")
    elif action == "GetBatchReadings":
        _counters["batch"] += 1
        resp  = etree.Element(f"{{{NS_SERVICE}}}GetBatchReadingsResponse")
        for r in reading_batch(size=100):
            resp.append(reading_to_xml(r))
        return Response(content=soap_envelope(resp), media_type="text/xml")
    elif action == "GetPaddedReading":
        body     = tree.find(f"{{{NS_ENVELOPE}}}Body")
        req_el   = body.find(f"{{{NS_SERVICE}}}GetPaddedReadingRequest") if body is not None else None
        size_lbl = req_el.findtext("size_label") if req_el is not None else "64B"
        pad      = PAD_SIZES.get(size_lbl, 0)
        rp       = next_reading_padded(pad_bytes=pad)
        resp     = etree.Element(f"{{{NS_SERVICE}}}GetPaddedReadingResponse")
        resp.append(reading_to_xml(rp.reading))
        etree.SubElement(resp, "pad_size_bytes").text = str(pad)
        return Response(content=soap_envelope(resp), media_type="text/xml")
    else:
        return Response(content=soap_fault("Client",
                        f"Unknown SOAPAction: '{action}'"),
                        media_type="text/xml", status_code=400)

@app.get("/metrics")
def get_metrics():
    uptime = round(time.time() - _start_ts, 2)
    return {
        "service"         : "soap",
        "uptime_seconds"  : uptime,
        "total_requests"  : _counters["requests"],
        "requests_per_sec": round(_counters["requests"] / max(uptime, 1), 2),
        "auth_failures"   : _counters["auth_failures"],
        "counters"        : dict(_counters),
    }
