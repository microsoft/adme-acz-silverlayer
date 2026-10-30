"""Local HTTPS fake of the ADME schema service backed by the public OSDU data-definitions schemas.

Schemas are downloaded from a pinned commit of the OSDU `data-definitions` project and bundled the way the ADME
schema service returns them: one document per kind with the referenced abstract schemas inlined under
`definitions`, keyed by their `osdu:wks:<Name>:<version>` kind.
"""

import copy
import datetime
import json
import ssl
import tempfile
import threading
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DATA_DEFINITIONS_COMMIT = "ff89d87caa43d05d1341dcaa6d258e1186ec8aa6"
RAW_URL = ("https://community.opengroup.org/osdu/data/data-definitions/-/raw/"
           f"{DATA_DEFINITIONS_COMMIT}/Generated/")
SCHEMA_PATH = "/api/schema-service/v1/schema"
ENTITY_FILES = {
    "osdu:wks:master-data--Well:1.0.0": "master-data/Well.1.0.0.json",
    "osdu:wks:master-data--Wellbore:1.0.0": "master-data/Wellbore.1.0.0.json",
}


def _download(relative: str, cache: Path | None) -> dict:
    target = cache / relative.replace("/", "__") if cache else None
    if target is not None and target.exists():
        return json.loads(target.read_text(encoding="utf-8"))
    with urllib.request.urlopen(RAW_URL + relative, timeout=60) as response:
        body = response.read()
    if target is not None:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    return json.loads(body)


def bundle_schema(relative: str, cache: Path | None = None) -> dict:
    """Return the schema with every referenced abstract schema inlined under `definitions`."""
    schema = _download(relative, cache)
    definitions: dict[str, dict] = {}

    def rewrite(node, base: str):
        if isinstance(node, list):
            for item in node:
                rewrite(item, base)
        elif isinstance(node, dict):
            reference = node.get("$ref")
            if isinstance(reference, str) and not reference.startswith("#"):
                target = str(Path(base, reference).resolve().relative_to(Path("/").resolve()))
                if target not in resolved:
                    document = _download(target, cache)
                    resolved[target] = document["x-osdu-schema-source"]
                    definitions[resolved[target]] = document
                    rewrite(document, str(Path("/", target).parent))
                node["$ref"] = f"#/definitions/{resolved[target]}"
            for value in node.values():
                rewrite(value, base)

    resolved: dict[str, str] = {}
    rewrite(schema, str(Path("/", relative).parent))
    for document in definitions.values():
        document.pop("definitions", None)
    schema["definitions"] = definitions
    return schema


def _self_signed_certificate(directory: Path) -> tuple[Path, Path]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    import ipaddress

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([
            x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
        ]), critical=False)
        .sign(key, hashes.SHA256())
    )
    certificate_path = directory / "fake-adme.pem"
    key_path = directory / "fake-adme.key"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    return certificate_path, key_path


class FakeAdme:
    """HTTPS server exposing the schema service endpoints the notebook calls."""

    def __init__(self, partition: str, token: str, cache: Path | None = None):
        self.partition = partition
        self.token = token
        self.requests: list[dict] = []
        self.schemas = {kind: bundle_schema(path, cache) for kind, path in ENTITY_FILES.items()}
        self._directory = tempfile.TemporaryDirectory(prefix="fake-adme-")
        self.certificate_path, key_path = _self_signed_certificate(Path(self._directory.name))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.certificate_path, key_path)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        return f"https://127.0.0.1:{self._server.server_address[1]}"

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
        self._directory.cleanup()

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status: int, payload):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                fake.requests.append({"path": parsed.path, "query": parsed.query, "headers": dict(self.headers)})
                if self.headers.get("Authorization") != f"Bearer {fake.token}":
                    return self._reply(401, {"code": 401, "reason": "Unauthorized"})
                if self.headers.get("data-partition-id") != fake.partition:
                    return self._reply(400, {"code": 400, "reason": "Missing or wrong data-partition-id"})
                if parsed.path == SCHEMA_PATH:
                    infos = [{"schemaIdentity": {"id": kind}} for kind in fake.schemas]
                    return self._reply(200, {"schemaInfos": infos})
                kind = urllib.parse.unquote(parsed.path.removeprefix(SCHEMA_PATH + "/"))
                if parsed.path.startswith(SCHEMA_PATH + "/") and kind in fake.schemas:
                    return self._reply(200, copy.deepcopy(fake.schemas[kind]))
                return self._reply(404, {"code": 404, "reason": f"Schema {kind} not found"})

        return Handler
