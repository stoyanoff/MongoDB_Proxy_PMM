#!/usr/bin/env python3
import socket
import struct
import logging
import time
import threading
import json
import uuid
from bson import BSON, ObjectId, Int64
from bson.binary import Binary, STANDARD

# ----------------- Helper Functions -----------------

class BSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Int64):
            return int(o)
        if isinstance(o, Binary):
            return o.hex()  # represent binary data as hex string
        return super().default(o)

def decode_bson_to_json(raw_bson: bytes) -> str:
    """
    Decodes raw BSON data to a pretty-printed JSON string.
    """
    try:
        doc = BSON(raw_bson).decode()
        return json.dumps(doc, indent=2, cls=BSONEncoder)
    except Exception as e:
        return f"Error decoding BSON: {e}"

# ----------------- MongoProxy Class -----------------

class MongoProxy:
    def __init__(self):
        # Timeout settings optimized for PMM (adjust if needed)
        self.timeout = 3.0  # seconds
        self.max_retries = 2
        self.connection_timeout = 1.0  # seconds
        self.buffer_size = 8192  # Standard buffer size

    def log_original_request(self, data):
        """Decode and log the original BSON request as JSON."""
        try:
            json_str = decode_bson_to_json(data)
            logger.debug("Full original message (JSON): " + json_str)
            logger.info("Original PMM request: " + json_str)
        except Exception as e:
            logger.error(f"Error decoding original request: {e}")

    def connect_with_retries(self):
        """Establish connection to MongoDB with retries."""
        for attempt in range(self.max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connection_timeout)
                sock.connect(("localhost", 27017))
                sock.settimeout(self.timeout)
                return sock
            except socket.timeout as e:
                logger.error(f"Connection attempt {attempt+1} timed out: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(0.1 * (attempt + 1))
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                raise

    def recv_all(self, sock, n):
        """Receive exactly n bytes from a socket."""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                break
            data += chunk
        return data

    def process_message(self, client_sock, mongo_sock, header):
        """
        Process a single MongoDB protocol message.
        For op_codes 2004 (OP_QUERY) and 2013 (OP_MSG), extract the BSON document,
        decode it for logging, and if it contains a "hello" field, rewrite it by
        replacing "hello" with "isMaster": 1 as the first key, removing "helloOk",
        and adding extra handshake fields.
        The re-encoded document is logged (both as hex and as pretty-printed JSON).
        """
        try:
            if len(header) < 16:
                raise ValueError("Incomplete header received")
            msg_length, request_id, response_to, op_code = struct.unpack("<iiii", header)
            remaining = msg_length - 16

            # Read full body from client
            body = self.recv_all(client_sock, remaining)
            logger.debug("Raw message header: " + header.hex())
            logger.debug("Raw message body: " + body.hex())
            full_message = header + body
            logger.debug("Full raw message: " + full_message.hex())
            if len(body) != remaining:
                raise ValueError("Incomplete message received")
            logger.debug(f"Received message of length {msg_length} with op_code {op_code}")

            new_header = header
            new_body = body

            if op_code in (2004, 2013):
                # Determine where the BSON document starts.
                # For OP_QUERY (2004): [4 bytes flags][collection name (null-terminated)][4 bytes skip][4 bytes return][BSON doc]
                # For OP_MSG (2013): if section kind == 0, then [4 bytes flags][1 byte section kind][BSON doc]
                flags = body[:4]
                query_start = 4  # default start if nothing else is found
                if op_code == 2004:
                    collection_end = body.find(b'\x00', 4)
                    if collection_end != -1:
                        query_start = collection_end + 1 + 4 + 4
                    else:
                        logger.debug("OP_QUERY: Collection name not found; using default query start")
                elif op_code == 2013:
                    if len(body) >= 5:
                        section_kind = body[4]
                        if section_kind == 0:
                            query_start = 5
                        else:
                            logger.debug(f"OP_MSG: Section kind {section_kind} not handled; using default query start")
                    else:
                        logger.debug("OP_MSG: Body too short; using default query start")
                query_doc = body[query_start:]
                decoded_json = decode_bson_to_json(query_doc)
                logger.debug("Decoded query doc: " + decoded_json)

                try:
                    decoded = BSON(query_doc).decode()
                    if "hello" in decoded:
                        if "lsid" in decoded:
                            lsid = decoded["lsid"]
                            if isinstance(lsid, dict) and "id" in lsid:
                                old_id = lsid["id"]
                                if isinstance(old_id, Binary) and old_id.subtype != STANDARD:
                                    logger.debug("Fixing lsid.id from deprecated UUID format")
                                    lsid["id"] = Binary(old_id, subtype=STANDARD)
                        if "lsid" not in decoded:
                            decoded["lsid"] = {
                                "id": Binary(uuid.uuid4().bytes, subtype=STANDARD)
                            }
                            logger.debug("Injected compliant lsid.id with UUID subtype 4")
                        logger.debug("Found 'hello' field; rewriting document.")
                        # Remove the "hello" field and rebuild the document with "isMaster" as the first key.
                        decoded.pop("hello")
                        # Remove "helloOk" if present.
                        if "helloOk" in decoded:
                            logger.debug(f"'helloOk' found with value: {decoded['helloOk']}")
                            decoded.pop("helloOk")
                            logger.debug("Removed helloOk field")
                        # Create a new document starting with "isMaster": 1.
                        new_doc = {"isMaster": 1}
                        # For handshake messages, add topologyVersion and maxAwaitTimeMS if missing.
                        if "topologyVersion" not in decoded:
                            new_doc["topologyVersion"] = {
                                "processId": ObjectId("000000000000000000000000"),
                                "counter": Int64(0)
                            }
                            logger.debug("Added topologyVersion with ObjectId and Int64 counter")
                        if "maxAwaitTimeMS" not in decoded:
                            new_doc["maxAwaitTimeMS"] = 1000
                            logger.debug("Added maxAwaitTimeMS")
                        # Append the remaining keys in their original order.
                        for key, value in decoded.items():
                            new_doc[key] = value
                        decoded = new_doc
                        logger.debug("Modified document (JSON): " + json.dumps(decoded, indent=2, cls=BSONEncoder))
                        modified_doc = BSON.encode(decoded)
                        # Log the re-encoded BSON message.
                        logger.debug("Re-encoded BSON (hex): " + modified_doc.hex())
                        logger.debug("Re-encoded BSON (JSON): " + decode_bson_to_json(modified_doc))
                        new_body = body[:query_start] + modified_doc
                        new_msg_length = 16 + len(new_body)
                        new_header = struct.pack("<iiii", new_msg_length, request_id, response_to, op_code)
                        logger.debug(f"Rebuilt message with new length {new_msg_length}")
                    else:
                        logger.debug("No 'hello' field found; forwarding original message.")
                except Exception as e:
                    logger.error(f"Decoding error in process_message: {e}")
            else:
                logger.debug("Message op_code not eligible for rewriting; passing through.")

            mongo_sock.sendall(new_header + new_body)
            logger.debug("Forwarded message to MongoDB")

            # Read full response from MongoDB and forward to client.
            resp_header = self.recv_all(mongo_sock, 16)
            if len(resp_header) != 16:
                raise ValueError("Incomplete response header")
            resp_length = struct.unpack("<i", resp_header[:4])[0]
            resp_body = self.recv_all(mongo_sock, resp_length - 16)
            if len(resp_body) != resp_length - 16:
                raise ValueError("Incomplete response body")
            logger.debug(f"Received response of length {resp_length}")
            client_sock.sendall(resp_header + resp_body)
            logger.debug("Sent response back to client")
        except Exception as e:
            logger.error(f"Message processing failed: {e}")
            raise

    def handle_connection(self, client_sock, addr):
        """Handle a single client connection, processing every new message."""
        mongo_sock = None
        try:
            client_sock.settimeout(self.timeout)
            mongo_sock = self.connect_with_retries()
            logger.info(f"Connected to MongoDB for client {addr}")

            # Continuously process each new message on this connection.
            while True:
                header = self.recv_all(client_sock, 16)
                if not header or len(header) < 16:
                    logger.info(f"Client {addr} closed connection or sent incomplete header")
                    break
                self.process_message(client_sock, mongo_sock, header)
        except socket.timeout:
            logger.warning(f"Client {addr} operation timed out")
        except Exception as e:
            logger.error(f"Connection error with {addr}: {e}")
        finally:
            if mongo_sock:
                mongo_sock.close()
            client_sock.close()
            logger.info(f"Connection with {addr} closed")

# ----------------- Logging Setup -----------------

logger = logging.getLogger("MongoProxy")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# File handler for logging to file
try:
    file_handler = logging.FileHandler('/var/log/mongo_proxy.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
except Exception as e:
    print("Failed to set up file logging:", e)

# Stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ----------------- Start Proxy -----------------

def start_proxy(host="0.0.0.0", port=27018):
    proxy = MongoProxy()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(10)
        logger.info(f"MongoDB compatibility proxy started on {host}:{port}")

        try:
            while True:
                client_sock, addr = server_sock.accept()
                logger.info(f"New connection from {addr[0]}:{addr[1]}")
                thread = threading.Thread(target=proxy.handle_connection, args=(client_sock, addr), daemon=True)
                thread.start()
        except KeyboardInterrupt:
            logger.info("Proxy stopped by user")
        except Exception as e:
            logger.error(f"Server error: {e}")

if __name__ == "__main__":
    start_proxy()

