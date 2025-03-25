MongoDB Compatibility Proxy
A lightweight TCP proxy that intercepts MongoDB wire protocol messages and rewrites handshake commands to provide compatibility between MongoDB versions. In particular, it converts newer MongoDB handshake messages (using the "hello" command) into the legacy "isMaster" command format required by older versions.

Features
Protocol Translation:
Rewrites MongoDB handshake messages by replacing the "hello" field with "isMaster": 1, removes "helloOk", and adds required fields like topologyVersion and maxAwaitTimeMS for backward compatibility.

Per-Message Processing:
Processes each incoming MongoDB wire protocol message individually, ensuring proper handling of multiple messages on a persistent connection.

Production Ready:
Comes with a production variant with minimal logging and robust error handling.

Requirements
Python 3.6 or later

pymongo (for the BSON library)
Install via pip:

bash

pip install pymongo
Linux/Unix-based system is recommended for socket programming and logging.

Installation
Clone the Repository:

bash

git clone https://github.com/yourusername/mongo-compatibility-proxy.git
cd mongo-compatibility-proxy
(Optional) Create a Virtual Environment:

bash

python3 -m venv venv
source venv/bin/activate
Install Dependencies: Dependencies are minimal. If you need to install pymongo (which provides the BSON module), run:

bash

pip install pymongo
Usage
Start the Proxy:

Run the script:

bash

python3 mongo_proxy.py
By default, the proxy listens on 0.0.0.0:27018 and connects to a MongoDB instance on localhost:27017.

Configure PMM or Your Client:

Point your PMM (or another MongoDB client) to use 127.0.0.1:27018 as the MongoDB host. The proxy will intercept handshake messages and translate them as needed.

Configuration
Proxy Listen Port:
By default, the proxy listens on port 27018. To change this, update the call to start_proxy() in the script.

MongoDB Connection:
The proxy connects to MongoDB on localhost:27017. Modify the connect_with_retries() method in the script if your MongoDB instance is elsewhere.

Timeouts and Retries:
Timeout values and retry counts can be adjusted in the MongoProxy.__init__() method.

Logging
The production variant logs essential information and errors at the INFO level. Logs are written to /var/log/mongo_proxy.log (if permissions allow) and also output to the console. You can adjust the logging configuration in the script as needed.

Contributing
Contributions are welcome! Feel free to fork the repository, make improvements, and submit pull requests. Please follow standard GitHub contribution guidelines.

License
This project is released under the MIT License. See the LICENSE file for details.

