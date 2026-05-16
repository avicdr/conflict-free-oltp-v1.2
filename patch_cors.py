import sys

with open("src/bin/dashboard_server.rs", "r") as f:
    content = f.read()

content = content.replace(
    "        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])",
    "        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])"
)

with open("src/bin/dashboard_server.rs", "w") as f:
    f.write(content)

