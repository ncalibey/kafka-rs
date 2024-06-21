_ensureCargo:
    #!/usr/bin/env bash
    if [ -z `which cargo` ]; then
        echo "Please install the Rust compiler toolchain to proceed: https://rustup.rs/"
        exit 1
    fi

localClusterSetup:
    #!/usr/bin/env bash
    set -Eeou pipefail
    docker compose up -d broker

localClusterTeardown:
    #!/usr/bin/env bash
    set -Eeou pipefail
    docker compose down