# Use a Debian-based image with necessary tools pre-installed
FROM jgoerzen/debian-base-standard:bookworm

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    container=docker \
    LEIN_ROOT=true

STOPSIGNAL SIGRTMIN+3

# Set the GITHUB_TOKEN as a build argument
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

# Optional branch or tag to clone (defaults to 'master')
ARG MONOREPO_BRANCH=master
ENV MONOREPO_BRANCH=${MONOREPO_BRANCH}

# Install packages in smaller groups to manage disk space better
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        curl \
        git \
        wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Install development tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        dos2unix \
        htop \
        iputils-ping \
        pssh \
        screen \
        maven \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Install Java and related packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libjna-java \
        openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Install larger packages separately
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        vim-tiny \
        emacs-nox \
        gnuplot \
        graphviz \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Install Leiningen
RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

# Copy and setup Jepsen

COPY jepsen/jepsen /jepsen/jepsen/
RUN if [ -f /jepsen/jepsen/project.clj ]; then \
      cd /jepsen/jepsen && \
      lein install && \
      lein deps; \
    fi
COPY jepsen /jepsen/

# Build Hazelcast Mono. Jenkins passes its own checkout as the "mono" build
# context. Without it (local bin/up), the context is empty and mono is cloned.
# Writes to the bind mount are discarded; only ~/.m2 ends up in the image.
WORKDIR /app
RUN --mount=type=bind,from=mono,target=/src/mono,rw \
    set -e; \
    if [ -f /src/mono/pom.xml ]; then \
      echo "Building hazelcast-mono from the build context"; \
      cd /src/mono; \
    else \
      echo "Cloning hazelcast-mono branch ${MONOREPO_BRANCH}"; \
      git clone --depth 1 --branch "${MONOREPO_BRANCH}" https://x-access-token:${GITHUB_TOKEN}@github.com/hazelcast/hazelcast-mono.git /app/hazelcast-mono; \
      cd /app/hazelcast-mono; \
    fi; \
    ./mvnw clean install -DskipTests -Dcheckstyle.skip -Pquick -q -pl '!hazelcast-enterprise-extensions/hazelcast-enterprise-cdc-db2-tests'; \
    ./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout > /etc/hz-version; \
    rm -rf /app/hazelcast-mono

# Setup initialization scripts
COPY ./bashrc /root/.bashrc
COPY ./init.sh /init.sh
RUN dos2unix /init.sh /root/.bashrc && \
    chmod +x /init.sh

CMD ["/init.sh"]