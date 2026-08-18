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

# Clone and build Hazelcast Mono repository
WORKDIR /app
RUN git clone --depth 1 --branch "${MONOREPO_BRANCH}" https://x-access-token:${GITHUB_TOKEN}@github.com/hazelcast/hazelcast-mono.git && \
    cd hazelcast-mono && \
    ./mvnw clean install -DskipTests -Dcheckstyle.skip -Pquick -q -pl '!hazelcast-enterprise-extensions/hazelcast-enterprise-cdc-db2-tests' && \
    cd .. && \
    rm -rf hazelcast-mono

# Setup initialization scripts
COPY ./bashrc /root/.bashrc
COPY ./init.sh /init.sh
RUN dos2unix /init.sh /root/.bashrc && \
    chmod +x /init.sh

CMD ["/init.sh"]