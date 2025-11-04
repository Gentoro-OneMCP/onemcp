# Product image built on the base image
ARG BASE_IMAGE=admingentoro/base:latest
FROM ${BASE_IMAGE}

ARG APP_JAR
ENV APP_JAR_PATH=/opt/app/onemcp.jar
ENV ACME_SERVER_FOUNDATION_DIR=/var/foundation
RUN mkdir -p /opt/app /var/foundation

# Copy app jar
COPY ${APP_JAR} ${APP_JAR_PATH}

# Build and copy TypeScript runtime
COPY src/typescript-runtime/package*.json /opt/ts-runtime/
WORKDIR /opt/ts-runtime
RUN npm install --production --legacy-peer-deps
COPY src/typescript-runtime/ /opt/ts-runtime/
RUN npm run build

# Copy mock server JAR
COPY src/acme-analytics-server/server/target/acme-analytics-server-1.0.0.jar /opt/mock-server/server.jar

# Copy startup scripts and default otel config
COPY scripts/docker/otel-collector-config.yaml /etc/otel-collector-config.yaml
COPY scripts/docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY scripts/docker/run-app.sh /opt/bin/run-app.sh
COPY scripts/docker/run-otel.sh /opt/bin/run-otel.sh
COPY scripts/docker/run-ts.sh /opt/bin/run-ts.sh
COPY scripts/docker/run-mock.sh /opt/bin/run-mock.sh
RUN chmod +x /opt/bin/*.sh

# Copy default foundation content (acme-analytics-server handbook)
COPY src/acme-analytics-server/onemcp-handbook/ ${ACME_SERVER_FOUNDATION_DIR}/
# Rename instructions.md to Agent.md `as required by the application
RUN mv ${ACME_SERVER_FOUNDATION_DIR}/instructions.md ${ACME_SERVER_FOUNDATION_DIR}/Agent.md
# Verify foundation content is copied correctly
RUN ls -la ${ACME_SERVER_FOUNDATION_DIR}/ && echo "Foundation content copied successfully"

EXPOSE 8080
ENTRYPOINT ["/opt/bin/entrypoint.sh"]
