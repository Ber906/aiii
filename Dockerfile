# Power Agent: works on Render, Railway, Fly.io, Koyeb, Cloud Run, Hugging Face Spaces, any VPS with Docker.
FROM node:22-bookworm-slim

# git: only needed when FreeLLMAPI is used. python3: for the run_python tool. The agent can apt-install more itself (it runs as root).
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl python3 \
    && rm -rf /var/lib/apt/lists/*

# Optional headless browser for the screenshot tool (+400 MB):  docker build --build-arg WITH_CHROMIUM=1 .
ARG WITH_CHROMIUM=0
RUN if [ "$WITH_CHROMIUM" = "1" ]; then \
      apt-get update && apt-get install -y --no-install-recommends chromium fonts-liberation && rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /app
COPY package.json ai.js ./

# /data = mount your platform's persistent volume here, otherwise chats and files vanish on redeploy.
RUN mkdir -p /data
ENV POWER_AGENT_HOME=/data \
    AGENT_HOST=0.0.0.0 \
    PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8080)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "ai.js"]
