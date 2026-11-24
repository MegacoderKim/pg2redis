FROM node:20-slim

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY test_pubsub.mjs test_security.mjs test_tls.mjs ./

CMD ["node", "test_pubsub.mjs"]
