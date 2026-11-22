FROM node:20-slim

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY test_bullmq.mjs ./

CMD ["node", "test_bullmq.mjs"]
