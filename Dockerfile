# Backend image: Node cluster server (primary clock + worker HTTP/SSE).
FROM node:24-alpine

WORKDIR /app

# Install only production deps first (better layer caching).
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# App source.
COPY src ./src
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

ENV NODE_ENV=production
EXPOSE 8080

# The entrypoint runs migrations (and seeds if the DB is empty), then starts
# whatever CMD is passed.
ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["node", "src/server.js"]
