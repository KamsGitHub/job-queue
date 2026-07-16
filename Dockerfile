# ---- Build stage ----
# Contains TypeScript, ESLint, dev dependencies — everything needed to compile,
# but none of it ships in the final image.
FROM node:20-alpine AS builder
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- Production stage ----
# Fresh, minimal image: only runtime dependencies and compiled JS.
FROM node:20-alpine AS production
WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

# Don't run the process as root inside the container.
USER node

EXPOSE 3000
CMD ["node", "dist/server.js"]
