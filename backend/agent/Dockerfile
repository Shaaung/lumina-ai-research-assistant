FROM node:20-bookworm-slim AS build

WORKDIR /app

# Copy only manifests and source required by this workspace. Secrets in the
# assignment-root .env never enter the image.
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/contract/package.json packages/contract/tsconfig.json ./packages/contract/
COPY backend/agent/package.json backend/agent/tsconfig.json ./backend/agent/
RUN npm ci --workspace @lumina/contract --workspace @lumina/agent --include-workspace-root

COPY packages/contract/src ./packages/contract/src
COPY backend/agent/src ./backend/agent/src
RUN npm run build -w @lumina/contract && npm run build -w @lumina/agent

FROM node:20-bookworm-slim AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages/contract/package.json ./packages/contract/package.json
COPY --from=build /app/packages/contract/dist ./packages/contract/dist
COPY --from=build /app/backend/agent/package.json ./backend/agent/package.json
COPY --from=build /app/backend/agent/dist ./backend/agent/dist

WORKDIR /app/backend/agent
CMD ["node", "dist/index.js"]
