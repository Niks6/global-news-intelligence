# Global News Intelligence — Frontend
# Multi-stage build: Node.js build → Nginx serve

FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
# Build with the backend URL pointing to the Docker backend service
ENV VITE_API_BASE_URL=http://localhost:8000
RUN npm run build

# Production image with Nginx
FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
