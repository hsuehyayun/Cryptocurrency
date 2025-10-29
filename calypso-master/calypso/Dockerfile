# Stage 1: Build the Node.js environment
FROM node:20-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy the rest of the source code
COPY . .

# Stage 2: Create the production image
FROM node:16-alpine

# Set a non-root user for security
USER node

# Set the working directory
WORKDIR /app

# Copy built files from the builder stage
COPY --from=builder /app .

# Expose the port that the bot will listen on (adjust if different)
EXPOSE 3000

# Command to run your bot
CMD ["node", "calypso.js"]
