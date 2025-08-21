FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o consumer main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS calls and tzdata for timezone support
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/consumer .

# Command to run
CMD ["./consumer"]
