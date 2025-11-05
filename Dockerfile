# build
FROM golang:1.20-alpine AS build
WORKDIR /app
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/campaign

# run
FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=build /app/campaign /campaign
EXPOSE 8080
ENTRYPOINT ["/campaign"]
