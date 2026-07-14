FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET
RUN go build -o /out/app ./${TARGET}

FROM alpine:3.20
COPY --from=builder /out/app /app
ENTRYPOINT ["/app"]
