# building
FROM golang:1.11

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# working dir
WORKDIR /srv/app

# copying in deps
COPY ./app/go.mod .
COPY ./app/go.sum .
RUN go mod download

# copying in source
COPY ./app/cmd ./cmd
COPY ./app/pkg ./pkg

# building the project
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -a -installsuffix cgo -o /go/bin/app ./cmd/app
