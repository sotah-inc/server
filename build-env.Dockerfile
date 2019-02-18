# building
FROM golang:1.11-alpine

# installing deps
RUN apk update \
  && apk upgrade \
  && apk add --no-cache bash git openssh

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
