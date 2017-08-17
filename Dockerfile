# building
FROM golang

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# copying in source
COPY ./app ./src/$APP_PROJECT

# building the project
RUN go get ./src/$APP_PROJECT/... \
  && go install $APP_PROJECT

RUN realpath ./bin/app


# running it out
FROM alpine

# copying in built app
COPY --from=0 /go/bin/app .

CMD ["./app"]
