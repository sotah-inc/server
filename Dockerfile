FROM golang

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# copying in source
COPY ./app ./src/$APP_PROJECT

# building the project
RUN go get ./src/$APP_PROJECT/... \
  && go get -t ./src/$APP_PROJECT
  && go install $APP_PROJECT

# running it out
CMD ["./bin/app"]
