FROM node:10-alpine

RUN apk --no-cache add python2 git make g++

RUN mkdir /app
WORKDIR /app

COPY ./package.json /app
COPY ./package-lock.json /app
RUN npm install

COPY . /app

VOLUME [ "/app/operator-keystore", "/app/operator-db" ]

EXPOSE 3000

ENTRYPOINT [ "node", "bin/plasma-chain.js" ]