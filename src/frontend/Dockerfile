FROM node:alpine

WORKDIR /app

RUN apk update && apk upgrade && \
    apk add git

RUN git clone https://github.com/mard4/uwm-dashboard.git && \
    npm install -g @angular/cli

WORKDIR /app/uwm-dashboard

RUN npm install

EXPOSE 4200

CMD [ "ng", "serve", "--host", "0.0.0.0" ]