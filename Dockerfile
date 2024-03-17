FROM php:8.1-cli

RUN apt-get -y update && apt-get install -y openssl libevent-dev libicu-dev libzip-dev libxml2-dev libgmp-dev libpq-dev libmagickwand-dev git

RUN curl -sS https://getcomposer.org/installer | php && chmod +x composer.phar && mv composer.phar /usr/local/bin/composer


COPY . /opt
COPY composer.json /opt/composer.json
COPY composer.lock /opt/composer.lock
