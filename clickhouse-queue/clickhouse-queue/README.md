# Clickhouse Queue

Обслуживает очередь добавления данных в кликхаус. Есть своя внутренняя логика для работы с RecOne событиями.

# Requirements

* PHP 7.0
* RabbitMQ
* [Composer](https://getcomposer.org/)

```sh
apt install php7.0 php7.0-cgi php7.0-dev php7.0-bcmath php7.0-mbstring php7.0-zip php7.0-curl
```

## Install

```sh
bundle install
composer install
```

## Deploy

```sh
cap production deploy
```

## Run development

```sh
./run start -v debug
```