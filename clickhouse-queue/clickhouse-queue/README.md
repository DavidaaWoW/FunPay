# Clickhouse Queue

Обслуживает очередь добавления данных в кликхаус. Есть своя внутренняя логика для работы с RecOne событиями.

# Requirements

* PHP 7.0
* RabbitMQ
* [Composer](https://getcomposer.org/)

```sh
apt install php7.0 php7.0-cgi php7.0-dev php7.0-bcmath php7.0-mbstring php7.0-zip php7.0-curl
```

## Download Composer
Run this in your terminal to get the latest Composer version:

```bash
php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');" && php composer-setup.php && php -r "unlink('composer-setup.php');" && mv composer.phar /usr/local/bin/composer
```

## Download GeoIP

```bash
mkdir -p /home/rails/geo_ip
wget http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz && tar -xvf GeoLite2-City.tar.gz && mv GeoLite2-City_*/GeoLite2-City.mmdb /home/rails/geo_ip/ && rm -Rf GeoLite2-*
wget http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz && tar -xvf GeoLite2-Country.tar.gz && mv GeoLite2-Country_*/GeoLite2-Country.mmdb /home/rails/geo_ip/ && rm -Rf GeoLite2-*
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