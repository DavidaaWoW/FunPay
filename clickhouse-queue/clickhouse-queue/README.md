# Clickhouse Queue

Обслуживает очередь добавления данных в кликхаус.

# Requirements

* PHP 8.0
* RabbitMQ
* [Composer](https://getcomposer.org/)

```sh
apt install php8.0 php8.0-cgi php8.0-dev php8.0-bcmath php8.0-mbstring php8.0-curl php8.0-pgsql php8.0-xml php8.0-gmp php-pear
```

## Install with lib Uv
```bash
apt install libuv1-dev
ln -s /etc/php/8.0/mods-available/uv.ini /etc/php/8.0/cli/conf.d/
echo 'extension=uv.so' > /etc/php/8.0/mods-available/uv.ini
git clone https://github.com/bwoebi/php-uv.git && cd php-uv && phpize && ./configure && make && make install && cd .. && rm -rf php-uv && php -m |grep uv
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
composer install
```

## Deploy

```sh
./deploy.php deploy
```

## Run development

```sh
./bin/run start -v debug
```