# Clickhouse Queue

Обслуживает очередь добавления данных в кликхаус.

# Requirements

* PHP 8.1
* RabbitMQ
* [Composer](https://getcomposer.org/)

```sh
apt install php8.1 php8.1-cgi php8.1-dev php8.1-bcmath php8.1-mbstring php8.1-curl php8.1-pgsql php8.1-xml php8.1-gmp php-pear
```

## Download Composer
Run this in your terminal to get the latest Composer version:

```bash
php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');" && php composer-setup.php && php -r "unlink('composer-setup.php');" && mv composer.phar /usr/local/bin/composer
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

## Optional install async filesystem driver

```sh
echo '' | pecl install eio
ln -s /etc/php/8.1/mods-available/eio.ini /etc/php/8.1/cli/conf.d/
echo 'extension=eio.so' > /etc/php/8.1/mods-available/eio.ini
php -m |grep eio
```
