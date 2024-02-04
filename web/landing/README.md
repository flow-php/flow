# Flow PHP - Web

## Prerequisite
To run web page locally, you need to have Symfony CLI installed locally.
Instruction on how to install it can be found here: [https://symfony.com/download](https://symfony.com/download).

## Setup

```shell
composer install
symfony server:ca:install
symfony server:start -d
```

To use [https://flow-php.wip](https://flow-php.wip) you need to [configure local proxy](https://symfony.com/doc/current/setup/symfony_server.html#setting-up-the-local-proxy).
Otherwise, you can use [https://127.0.0.1:800X](https://127.0.0.1:8000);