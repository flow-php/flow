# Array Dot

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

Array Dot is a set of functions that allows to manipulate PHP arrays using custom **dot notation** known
from [JavaScript](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Property_accessors).
This implementation brings some custom syntax which will be explained bellow.

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
composer install --working-dir ./tools
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
