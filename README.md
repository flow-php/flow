# ETL Adapter: XML

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides memory safe XML support for ETL.

Following implementation are available:
- [JSON Machine](https://github.com/halaxa/json-machine)

## Entry - XMLEntry

```php 
<?php

use Flow\ETL\Row\Entry\XMLEntry;

$entry = XMLEntry::fromString('0', '<xml><name>node</name></xml>')
```