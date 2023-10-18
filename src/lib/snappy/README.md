# Snappy

Pure PHP implementation of Google [Snappy](https://github.com/google/snappy) compression algorithm.

This library is a port of javascript [snappyjs](https://github.com/zhipeng-jia/snappyjs).
Whenever it's possible it's recommended to install [PHP Extension](https://github.com/kjdev/php-ext-snappy),
otherwise this lib will register polyfill functions. 

## Installation

```
composer require flow-php/snappy:1.x@dev
```

## Usage

```php
<?php

$string = 'This is some random string';

echo \snappy_decompress(\snappy_compress($string)); // This is some random string
```

## Performance

PHP Implementation is significantly slower than extension, below you can find a benchmark script and results:

```php
<?php

include __DIR__ . '/../vendor/autoload.php';

$faker = \Faker\Factory::create();

$texts = [];
for ($i = 0; $i < 10_000; $i++) {
    $textSize = \random_int(100, 5000);
    $texts[] = $faker->text($textSize);
}

$snappy = new \Flow\Snappy\Snappy();

echo "Starting Benchmark\n\n";

$flowStart = microtime(true);
foreach ($texts as $text) {
    if ($snappy->uncompress($snappy->compress($text)) !== $text) {
        die('snappy flow failed');
    }
}
$flowEnd = microtime(true);
echo "Snappy Flow time: " . ($flowEnd - $flowStart) . "\n";

$extStart = microtime(true);
foreach ($texts as $text) {
    if (\snappy_uncompress(\snappy_compress($text)) !== $text) {
        die('snappy ext failed');
    }
}
$extEnd = microtime(true);
echo "Snappy PHP Extension time: " . ($extEnd - $extStart) . "\n";
```

Output: 

```console
$ php benchmark_snappy.php
Starting Benchmark

Snappy Flow time: 6.6838178634644
Snappy PHP Extension time: 0.31190991401672
```