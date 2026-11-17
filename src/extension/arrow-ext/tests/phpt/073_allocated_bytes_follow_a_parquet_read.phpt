--TEST--
Flow\Arrow\allocated_bytes() grows while a batch read from a Parquet file is held and returns to its start value once the reader and the batch are released
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetFileReader;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

$start = \Flow\Arrow\allocated_bytes();
$file = new RustParquetFileReader(native_local_filesystem()->readFrom(path(__DIR__ . '/fixtures/arrow_schema_hints.parquet')));
$columns = $file->readColumns(['id', 'plain', 'binary'], 10, arrow_read_plan($file));
$columns->rewind();
$chunk = $columns->current();
$held = \Flow\Arrow\allocated_bytes();

unset($chunk, $columns, $file);

var_dump(is_int($start));
var_dump($held > $start);
var_dump(\Flow\Arrow\allocated_bytes() === $start);
--EXPECT--
bool(true)
bool(true)
bool(true)
