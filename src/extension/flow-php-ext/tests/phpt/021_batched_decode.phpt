--TEST--
RowsDecoder::rows decodes a batch identically to per-body row() and the pure-PHP decoder
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\RowsDecoder;

use function Flow\ETL\DSL\{row, rows, int_entry, str_entry, float_entry};

$build = static fn(int $i): Flow\ETL\Row => row(
    int_entry('id', $i),
    str_entry('name', $i % 7 === 0 ? null : 'user_' . $i),
    float_entry('price', $i / 4),
);

$rows = rows(...array_map($build, range(1, 500)));
$frames = php_frames($rows);

$schemaBody = null;
$rowBodies = [];

foreach ($frames as $frame) {
    if ($frame['type'] === Format::FRAME_SCHEMA) {
        $schemaBody = $frame['body'];
    } else {
        $rowBodies[] = $frame['body'];
    }
}

var_dump(count($rowBodies));

$batchDecoder = new RowsDecoder();
$batchDecoder->schema($schemaBody);
$batched = $batchDecoder->rows($rowBodies);

var_dump($batched instanceof Rows);
var_dump($batched->count());

$singleDecoder = new RowsDecoder();
$singleDecoder->schema($schemaBody);
$perBody = [];

foreach ($rowBodies as $body) {
    $perBody[] = $singleDecoder->row($body);
}

$php = php_decode_frames($frames);
$batchedRows = $batched->all();

$identical = true;

foreach ($php as $i => $expected) {
    if (serialize($expected) !== serialize($batchedRows[$i])) {
        $identical = false;
        echo "FAIL: batched row {$i} differs\n";
    }

    if (serialize($expected) !== serialize($perBody[$i])) {
        $identical = false;
        echo "FAIL: per-body row {$i} differs\n";
    }
}

var_dump($identical);

$emptyDecoder = new RowsDecoder();
$emptyDecoder->schema($schemaBody);
$empty = $emptyDecoder->rows([]);
var_dump($empty instanceof Rows);
var_dump($empty->count());

$fresh = new RowsDecoder();
try {
    $fresh->rows($rowBodies);
    echo "FAIL: no exception\n";
} catch (Flow\Floe\Exception\ExtensionException $e) {
    echo $e->getMessage(), "\n";
}
?>
--EXPECT--
int(500)
bool(true)
int(500)
bool(true)
bool(true)
int(0)
flow_php found a row frame before any schema frame
