--TEST--
time (DateInterval) and uuid columns round-trip identically
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_uuid;

$negative = (new DateTimeImmutable('2025-01-02'))->diff(new DateTimeImmutable('2025-01-01'));
$fractional = new DateInterval('PT1S');
$fractional->f = 0.123456;

$rows = rows(
    schema(
        int_schema('id'),
        time_schema('duration'),
        time_schema('negative'),
        time_schema('fractional'),
        uuid_schema('uuid'),
    ),
    row([
        'id' => 1,
        'duration' => new DateInterval('P3DT4H5M6S'),
        'negative' => $negative,
        'fractional' => $fractional,
        'uuid' => type_uuid()->cast('01234567-89ab-4def-8123-456789abcdef'),
    ]),
);

$frames = php_frames($rows);
$actual = ext_decode_frames($frames);

assert_rows_identical(php_decode_frames($frames), $actual);

$duration = $actual[0]->get('duration');
var_dump($duration->d, $duration->h, $duration->i, $duration->s);
var_dump($actual[0]->get('negative')->invert);
var_dump($actual[0]->get('fractional')->f);
var_dump((string) $actual[0]->get('uuid'));
?>
--EXPECT--
identical
int(3)
int(4)
int(5)
int(6)
int(1)
float(0.123456)
string(36) "01234567-89ab-4def-8123-456789abcdef"
