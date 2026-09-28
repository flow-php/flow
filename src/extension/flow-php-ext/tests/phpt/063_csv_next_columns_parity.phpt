--TEST--
NativeCSVOpenSource::batches() yields the frames or refusals of PhpCSVOpenSource::batches(), through either backend
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\{CSVDecoder, CSVLineReader, NativeCSVOpenSource, PhpCSVOpenSource, RustCSVReaderNative};
use Flow\ETL\Column\{DefaultBackend, NativeColumn, PhpBackend};
use Flow\ETL\Schema;
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, float_schema, int_schema, json_schema, schema, str_schema, uuid_schema};
use function Flow\Filesystem\DSL\path;

$stream = static fn(string $raw): StringSourceStream => new StringSourceStream(path('memory://phpt.csv'), $raw);
$batches = static fn(object $open, Schema $schema, object $backend): string => outcome(static function () use ($open, $schema, $backend): array {
    $frames = [];

    foreach ($open->batches($schema, 2, $backend) as $batch) {
        foreach ($batch->columns() as $column) {
            if ($backend instanceof PhpBackend && $column instanceof NativeColumn) {
                return ['a NativeColumn survived PhpBackend'];
            }
        }

        $frames[] = bin2hex($batch->encodeFrame());
    }

    return $frames;
});
$cases = [];

foreach (csv_narrow_corpus() as $cell) {
    $raw = "v\n\"" . str_replace('"', '""', $cell) . "\"\n";

    foreach ([int_schema('v'), float_schema('v'), bool_schema('v'), datetime_schema('v'), date_schema('v'), uuid_schema('v'), json_schema('v'), str_schema('v', nullable: true)] as $definition) {
        $cases[] = [$raw, schema($definition)];
    }
}

$idName = schema(int_schema('id'), str_schema('name'));

foreach ([
    ["id\n1\n", $idName],
    ["id\n1\nx\n", $idName],
    ["a,b\nx,y\n", schema(int_schema('b'), int_schema('a'))],
    ["id,name\n1,a\n2,\n", $idName],
    ["id,name,name\n1,a,b\n2,c,d\n", $idName],
    ["id,name\n1\n2,b,extra\n", schema(int_schema('id'), str_schema('name', nullable: true))],
    ["1,2\na,3\n", schema(str_schema('1'), int_schema('2'))],
    ["id,name\n1,a\n2,b\n3,c\nx,d\n", $idName],
    ["id\n", schema(int_schema('id'), str_schema('absent'))],
] as $case) {
    $cases[] = $case;
}

$identical = 0;

foreach ($cases as [$raw, $schema]) {
    $php = $batches(new PhpCSVOpenSource($stream($raw), new CSVDecoder(), new CSVLineReader('"', ',', '\\')), $schema, new PhpBackend());

    foreach ([new DefaultBackend(), new PhpBackend()] as $backend) {
        $native = $batches(new NativeCSVOpenSource($stream($raw), new RustCSVReaderNative(',', '"', '\\', true, true, true)), $schema, $backend);

        if ($native === $php) {
            $identical++;
        } else {
            echo json_encode($raw), ' ', get_class($backend), "\n  php:    {$php}\n  native: {$native}\n";
        }
    }
}

echo "{$identical} of " . (2 * count($cases)) . " identical\n";
?>
--EXPECT--
6274 of 6274 identical
