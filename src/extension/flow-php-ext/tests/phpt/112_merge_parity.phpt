--TEST--
KWayMerge merges sorted streams identically on RustBackend and PhpBackend: ties across streams by stream then position, mixed directions, nulls, empty streams, a merge batch smaller than the blocks
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Kernels\Sort\Merge\KWayMerge;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\{Backend, PhpBackend, RustBackend};
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\ETL\Values\SortOrder;

use function Flow\ETL\DSL\{array_to_rows, float_schema, int_schema, schema, str_schema};

function merged(Backend $backend, array $streams, array $columns, int $streamBatch, int $mergeBatch): string
{
    $schema = schema(int_schema('seq'), str_schema('g'), float_schema('s', nullable: true));
    $generators = array_map(static function (array $rows) use ($backend, $schema, $columns, $streamBatch): Generator {
        $sorted = $backend->sort(array_to_rows($rows, $schema, $backend), $columns);

        foreach ($sorted->chunks($streamBatch) as $chunk) {
            yield $chunk;
        }
    }, $streams);
    $sequence = [];
    $sizes = [];

    foreach ((new KWayMerge($columns, $backend, new BatchSize($mergeBatch)))->merge($generators) as $batch) {
        $sizes[] = $batch->count();
        $sequence = [...$sequence, ...$batch->reduceToArray('seq')];
    }

    return implode(',', $sizes) . ' | ' . md5(implode(',', $sequence)) . ' ' . count($sequence);
}

$seq = 0;
$streams = [];
foreach ([0, 1, 2, 3] as $stream) {
    $rows = [];
    for ($i = 0; $i < 97 + $stream * 31; $i++) {
        $rows[] = ['seq' => $seq++, 'g' => ['a', 'b', 'c'][($i + $stream) % 3], 's' => $i % 7 === 0 ? null : (float) ($i % 5)];
    }
    $streams[] = $rows;
}
$streams[] = [];

$cases = [
    'one key ascending, heavy ties' => [[new SortColumn('g')], 16, 50],
    'one key descending, heavy ties' => [[new SortColumn('g', SortOrder::DESC)], 16, 50],
    'two keys mixed directions with nulls' => [[new SortColumn('g'), new SortColumn('s', SortOrder::DESC)], 13, 7],
    'float key nulls last' => [[new SortColumn('s', SortOrder::ASC, NullsOrder::LAST)], 40, 1000],
];

foreach ($cases as $label => [$columns, $streamBatch, $mergeBatch]) {
    $php = merged(new PhpBackend(), $streams, $columns, $streamBatch, $mergeBatch);
    $rust = merged(new RustBackend(), $streams, $columns, $streamBatch, $mergeBatch);
    $summary = explode(' | ', $rust);

    echo $label, ': ', $php === $rust ? (strlen($summary[0]) > 40 ? substr($summary[0], 0, 40) . '...' : $summary[0]) . ' rows ' . explode(' ', $summary[1])[1] : "MISMATCH\n  php:  " . $php . "\n  rust: " . $rust, "\n";
}
?>
--EXPECT--
one key ascending, heavy ties: 50,50,50,50,50,50,50,50,50,50,50,24 rows 574
one key descending, heavy ties: 50,50,50,50,50,50,50,50,50,50,50,24 rows 574
two keys mixed directions with nulls: 7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,... rows 574
float key nulls last: 574 rows 574
