--TEST--
RustParquetOpenSource fills every batch to the batch size across row groups exactly as PhpParquetOpenSource: batches smaller than a group, larger than a group, larger than the file, a non-divisor, and behind a matched-row offset
--SKIPIF--
<?php if (!extension_loaded("flow_php") || !extension_loaded("arrow")) die("skip flow_php and arrow extensions not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\Parquet\IO\Extractor\{PhpParquetOpenSource, RustParquetOpenSource};
use Flow\ETL\Adapter\Parquet\IO\Loader\{ParquetSchemaConformance, RustParquetFileWriter};
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\IO\Extractor\File\SkippedRows;
use Flow\ETL\Values\Column\{PhpBackend, RustBackend};
use Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter as ArrowRustParquetFileWriter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\{Option, Options, Reader};
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader\ReadPlan;

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema, str_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Parquet\DSL\parquet_column;

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'), str_schema('name', nullable: true));
$writer = new RustParquetFileWriter(
    new ArrowRustParquetFileWriter(
        $filesystem->writeTo(path('memory://groups.parquet')),
        ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquetSchema($schema)),
        Compressions::SNAPPY,
        OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
        1_000,
    ),
    new ParquetSchemaConformance((new SchemaConverter())->toParquetSchema($schema)),
    new PhpBackend(),
);
$next = 0;

foreach ([100, 37, 250, 1, 100] as $groupRows) {
    $writer->write(array_to_rows(array_map(
        static fn(int $id): array => ['id' => $id, 'name' => $id % 3 === 0 ? null : 'n' . $id],
        range($next, $next + $groupRows - 1),
    ), $schema));
    $next += $groupRows;
}

$writer->close();

function batches(iterable $batches): string
{
    $sizes = [];
    $rows = [];

    foreach ($batches as $batch) {
        if ($batch instanceof SkippedRows) {
            $sizes[] = 'skipped ' . $batch->count;

            continue;
        }

        $sizes[] = $batch->count();
        $rows = [...$rows, ...$batch->toArray()];
    }

    return implode(',', $sizes) . ' ' . md5(json_encode($rows));
}

$cases = [
    'batch 30' => [30, null, 0, null],
    'batch 64' => [64, null, 0, null],
    'batch 100' => [100, null, 0, null],
    'batch 300' => [300, null, 0, null],
    'batch 1000' => [1000, null, 0, null],
    'batch 1' => [1, null, 0, null],
    'batch 64 behind an offset with a limit' => [64, parquet_column('id')->greaterThanOrEqual(20), 90, 200],
];

foreach ($cases as $label => [$batchSize, $filter, $offset, $limit]) {
    $phpFile = Reader::php()->readStream($filesystem->readFrom(path('memory://groups.parquet')));
    $plan = $filter === null
        ? ReadPlan::forFilter($phpFile->reader()->footer(), $phpFile->indexes(), null)
        : ReadPlan::forFilter($phpFile->reader()->footer(), $phpFile->indexes(), BoundFilter::bind($filter, $phpFile->schema(), new Options()), limit: $limit, offset: $offset);
    $php = batches((new PhpParquetOpenSource($phpFile))->batches($schema, $batchSize, $plan, new PhpBackend()));
    $rust = batches((new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path('memory://groups.parquet')))))->batches($schema, $batchSize, $plan, new RustBackend()));
    $sizes = substr($rust, 0, (int) strrpos($rust, ' '));

    echo $label, ': ', $php === $rust ? (strlen($sizes) > 60 ? substr($sizes, 0, 40) . '... (' . count(explode(',', $sizes)) . ' batches)' : $sizes) : "MISMATCH\n  php:  " . $php . "\n  rust: " . $rust, "\n";
}
?>
--EXPECT--
batch 30: 30,30,30,30,30,30,30,30,30,30,30,30,30,30,30,30,8
batch 64: 64,64,64,64,64,64,64,40
batch 100: 100,100,100,100,88
batch 300: 300,188
batch 1000: 488
batch 1: 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,... (488 batches)
batch 64 behind an offset with a limit: skipped 90,64,64,64,8
