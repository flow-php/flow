--TEST--
RustParquetOpenSource yields SkippedRows exactly as PhpParquetOpenSource for a matched-row offset inside a file, past every match, beside no match, and not at all without an offset
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
use Flow\ETL\Values\Rows;
use Flow\Parquet\Engine\{RustParquetFileReader, RustParquetFileWriter as ArrowRustParquetFileWriter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\{Option, Options, Reader};
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader\ReadPlan;

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Parquet\DSL\parquet_column;

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'));
$writer = new RustParquetFileWriter(
    new ArrowRustParquetFileWriter(
        $filesystem->writeTo(path('memory://ids.parquet')),
        ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquetSchema($schema)),
        Compressions::SNAPPY,
        OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
        1_000,
    ),
    new ParquetSchemaConformance((new SchemaConverter())->toParquetSchema($schema)),
    new PhpBackend(),
);

for ($group = 0; $group < 4; $group++) {
    $writer->write(array_to_rows(array_map(static fn(int $id): array => ['id' => $id], range($group * 100, $group * 100 + 99)), $schema));
}

$writer->close();

function yielded(iterable $batches): string
{
    $yielded = [];
    $ids = null;

    foreach ($batches as $batch) {
        if ($batch instanceof SkippedRows) {
            $yielded[] = 'skipped ' . $batch->count;

            continue;
        }

        $ids = [...$ids ?? [], ...array_column($batch->toArray(), 'id')];
    }

    if ($ids !== null) {
        $yielded[] = count($ids) . ' rows ' . ($ids === [] ? '' : $ids[0] . '..' . $ids[count($ids) - 1]) . ' ' . md5(json_encode($ids));
    }

    return $yielded === [] ? 'nothing' : implode(' | ', $yielded);
}

$cases = [
    'no offset' => [parquet_column('id')->greaterThanOrEqual(50), 0, null],
    'offset inside the file' => [parquet_column('id')->greaterThanOrEqual(50), 120, null],
    'offset past every match' => [parquet_column('id')->greaterThanOrEqual(50), 1_000, null],
    'offset beside no match' => [parquet_column('id')->greaterThan(10_000), 5, null],
    'offset beside a zero limit' => [parquet_column('id')->greaterThanOrEqual(50), 10, 0],
    'offset beside a limit' => [parquet_column('id')->greaterThanOrEqual(50), 10, 70],
];

foreach ($cases as $label => [$filter, $offset, $limit]) {
    $phpFile = Reader::php()->readStream($filesystem->readFrom(path('memory://ids.parquet')));
    $plan = ReadPlan::forFilter(
        $phpFile->reader()->footer(),
        $phpFile->indexes(),
        BoundFilter::bind($filter, $phpFile->schema(), new Options()),
        limit: $limit,
        offset: $offset,
    );
    $php = yielded((new PhpParquetOpenSource($phpFile))->batches($schema, 64, $plan, new PhpBackend()));
    $rust = yielded((new RustParquetOpenSource(new RustParquetFileReader($filesystem->readFrom(path('memory://ids.parquet')))))->batches($schema, 64, $plan, new RustBackend()));

    echo $label, ': ', $php === $rust ? $rust : "MISMATCH\n  php:  " . $php . "\n  rust: " . $rust, "\n";
}
?>
--EXPECT--
no offset: 350 rows 50..399 3077fc6da61d6d6cb6689c563ef87065
offset inside the file: skipped 120 | 230 rows 170..399 dea68633dc3cb28320a70b9bf10cd708
offset past every match: skipped 350
offset beside no match: skipped 0
offset beside a zero limit: nothing
offset beside a limit: skipped 10 | 70 rows 60..129 ae0e09fe87f1702dd23bfa8f68d6177f
