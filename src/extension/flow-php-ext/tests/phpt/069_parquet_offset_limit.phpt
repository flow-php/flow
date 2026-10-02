--TEST--
NativeParquetReader reads offset/limit windows of an 8-row-group file, and refuses a batch size, offset or limit out of range
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RowsWriter;

use Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter, SchemaConverter};
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\{Option, Options, Reader};

use function Flow\ETL\DSL\{array_to_rows, int_schema, schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};

$filesystem = memory_filesystem();
$schema = schema(int_schema('id'));
$writer = new NativeParquetWriter(new RowsWriter(
    $filesystem->writeTo(path('memory://groups.parquet')),
    ArrowSchemaConverter::toExtension((new SchemaConverter())->toParquet($schema)),
    'SNAPPY',
    OptionsConverter::toExtension(Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)),
    1_000,
));

for ($group = 0; $group < 8; $group++) {
    $writer->write(array_to_rows(array_map(static fn(int $id): array => ['id' => $id], range($group * 8192, $group * 8192 + 8191)), $schema));
}

$writer->close();

echo 'row groups: ', implode(',', array_map(
    static fn($rowGroup): int => $rowGroup->rowsCount(),
    (new Reader())->readStream($filesystem->readFrom(path('memory://groups.parquet')))->metadata()->rowGroups()->all(),
)), "\n";

foreach ([[0, 10], [5399, 3], [20000, 15000], [65000, 100], [65046, 1], [65536, 10], [65530, null]] as [$offset, $limit]) {
    $reader = native_parquet_reader($filesystem->readFrom(path('memory://groups.parquet')), $schema, 1000, $offset, $limit);
    $ids = [];

    while (($batch = $reader->next()) !== null) {
        array_push($ids, ...$batch->column('id')->values());
    }

    printf("%d %s: %d rows, %s\n", $offset, $limit ?? 'null', count($ids), $ids === [] ? '-' : $ids[0] . '..' . $ids[count($ids) - 1]);
    var_dump($ids === ($ids === [] ? [] : range($offset, $offset + count($ids) - 1)));
}

foreach ([[0, null, null], [-1, null, null], [10, -1, null], [10, null, -1]] as [$batchSize, $offset, $limit]) {
    echo outcome(static fn() => native_parquet_reader($filesystem->readFrom(path('memory://groups.parquet')), $schema, $batchSize, $offset, $limit)), "\n";
}
?>
--EXPECT--
row groups: 8192,8192,8192,8192,8192,8192,8192,8192
0 10: 10 rows, 0..9
bool(true)
5399 3: 3 rows, 5399..5401
bool(true)
20000 15000: 15000 rows, 20000..34999
bool(true)
65000 100: 100 rows, 65000..65099
bool(true)
65046 1: 1 rows, 65046..65046
bool(true)
65536 10: 0 rows, -
bool(true)
65530 null: 6 rows, 65530..65535
bool(true)
Flow\Parquet\Exception\InvalidArgumentException: arrow Parquet batch size must be greater than 0
Flow\Parquet\Exception\InvalidArgumentException: arrow Parquet batch size must be greater than 0
Flow\Parquet\Exception\InvalidArgumentException: arrow Parquet offset must be greater or equal to 0
Flow\Parquet\Exception\InvalidArgumentException: arrow Parquet limit must be greater or equal to 0
