--TEST--
A UINT_64 leaf crosses RustBatchReader → RustParquetOpenSource as a numeric_string column (arrow-ext hands it over as LargeBinary decimal text, the Bytes kind flow_php stores numeric_string in); DataFrame rows written through RustParquetOpenSink into an explicit UINT_64 schema take an integer column (negatives refused) and a numeric_string column (non-canonical text refused)
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
<?php extension_loaded('arrow') || die('skip arrow'); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\Parquet\{RustParquetOpenSink, SchemaConverter};
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Engine\Arrow\{OptionsConverter, SchemaConverter as ArrowSchemaConverter};
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Reader;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer;

use function Flow\ETL\DSL\{definition_from_type, int_schema, schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{type_equals, type_numeric_string, type_optional};

$parquet = Schema::with(new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)));
$filesystem = memory_filesystem();
$writer = new Writer(engine: new PhpParquetEngine());
$writer->openForStream($filesystem->writeTo(path('memory://uint64.parquet')), $parquet);
$writer->writeBatch([['u' => 0], ['u' => null], ['u' => '9223372036854775808'], ['u' => '18446744073709551615']]);
$writer->close();

$schema = (new SchemaConverter())->toFlow($parquet);
var_dump(type_equals(type_numeric_string(), $schema->get('u')->type()));

foreach (rust_parquet_batches($filesystem->readFrom(path('memory://uint64.parquet')), $schema, 10) as $batch) {
    var_dump(type_equals(type_numeric_string(), $batch->column('u')->type()) || type_equals(type_optional(type_numeric_string()), $batch->column('u')->type()));
    var_dump($batch->column('u')->values());
}

$write = static function (Flow\ETL\Schema $schema, array $values) use ($parquet, $filesystem): string {
    return outcome(static function () use ($schema, $values, $parquet, $filesystem): array {
        $sink = new RustParquetOpenSink(new RustParquetFileWriter(
            $filesystem->writeTo(path('memory://uint64_write.parquet')),
            ArrowSchemaConverter::toExtension($parquet),
            Compressions::SNAPPY,
            OptionsConverter::toExtension(Options::default()),
            1_000,
        ));

        try {
            $sink->write(php_rows($schema, $values));
        } finally {
            $sink->close();
        }

        return iterator_to_array(Reader::php()->readStream($filesystem->readFrom(path('memory://uint64_write.parquet')))->values());
    });
};

echo $write(schema(int_schema('u', nullable: true)), [['u' => 0], ['u' => null], ['u' => PHP_INT_MAX]]), "\n";
echo $write(schema(int_schema('u', nullable: true)), [['u' => 1], ['u' => -1]]), "\n";
echo $write(schema(definition_from_type('u', type_numeric_string(), true)), [['u' => '18446744073709551615'], ['u' => null], ['u' => '0']]), "\n";
echo $write(schema(definition_from_type('u', type_numeric_string(), true)), [['u' => '1'], ['u' => '007']]), "\n";
?>
--EXPECT--
bool(true)
bool(true)
array(4) {
  [0]=>
  string(1) "0"
  [1]=>
  NULL
  [2]=>
  string(19) "9223372036854775808"
  [3]=>
  string(20) "18446744073709551615"
}
a:3:{i:0;a:1:{s:1:"u";s:1:"0";}i:1;a:1:{s:1:"u";N;}i:2;a:1:{s:1:"u";s:19:"9223372036854775807";}}
Flow\Parquet\Exception\RuntimeException: Parquet column "u" row 1 holds a value out of the range arrow stores it in
a:3:{i:0;a:1:{s:1:"u";s:20:"18446744073709551615";}i:1;a:1:{s:1:"u";N;}i:2;a:1:{s:1:"u";s:1:"0";}}
Flow\Parquet\Exception\ValidationException: Column "u" row 1: expected int >= 0 or digit string, got string
