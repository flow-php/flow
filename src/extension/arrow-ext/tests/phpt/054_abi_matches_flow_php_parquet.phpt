--TEST--
arrow registers FLOW_ARROW_ABI equal to the ABI flow-php/parquet expects, so AdaptiveParquetEngine takes the Rust lane
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Engine\ArrowExtension;
use Flow\Parquet\Engine\RustParquetFileWriter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function Flow\Filesystem\DSL\path;

var_dump(FLOW_ARROW_ABI === ArrowExtension::ABI);
var_dump(ArrowExtension::detect()->available());

$writer = (new AdaptiveParquetEngine())->openForWrite(
    new StringDestinationStream(path('memory://out.parquet')),
    Schema::with(FlatColumn::int32('id')),
    Compressions::UNCOMPRESSED,
    new Options(),
);
var_dump($writer instanceof RustParquetFileWriter);
$writer->close();
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
