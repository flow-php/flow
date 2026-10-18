--TEST--
arrow refuses a read plan ReadPlanConverter::toExtension() did not shape as (row count, skip) selectors
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
namespace Flow\Parquet\Engine\Arrow;

// declared before the autoloader sees it: stands in for the lib's converter
final class ReadPlanConverter
{
    public static mixed $converted = null;

    public static function toExtension(mixed $plan): mixed
    {
        return self::$converted;
    }
}

namespace Test;

require __DIR__ . '/bootstrap.php';

use Flow\Arrow\Parquet\RustColumnsReader;
use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Engine\Arrow\ReadPlanConverter;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

$file = new RustParquetFileReader(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));

foreach ([null, [], ['selectors' => 'all'], ['selectors' => [6]], ['selectors' => [[-1, false]]], ['selectors' => [[6, 'no']]], ['selectors' => [[6]]]] as $converted) {
    ReadPlanConverter::$converted = $converted;
    echo json_encode($converted), ' => ', arrow_outcome(static fn() => new RustColumnsReader($file, ['id'], 10, ReadPlan::window(6, new InMemoryIndexes()))), "\n";
}
?>
--EXPECT--
null => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
[] => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":"all"} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[6]} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[[-1,false]]} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[[6,"no"]]} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[[6]]} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
