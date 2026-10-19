--TEST--
arrow refuses a read plan ReadPlanConverter::toExtension() did not shape as (row count, skip) selectors, a limit and a row filter tree
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

foreach ([
    null,
    [],
    ['selectors' => 'all'],
    ['selectors' => [6]],
    ['selectors' => [[-1, false]]],
    ['selectors' => [[6, 'no']]],
    ['selectors' => [[6]]],
    ['selectors' => [[6, false]]],
    ['selectors' => [[6, false]], 'limit' => -1],
    ['selectors' => [[6, false]], 'limit' => null, 'filter' => ['leaf' => 'absent']],
    ['selectors' => [[6, false]], 'limit' => null, 'filter' => ['xor' => []]],
    ['selectors' => [[6, false]], 'limit' => null, 'filter' => ['leaf' => 'intervals', 'path' => ['id'], 'physical' => 'INT64', 'intervals' => [['1', true, null, false]]]],
] as $converted) {
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
{"selectors":[[6,false]]} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[[6,false]],"limit":-1} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed read plan
{"selectors":[[6,false]],"limit":null,"filter":{"leaf":"absent"}} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed row filter: missing key "is_null"
{"selectors":[[6,false]],"limit":null,"filter":{"xor":[]}} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed row filter: missing key "leaf"
{"selectors":[[6,false]],"limit":null,"filter":{"leaf":"intervals","path":["id"],"physical":"INT64","intervals":[["1",true,null,false]]}} => Flow\Parquet\Exception\RuntimeException: arrow received a malformed row filter: bounds of "id" are not Int64 values
