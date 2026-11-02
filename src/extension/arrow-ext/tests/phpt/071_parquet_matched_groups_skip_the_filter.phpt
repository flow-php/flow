--TEST--
arrow skips the row filter on row groups a plan's matched rows fully cover and returns exactly the PHP engine's rows and skipped matches: groups proven by statistics, a group matched only in part, a group narrowed by the selection, no group and every group proven, under offsets and limits that land inside and around proven runs
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\{Option, Options, Reader, Writer};
use Flow\Parquet\Reader\{ReadPlan, RowSelection, RowSelector, SkippedMatches};
use Flow\Parquet\Tests\Context\MemoryParquetFile;

use function Flow\Parquet\DSL\parquet_column;

$bytes = MemoryParquetFile::written(
    Writer::php(options: (new Options())->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 50)),
    Schema::with(FlatColumn::int64('id'), FlatColumn::string('v')),
    array_map(static fn(int $id): array => ['id' => $id, 'v' => intdiv($id, 100) === 1 && $id % 7 === 0 ? '' : 'v' . $id], range(0, 299)),
);

function read_all(Reader $reader, string $bytes, ReadPlan $plan): string
{
    $rows = [];
    $skipped = [];

    $file = MemoryParquetFile::read($reader, $bytes);

    foreach ($file->reader()->readColumns(['id', 'v'], 32, $plan) as $chunk) {
        if ($chunk instanceof SkippedMatches) {
            $skipped[] = $chunk->count;

            continue;
        }

        foreach (array_keys($chunk['id']) as $index) {
            $rows[] = [$chunk['id'][$index], $chunk['v'][$index]];
        }
    }

    return 'skipped ' . json_encode($skipped) . ', ' . count($rows) . ' rows ' . ($rows === [] ? '' : $rows[0][0] . '..' . $rows[count($rows) - 1][0]) . ' ' . md5(json_encode($rows));
}

function selection(array $selectors): RowSelection
{
    return new RowSelection(...array_map(static fn(array $selector): RowSelector => new RowSelector(...$selector), $selectors));
}

$php = Reader::php();
$arrow = new Reader(engine: new RustParquetEngine());
$file = MemoryParquetFile::read($php, $bytes);
$filter = BoundFilter::bind(parquet_column('v')->notEquals(''), $file->schema(), new Options());
$everything = BoundFilter::bind(parquet_column('id')->greaterThanOrEqual(0), $file->schema(), new Options());
$planned = ReadPlan::forFilter($file->reader()->footer(), $file->indexes(), $filter);

echo 'row groups: ', json_encode(array_map(static fn($group): int => $group->rowsCount(), $file->metadata()->rowGroups()->all())), "\n";
echo 'statistics prove: ', json_encode(array_map(static fn(RowSelector $selector): array => [$selector->rowCount, $selector->skip], $planned->matched->selectors())), "\n";

$plans = [
    'statistics' => static fn(?int $limit, int $offset): ReadPlan => ReadPlan::forFilter($file->reader()->footer(), $file->indexes(), $filter, limit: $limit, offset: $offset),
    'first group matched in part' => static fn(?int $limit, int $offset): ReadPlan => new ReadPlan(
        RowSelection::all(300),
        selection([[50, false], [150, true], [100, false]]),
        $file->indexes(), $filter, $limit, $offset,
    ),
    'last group narrowed by the selection and proven' => static fn(?int $limit, int $offset): ReadPlan => new ReadPlan(
        selection([[250, false], [20, true], [30, false]]),
        selection([[200, true], [50, false], [20, true], [30, false]]),
        $file->indexes(), $filter, $limit, $offset,
    ),
    'no group proven' => static fn(?int $limit, int $offset): ReadPlan => new ReadPlan(RowSelection::all(300), RowSelection::none(300), $file->indexes(), $filter, $limit, $offset),
    'every group proven' => static fn(?int $limit, int $offset): ReadPlan => new ReadPlan(RowSelection::all(300), RowSelection::all(300), $file->indexes(), $everything, $limit, $offset),
];

foreach ($plans as $label => $plan) {
    foreach ([[null, 0], [null, 30], [null, 49], [null, 50], [null, 120], [10, 40], [75, 30], [30, 135], [null, 250], [10, 280], [null, 400], [5, 400]] as [$limit, $offset]) {
        $expected = read_all($php, $bytes, $plan($limit, $offset));
        $actual = read_all($arrow, $bytes, $plan($limit, $offset));

        echo $label, ' limit ', json_encode($limit), ' offset ', $offset, ': ', $expected === $actual ? substr($actual, 0, strrpos($actual, ' ')) : "MISMATCH\n  php:   " . $expected . "\n  arrow: " . $actual, "\n";
    }
}
?>
--EXPECT--
row groups: [100,100,100]
statistics prove: [[100,false],[100,true],[100,false]]
statistics limit null offset 0: skipped [], 286 rows 0..299
statistics limit null offset 30: skipped [30], 256 rows 30..299
statistics limit null offset 49: skipped [49], 237 rows 49..299
statistics limit null offset 50: skipped [50], 236 rows 50..299
statistics limit null offset 120: skipped [120], 166 rows 123..299
statistics limit 10 offset 40: skipped [40], 10 rows 40..49
statistics limit 75 offset 30: skipped [30], 75 rows 30..104
statistics limit 30 offset 135: skipped [135], 30 rows 141..174
statistics limit null offset 250: skipped [250], 36 rows 264..299
statistics limit 10 offset 280: skipped [280], 6 rows 294..299
statistics limit null offset 400: skipped [286], 0 rows 
statistics limit 5 offset 400: skipped [286], 0 rows 
first group matched in part limit null offset 0: skipped [], 286 rows 0..299
first group matched in part limit null offset 30: skipped [30], 256 rows 30..299
first group matched in part limit null offset 49: skipped [49], 237 rows 49..299
first group matched in part limit null offset 50: skipped [50], 236 rows 50..299
first group matched in part limit null offset 120: skipped [120], 166 rows 123..299
first group matched in part limit 10 offset 40: skipped [40], 10 rows 40..49
first group matched in part limit 75 offset 30: skipped [30], 75 rows 30..104
first group matched in part limit 30 offset 135: skipped [135], 30 rows 141..174
first group matched in part limit null offset 250: skipped [250], 36 rows 264..299
first group matched in part limit 10 offset 280: skipped [280], 6 rows 294..299
first group matched in part limit null offset 400: skipped [286], 0 rows 
first group matched in part limit 5 offset 400: skipped [286], 0 rows 
last group narrowed by the selection and proven limit null offset 0: skipped [], 266 rows 0..299
last group narrowed by the selection and proven limit null offset 30: skipped [30], 236 rows 30..299
last group narrowed by the selection and proven limit null offset 49: skipped [49], 217 rows 49..299
last group narrowed by the selection and proven limit null offset 50: skipped [50], 216 rows 50..299
last group narrowed by the selection and proven limit null offset 120: skipped [120], 146 rows 123..299
last group narrowed by the selection and proven limit 10 offset 40: skipped [40], 10 rows 40..49
last group narrowed by the selection and proven limit 75 offset 30: skipped [30], 75 rows 30..104
last group narrowed by the selection and proven limit 30 offset 135: skipped [135], 30 rows 141..174
last group narrowed by the selection and proven limit null offset 250: skipped [250], 16 rows 284..299
last group narrowed by the selection and proven limit 10 offset 280: skipped [266], 0 rows 
last group narrowed by the selection and proven limit null offset 400: skipped [266], 0 rows 
last group narrowed by the selection and proven limit 5 offset 400: skipped [266], 0 rows 
no group proven limit null offset 0: skipped [], 286 rows 0..299
no group proven limit null offset 30: skipped [30], 256 rows 30..299
no group proven limit null offset 49: skipped [49], 237 rows 49..299
no group proven limit null offset 50: skipped [50], 236 rows 50..299
no group proven limit null offset 120: skipped [120], 166 rows 123..299
no group proven limit 10 offset 40: skipped [40], 10 rows 40..49
no group proven limit 75 offset 30: skipped [30], 75 rows 30..104
no group proven limit 30 offset 135: skipped [135], 30 rows 141..174
no group proven limit null offset 250: skipped [250], 36 rows 264..299
no group proven limit 10 offset 280: skipped [280], 6 rows 294..299
no group proven limit null offset 400: skipped [286], 0 rows 
no group proven limit 5 offset 400: skipped [286], 0 rows 
every group proven limit null offset 0: skipped [], 300 rows 0..299
every group proven limit null offset 30: skipped [30], 270 rows 30..299
every group proven limit null offset 49: skipped [49], 251 rows 49..299
every group proven limit null offset 50: skipped [50], 250 rows 50..299
every group proven limit null offset 120: skipped [120], 180 rows 120..299
every group proven limit 10 offset 40: skipped [40], 10 rows 40..49
every group proven limit 75 offset 30: skipped [30], 75 rows 30..104
every group proven limit 30 offset 135: skipped [135], 30 rows 135..164
every group proven limit null offset 250: skipped [250], 50 rows 250..299
every group proven limit 10 offset 280: skipped [280], 10 rows 280..289
every group proven limit null offset 400: skipped [300], 0 rows 
every group proven limit 5 offset 400: skipped [300], 0 rows 
