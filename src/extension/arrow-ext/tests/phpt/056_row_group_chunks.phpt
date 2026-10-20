--TEST--
arrow slices the asked leaves' column chunks and orders from the footer for RustRowGroupChunks::fromThrift(), called by name: the statistics the PHP engine decodes from the same file
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Engine\Arrow\RustRowGroupChunks;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;

$bytes = MemoryParquetFile::threeRowGroups();
$phpFile = MemoryParquetFile::read(Reader::php(), $bytes);
$php = $phpFile->reader()->footer();
$id = $php->schema()->getFlat('id');
$tag = $php->schema()->getFlat('tags.list.element');

var_dump(is_callable([RustRowGroupChunks::class, 'fromThrift']));

$rust = (new RustParquetFileReader(new MemorySourceStream($bytes)))->footer()->rowGroupChunks([$id, $tag]);
$expected = $php->rowGroupChunks([$id, $tag]);

$rowGroups = $php->metadata()->rowGroups()->all();
$groups = count($rowGroups);

echo get_class($rust), ' ', $groups, "\n";

for ($group = 0; $group < $groups; $group++) {
    foreach ([$id, $tag] as $column) {
        $statistics = $rust->chunk($group, $column)->statistics();
        printf(
            "%d %s: %d rows, %s..%s, %s\n",
            $group,
            $column->flatPath(),
            $rowGroups[$group]->rowsCount(),
            var_export($statistics?->minValue($column), true),
            var_export($statistics?->maxValue($column), true),
            $rust->chunk($group, $column) == $expected->chunk($group, $column) ? 'as PHP decodes it' : 'NOT as PHP decodes it',
        );
    }
}

var_dump($rust->columnOrder($id) === $expected->columnOrder($id));
echo arrow_outcome(static fn() => (new RustParquetFileReader(new MemorySourceStream($bytes)))->footer()->rowGroupChunks([FlatColumn::int64('missing')])), "\n";
echo arrow_outcome(static fn() => (new RustParquetFileReader(new MemorySourceStream($bytes)))->footer()->rowGroupChunks([FlatColumn::int64('tags')])), "\n";
?>
--EXPECT--
bool(true)
Flow\Parquet\Engine\Arrow\RustRowGroupChunks 3
0 id: 2 rows, 1..2, as PHP decodes it
0 tags.list.element: 2 rows, 't1'..'t2', as PHP decodes it
1 id: 2 rows, 3..4, as PHP decodes it
1 tags.list.element: 2 rows, 't3'..'t4', as PHP decodes it
2 id: 2 rows, 5..6, as PHP decodes it
2 tags.list.element: 2 rows, 't5'..'t6', as PHP decodes it
bool(true)
Flow\Parquet\Exception\InvalidArgumentException: Parquet file has no leaf column "missing"
Flow\Parquet\Exception\InvalidArgumentException: Parquet file has no leaf column "tags"
