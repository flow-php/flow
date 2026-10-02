--TEST--
RowsWriter::writeColumns() accepts, refuses and keeps what writeRows() does, for every write-acceptance cell and across flushed batches
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\Engine\ArrowParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\ColumnDoor;
use Flow\Parquet\Tests\Context\WriteAcceptance;

$outcome = static function (Schema $schema, array $rows, string $door, int $batchSize = 1_000): string {
    $stream = ColumnDoor::stream();
    $file = ColumnDoor::open(new ArrowParquetEngine(Options::default()->set(Option::ARROW_WRITE_BATCH_SIZE, $batchSize)), $stream, $schema);
    $refusal = 'written';

    try {
        $door === 'rows' ? $file->writeBatch($rows) : $file->writeColumns(ColumnDoor::columns($rows));
    } catch (Throwable $e) {
        $refusal = $e::class . ': ' . $e->getMessage();
    }

    $file->close();

    return $refusal . ' ' . var_export(ColumnDoor::read($stream), true);
};

$identical = 0;
$refused = 0;
$cells = 0;

foreach (WriteAcceptance::targets() as $target) {
    foreach (WriteAcceptance::inputs() as $input) {
        $schema = Schema::with(WriteAcceptance::target($target));
        // an accepted row before the cell: a refusal keeps it
        $rows = [['c' => null], ['c' => WriteAcceptance::input($input)]];
        $cells++;

        try {
            $rowsOutcome = $outcome($schema, $rows, 'rows');
            $columnsOutcome = $outcome($schema, $rows, 'columns');
        } catch (Throwable $e) {
            // a REQUIRED target refuses the null row before the cell is reached, through both doors
            $rowsOutcome = $columnsOutcome = $e::class;
        }

        $refused += (int) !str_starts_with($rowsOutcome, 'written');

        if ($rowsOutcome === $columnsOutcome) {
            $identical++;
        } else {
            echo "{$target} <- {$input}\n  rows:    {$rowsOutcome}\n  columns: {$columnsOutcome}\n";
        }
    }
}

echo "{$identical} of {$cells} cells identical, ", $refused > 0 ? 'some refused' : 'none refused', "\n";

$schema = Schema::with(FlatColumn::int32('a'), FlatColumn::int32('b'), FlatColumn::int32('c'));
$rows = array_map(static fn(int $i): array => ['a' => $i, 'b' => $i, 'c' => $i], range(0, 9));
$rows[7]['a'] = 'seven';
$rows[4]['c'] = 'four';
$rows[4]['b'] = 'four';

foreach ([1_000, 3, 2, 1] as $batchSize) {
    $rowsOutcome = $outcome($schema, $rows, 'rows', $batchSize);
    echo "batch size {$batchSize}: ", $rowsOutcome === $outcome($schema, $rows, 'columns', $batchSize) ? strtok($rowsOutcome, "\n") : 'DIFFERENT', "\n";
}

$columns = ColumnDoor::columns(ColumnDoor::rows(50));
$batch = ColumnDoor::stream();
$file = ColumnDoor::open(new ArrowParquetEngine(Options::default()->set(Option::ARROW_WRITE_BATCH_SIZE, 7)), $batch, ColumnDoor::schema());
$file->writeBatch(ColumnDoor::rows(50));
$file->close();
$door = ColumnDoor::stream();
$file = ColumnDoor::open(new ArrowParquetEngine(Options::default()->set(Option::ARROW_WRITE_BATCH_SIZE, 7)), $door, ColumnDoor::schema());
$file->writeColumns($columns);
$file->close();
var_dump($batch->content() === $door->content());

foreach ([['a' => [1, 2], 'b' => [1]], ['a' => 'no list']] as $columns) {
    try {
        ColumnDoor::open(new ArrowParquetEngine(), ColumnDoor::stream(), $schema)->writeColumns($columns);
    } catch (Throwable $e) {
        echo $e::class, ': ', $e->getMessage(), "\n";
    }
}
?>
--EXPECT--
624 of 624 cells identical, some refused
batch size 1000: Flow\Parquet\Exception\ValidationException: Column "b" row 4: expected int, got string array (
batch size 3: Flow\Parquet\Exception\ValidationException: Column "b" row 1: expected int, got string array (
batch size 2: Flow\Parquet\Exception\ValidationException: Column "b" row 0: expected int, got string array (
batch size 1: Flow\Parquet\Exception\ValidationException: Column "b" row 0: expected int, got string array (
bool(true)
Flow\Parquet\Exception\InvalidArgumentException: writeColumns() takes lists of one length, got "a": 2, "b": 1
Flow\Parquet\Exception\InvalidArgumentException: arrow Parquet writer columns must be arrays
