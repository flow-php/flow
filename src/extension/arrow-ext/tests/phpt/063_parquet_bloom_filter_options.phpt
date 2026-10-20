--TEST--
COLUMNS_BLOOM_FILTERS reaches the arrow writer: the columns asked get a bloom filter, located in PHP metadata and holding the written values; a column not asked gets none, and a digit-only path (an int array key) is not skipped
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Writer;

$id = FlatColumn::int64('id');
$name = FlatColumn::string('name');
$options = (new Options())->set(Option::COLUMNS_BLOOM_FILTERS, [
    'id' => new BloomFilterOptions(0.01, 100),
    'name' => new BloomFilterOptions(),
    '2024' => new BloomFilterOptions(),
]);
$file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
    new Writer(options: $options, engine: new RustParquetEngine()),
    Schema::with($id, $name, $year = FlatColumn::int64('2024'), $other = FlatColumn::int64('other')),
    [['id' => 7, 'name' => 'ann', '2024' => 5, 'other' => 1], ['id' => 9, 'name' => 'bob', '2024' => 6, 'other' => 2]],
));

foreach ($file->metadata()->columnChunks() as $chunk) {
    echo $chunk->flatPath(), ': ', $chunk->bloomFilterOffset() === null ? 'no bloom filter' : 'bloom filter', "\n";
}

$hash = new BloomHash();
echo 'id 7: ', var_export($file->indexes()->bloomFilter(0, $id)?->check($hash->of($id, 7)), true), "\n";
echo 'name bob: ', var_export($file->indexes()->bloomFilter(0, $name)?->check($hash->of($name, 'bob')), true), "\n";
echo '2024 6: ', var_export($file->indexes()->bloomFilter(0, $year)?->check($hash->of($year, 6)), true), "\n";
echo 'other: ', var_export($file->indexes()->bloomFilter(0, $other), true), "\n";
?>
--EXPECT--
id: bloom filter
name: bloom filter
2024: bloom filter
other: no bloom filter
id 7: true
name bob: true
2024 6: true
other: NULL
