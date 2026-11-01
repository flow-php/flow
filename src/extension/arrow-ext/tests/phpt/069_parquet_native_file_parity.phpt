--TEST--
A NativeLocalSourceStream is read by Rust with pread and yields what the same file read through a PHP stream yields, for every lib Parquet fixture
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\StringSourceStream;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\ParquetRows;

use function Flow\Filesystem\DSL\path;

$fixtures = realpath(__DIR__ . '/../../../../lib/parquet/tests/Flow/Parquet/Tests/Integration/IO/Fixtures');
$hex = static function (mixed $value) use (&$hex): mixed {
    if (is_array($value)) {
        return array_map($hex, $value);
    }

    return is_string($value) ? bin2hex($value) : $value;
};
$rows = static fn(callable $open): string => arrow_outcome(static fn(): array => $hex(ParquetRows::read($open(), 100_000)));

foreach ([...glob($fixtures . '/*.parquet'), ...glob($fixtures . '/EdgeCases/*.parquet')] as $file) {
    // Reader::read() opens the file through NativeLocalFilesystem: a NativeLocalSourceStream, the native arm
    $native = $rows(static fn() => (new Reader(engine: new RustParquetEngine()))->read($file));
    // a StringSourceStream over the same bytes stays on the PHP arm: every byte through read()
    $php = $rows(static fn() => (new Reader(engine: new RustParquetEngine()))->readStream(new StringSourceStream(path($file), file_get_contents($file))));
    $name = substr($file, strlen($fixtures) + 1);

    echo match (true) {
        $php === $native && arrow_refused($php) => "{$name}: both refuse\n",
        $php === $native => "{$name}: identical\n",
        default => "{$name}: DIFFER\n    php:    " . substr($php, 0, 200) . "\n    native: " . substr($native, 0, 200) . "\n",
    };
}
--EXPECT--
columns.required.parquet: identical
decimals_fixed_len.parquet: identical
delta_binary_acked_encoded_integers.parquet: identical
delta_byte_array_arrow_v2.parquet: identical
delta_length_byte_array_external_v2.parquet: identical
lists.parquet: identical
logical_types.parquet: identical
maps.parquet: identical
multiple_pages.parquet: identical
pagination_row_group_1kb_5k_rows.snappy.parquet: identical
primitives.parquet: identical
structs.parquet: identical
EdgeCases/datapage_v2.snappy.parquet: identical
EdgeCases/int96.parquet: identical
EdgeCases/interval.parquet: both refuse
EdgeCases/map_keys.parquet: both refuse
EdgeCases/nonnullable.impala.parquet: identical
EdgeCases/null_list.parquet: identical
EdgeCases/uint64_overflow.parquet: identical
EdgeCases/unsigned.parquet: identical
