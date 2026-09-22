--TEST--
native CSV consumedBytes() counts the bytes of the rows it produced exactly like the PHP path, over any chunk size
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\CSVEncoder;
use Flow\ETL\Adapter\CSV\CSVLineReader;
use Flow\ETL\Adapter\CSV\PhpCSVOpenSource;
use Flow\ETL\Adapter\CSV\RustColumnFoldNative;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\Filesystem\DSL\path;

$php = static function (string $raw, bool $withHeader, int $rows): int {
    $source = new PhpCSVOpenSource(
        new StringSourceStream(path('memory://phpt.csv'), $raw),
        new CSVEncoder(withHeader: $withHeader),
        new CSVLineReader('"'),
    );
    $taken = 0;

    foreach ($source->records() as $values) {
        if (++$taken >= $rows) {
            break;
        }
    }

    return $source->producedBytes();
};

$native = static function (string $raw, bool $withHeader, int $rows, int $chunk): int {
    $reader = new RustCSVReaderNative(',', '"', '\\', $withHeader, true, true);
    $fold = new RustColumnFoldNative([], []);
    $folded = 0;

    foreach (str_split($raw, $chunk) as $piece) {
        $reader->feed($piece);
        $folded += $reader->fold($fold, $rows - $folded);
    }

    $reader->finish();
    $reader->fold($fold, $rows - $folded);

    return $reader->consumedBytes();
};

$cases = [
    'LF' => ["a,b\n1,2\n33,44\n", true, 2],
    'CRLF' => ["a,b\r\n1,2\r\n33,44\r\n", true, 2],
    'quoted multi-line LF' => ["a,b\n\"x\ny\",1\n2,3\n", true, 2],
    'quoted multi-line CRLF' => ["a,b\r\n\"x\r\ny\",1\r\n2,3\r\n", true, 2],
    'blank lines' => ["a,b\n\n1,2\n\n", true, 3],
    'BOM without header' => ["\xEF\xBB\xBF1,2\n3,4\n", false, 2],
    'first two of three rows' => ["a,b\n1,2\n33,44\n555,666\n", true, 2],
];

foreach ($cases as $label => [$raw, $withHeader, $rows]) {
    $expected = $php($raw, $withHeader, $rows);
    $counts = array_unique(array_map(
        static fn(int $chunk): int => $native($raw, $withHeader, $rows, $chunk),
        [1, 3, 7, 4096],
    ));

    echo $label, ': ', $counts === [$expected] ? 'identical ' . $expected : 'FAIL php=' . $expected . ' native=' . implode(',', $counts), "\n";
}

$reader = new RustCSVReaderNative(',', '"', '\\', true, true, true);
$reader->feed("a,b\n1,2\n33,44\n");
$reader->finish();
$reader->next(1);
echo 'next() counts too: ', $reader->consumedBytes(), "\n";
$reader->next(10);
echo 'header excluded: ', $reader->consumedBytes(), "\n";
?>
--EXPECT--
LF: identical 10
CRLF: identical 12
quoted multi-line LF: identical 12
quoted multi-line CRLF: identical 15
blank lines: identical 6
BOM without header: identical 11
first two of three rows: identical 10
next() counts too: 4
header excluded: 10
