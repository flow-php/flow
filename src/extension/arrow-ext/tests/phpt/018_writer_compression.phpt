--TEST--
Writer supports all compression codecs and defaults to SNAPPY
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip"); ?>
--FILE--
<?php

require_once __DIR__ . '/../../php/Flow/Arrow/Parquet/Exception.php';
require_once __DIR__ . '/../../php/Flow/Arrow/OutputStream.php';
require_once __DIR__ . '/../../php/Flow/Arrow/RandomAccessFile.php';

class TestDestinationStream implements Flow\Arrow\OutputStream {
    public string $data = '';
    public function append(string $data): self {
        $this->data .= $data;
        return $this;
    }
}

class TestSourceStream implements Flow\Arrow\RandomAccessFile {
    private string $data;
    public function __construct(string $data) {
        $this->data = $data;
    }
    public function read(int $length, int $offset): string {
        return substr($this->data, $offset, $length);
    }
    public function size(): ?int {
        return strlen($this->data);
    }
}

$schema = [
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
    ['name' => 'name', 'type' => 'STRING', 'optional' => false],
];
$batch = [
    'id' => [1, 2, 3],
    'name' => ['Alice', 'Bob', 'Charlie'],
];

$codecs = ['UNCOMPRESSED', 'SNAPPY', 'GZIP', 'ZSTD', 'LZ4', 'BROTLI'];

foreach ($codecs as $codec) {
    $dest = new TestDestinationStream();
    $writer = new Flow\Arrow\Parquet\Writer($dest, $schema, $codec);
    $writer->writeBatch($batch);
    $writer->close();

    $source = new TestSourceStream($dest->data);
    $reader = new Flow\Arrow\Parquet\Reader($source);
    $data = $reader->readRowGroup();

    $ok = $data['id'][0] === 1
        && $data['id'][1] === 2
        && $data['id'][2] === 3
        && $data['name'][0] === 'Alice'
        && $data['name'][1] === 'Bob'
        && $data['name'][2] === 'Charlie';

    echo "$codec: ";
    var_dump($ok);
    $reader->close();
}

// Default compression (no arg) should also work (SNAPPY)
$dest = new TestDestinationStream();
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema);
$writer->writeBatch($batch);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

echo "DEFAULT: ";
var_dump($data['id'][0] === 1 && $data['name'][2] === 'Charlie');
$reader->close();
?>
--EXPECT--
UNCOMPRESSED: bool(true)
SNAPPY: bool(true)
GZIP: bool(true)
ZSTD: bool(true)
LZ4: bool(true)
BROTLI: bool(true)
DEFAULT: bool(true)
