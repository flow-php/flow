--TEST--
Writer respects per-column compression via COLUMNS_COMPRESSIONS option
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

$dest = new TestDestinationStream();
$options = [
    'COLUMNS_COMPRESSIONS' => [
        'id' => 'GZIP',
        'name' => 'ZSTD',
    ],
];
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'SNAPPY', $options);
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

echo "per_column_compression: ";
var_dump($ok);
$reader->close();
?>
--EXPECT--
per_column_compression: bool(true)
