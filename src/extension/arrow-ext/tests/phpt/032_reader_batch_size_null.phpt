--TEST--
Reader with BATCH_SIZE null returns full row group in one call
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
];

$dest = new TestDestinationStream();
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema);
$writer->writeBatch([
    'id' => [1, 2, 3, 4, 5],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$options = ['BATCH_SIZE' => null];
$reader = new Flow\Arrow\Parquet\Reader($source, $options);

$batch = $reader->readRowGroup();
echo "count: " . count($batch['id']) . "\n";
echo "values_ok: ";
var_dump($batch['id'] === [1, 2, 3, 4, 5]);

$next = $reader->readRowGroup();
echo "exhausted: ";
var_dump($next === null);

$reader->close();
?>
--EXPECT--
count: 5
values_ok: bool(true)
exhausted: bool(true)
