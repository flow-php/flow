--TEST--
Reader BATCH_SIZE option returns rows in batches
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
$options = ['BATCH_SIZE' => 2];
$reader = new Flow\Arrow\Parquet\Reader($source, $options);

$batch1 = $reader->readRowGroup();
echo "batch1_count: " . count($batch1['id']) . "\n";

$batch2 = $reader->readRowGroup();
echo "batch2_count: " . count($batch2['id']) . "\n";

$batch3 = $reader->readRowGroup();
echo "batch3_count: " . count($batch3['id']) . "\n";

$batch4 = $reader->readRowGroup();
echo "batch4_null: ";
var_dump($batch4 === null);

$reader->close();
?>
--EXPECT--
batch1_count: 2
batch2_count: 2
batch3_count: 1
batch4_null: bool(true)
