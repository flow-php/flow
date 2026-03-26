--TEST--
Reader BATCH_SIZE streams transparently across row group boundaries
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

$dest = new TestDestinationStream();
$writerOptions = ['ROW_GROUP_SIZE_BYTES' => 500];
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED', $writerOptions);

$ids = [];
$names = [];
for ($i = 0; $i < 100; $i++) {
    $ids[] = $i;
    $names[] = "name_" . $i;
}
$writer->writeBatch(['id' => $ids, 'name' => $names]);
$writer->close();

$source = new TestSourceStream($dest->data);
$readerOptions = ['BATCH_SIZE' => 3];
$reader = new Flow\Arrow\Parquet\Reader($source, $readerOptions);

$totalRows = 0;
$batchCount = 0;
while (null !== ($batch = $reader->readRowGroup())) {
    $totalRows += count($batch['id']);
    $batchCount++;
}
$reader->close();

echo "total_rows: " . $totalRows . "\n";
echo "multiple_batches: ";
var_dump($batchCount > 1);
echo "all_rows_read: ";
var_dump($totalRows === 100);
?>
--EXPECT--
total_rows: 100
multiple_batches: bool(true)
all_rows_read: bool(true)
