--TEST--
Writer respects ROW_GROUP_SIZE_BYTES option to produce multiple row groups
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
$options = ['ROW_GROUP_SIZE_BYTES' => 500];
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED', $options);
for ($i = 0; $i < 100; $i++) {
    $batch = [
        'id' => [($i * 10) + 1, ($i * 10) + 2, ($i * 10) + 3, ($i * 10) + 4, ($i * 10) + 5,
                 ($i * 10) + 6, ($i * 10) + 7, ($i * 10) + 8, ($i * 10) + 9, ($i * 10) + 10],
        'name' => [str_repeat('x', 100), str_repeat('y', 100), str_repeat('z', 100),
                   str_repeat('a', 100), str_repeat('b', 100), str_repeat('c', 100),
                   str_repeat('d', 100), str_repeat('e', 100), str_repeat('f', 100),
                   str_repeat('g', 100)],
    ];
    $writer->writeBatch($batch);
}
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$meta = $reader->metadata();

echo "multiple_row_groups: ";
var_dump($meta['row_groups'] > 1);
$reader->close();
?>
--EXPECT--
multiple_row_groups: bool(true)
