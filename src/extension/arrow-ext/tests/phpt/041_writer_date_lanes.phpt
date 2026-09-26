--TEST--
Writer DATE int lane is days since epoch, objects write their wall-clock day
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

$dest = new TestDestinationStream();
$writer = new Flow\Arrow\Parquet\Writer($dest, [
    ['name' => 'd_int', 'type' => 'DATE', 'optional' => true],
    ['name' => 'd_object', 'type' => 'DATE', 'optional' => true],
], 'UNCOMPRESSED');
$writer->writeBatch([
    'd_int' => [18263, -1],
    'd_object' => [
        new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('Europe/Warsaw')),
        new DateTimeImmutable('1969-12-31 12:00:00', new DateTimeZone('UTC')),
    ],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

foreach (['d_int', 'd_object'] as $column) {
    foreach ($data[$column] as $value) {
        echo $value->format('Y-m-d'), "\n";
    }
}
?>
--EXPECT--
2020-01-02
1969-12-31
2024-01-01
1969-12-31
