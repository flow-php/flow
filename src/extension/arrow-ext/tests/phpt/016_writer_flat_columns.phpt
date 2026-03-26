--TEST--
Writer writes INT64+STRING columns and Reader reads them back
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
$schema = [
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
    ['name' => 'name', 'type' => 'STRING', 'optional' => false],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'name' => ['Alice', 'Bob', 'Charlie'],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

var_dump($data['id']);
var_dump($data['name']);

$reader->close();
?>
--EXPECT--
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(2)
  [2]=>
  int(3)
}
array(3) {
  [0]=>
  string(5) "Alice"
  [1]=>
  string(3) "Bob"
  [2]=>
  string(7) "Charlie"
}
