--TEST--
Reader returns only selected columns with projection
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip"); ?>
--FILE--
<?php

require_once __DIR__ . '/../../php/Flow/Arrow/Parquet/Exception.php';
require_once __DIR__ . '/../../php/Flow/Arrow/RandomAccessFile.php';

class TestSourceStream implements Flow\Arrow\RandomAccessFile {
    private string $data;

    public function __construct(string $filePath) {
        $this->data = file_get_contents($filePath);
    }

    public function read(int $length, int $offset): string {
        return substr($this->data, $offset, $length);
    }

    public function size(): ?int {
        return strlen($this->data);
    }
}

$stream = new TestSourceStream(__DIR__ . '/../fixtures/simple.parquet');
$reader = new Flow\Arrow\Parquet\Reader($stream);
$data = $reader->readRowGroup(['name']);

echo "column count: ";
var_dump(count($data));

echo "has name: ";
var_dump(array_key_exists('name', $data));

echo "has id: ";
var_dump(array_key_exists('id', $data));

echo "name values: ";
var_dump($data['name']);

$reader->close();
?>
--EXPECT--
column count: int(1)
has name: bool(true)
has id: bool(false)
name values: array(3) {
  [0]=>
  string(5) "Alice"
  [1]=>
  string(3) "Bob"
  [2]=>
  string(7) "Charlie"
}
