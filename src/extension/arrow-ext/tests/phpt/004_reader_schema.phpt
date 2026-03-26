--TEST--
Reader schema returns column descriptors
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
$schema = $reader->schema();

var_dump(count($schema));
var_dump($schema[0]['name']);
var_dump($schema[0]['type']);
var_dump($schema[0]['optional']);
var_dump($schema[1]['name']);
var_dump($schema[1]['type']);
var_dump($schema[1]['optional']);
$reader->close();
?>
--EXPECT--
int(2)
string(2) "id"
string(5) "INT64"
bool(false)
string(4) "name"
string(6) "STRING"
bool(false)
