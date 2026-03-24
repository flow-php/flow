--TEST--
Reader reads LIST columns from nested.parquet
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

$stream = new TestSourceStream(__DIR__ . '/../fixtures/nested.parquet');
$reader = new Flow\Arrow\Parquet\Reader($stream);
$data = $reader->readRowGroup();

// Row 0: ["a", "b", "c"]
echo "row0 is array: ";
var_dump(is_array($data['tags'][0]));
echo "row0 count: ";
var_dump(count($data['tags'][0]));
echo "row0 values: ";
var_dump($data['tags'][0][0] === "a" && $data['tags'][0][1] === "b" && $data['tags'][0][2] === "c");

// Row 1: [] (empty list)
echo "row1 is array: ";
var_dump(is_array($data['tags'][1]));
echo "row1 count: ";
var_dump(count($data['tags'][1]));

// Row 2: null
echo "row2 is null: ";
var_dump($data['tags'][2] === null);

$reader->close();
?>
--EXPECT--
row0 is array: bool(true)
row0 count: int(3)
row0 values: bool(true)
row1 is array: bool(true)
row1 count: int(0)
row2 is null: bool(true)
