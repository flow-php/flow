--TEST--
Reader reads MAP columns from nested.parquet
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

// Row 0: {"p1": 100, "p2": 200}
echo "row0 is array: ";
var_dump(is_array($data['props'][0]));
echo "row0 p1: ";
var_dump($data['props'][0]['p1']);
echo "row0 p2: ";
var_dump($data['props'][0]['p2']);

// Row 1: {"p3": 300}
echo "row1 p3: ";
var_dump($data['props'][1]['p3']);

// Row 2: null
echo "row2 is null: ";
var_dump($data['props'][2] === null);

$reader->close();
?>
--EXPECT--
row0 is array: bool(true)
row0 p1: int(100)
row0 p2: int(200)
row1 p3: int(300)
row2 is null: bool(true)
