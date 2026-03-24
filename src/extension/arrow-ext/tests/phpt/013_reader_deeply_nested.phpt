--TEST--
Reader reads deeply nested columns from deeply_nested.parquet
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

$stream = new TestSourceStream(__DIR__ . '/../fixtures/deeply_nested.parquet');
$reader = new Flow\Arrow\Parquet\Reader($stream);
$data = $reader->readRowGroup();

// Row 0: [{name:"alpha", scores:[10,20]}, {name:"beta", scores:[30]}]
echo "row0 count: ";
var_dump(count($data['groups'][0]));
echo "row0[0] name: ";
var_dump($data['groups'][0][0]['name']);
echo "row0[0] scores: ";
var_dump($data['groups'][0][0]['scores'][0] === 10 && $data['groups'][0][0]['scores'][1] === 20);
echo "row0[1] name: ";
var_dump($data['groups'][0][1]['name']);
echo "row0[1] scores: ";
var_dump($data['groups'][0][1]['scores'][0] === 30);

// Row 1: [{name:"gamma", scores:[]}]
echo "row1 count: ";
var_dump(count($data['groups'][1]));
echo "row1[0] name: ";
var_dump($data['groups'][1][0]['name']);
echo "row1[0] scores count: ";
var_dump(count($data['groups'][1][0]['scores']));

// Row 2: null
echo "row2 is null: ";
var_dump($data['groups'][2] === null);

$reader->close();
?>
--EXPECT--
row0 count: int(2)
row0[0] name: string(5) "alpha"
row0[0] scores: bool(true)
row0[1] name: string(4) "beta"
row0[1] scores: bool(true)
row1 count: int(1)
row1[0] name: string(5) "gamma"
row1[0] scores count: int(0)
row2 is null: bool(true)
