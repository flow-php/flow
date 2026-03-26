--TEST--
Reader reads STRUCT columns from nested.parquet
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

// Row 0: {key: "x", value: 10}
echo "row0 is array: ";
var_dump(is_array($data['metadata'][0]));
echo "row0 key: ";
var_dump($data['metadata'][0]['key']);
echo "row0 value: ";
var_dump($data['metadata'][0]['value']);

// Row 1: null
echo "row1 is null: ";
var_dump($data['metadata'][1] === null);

// Row 2: {key: "y", value: 20}
echo "row2 key: ";
var_dump($data['metadata'][2]['key']);
echo "row2 value: ";
var_dump($data['metadata'][2]['value']);

$reader->close();
?>
--EXPECT--
row0 is array: bool(true)
row0 key: string(1) "x"
row0 value: int(10)
row1 is null: bool(true)
row2 key: string(1) "y"
row2 value: int(20)
