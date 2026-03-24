--TEST--
Writer roundtrips LIST(STRING) columns through Writer then Reader
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
    ['name' => 'tags', 'type' => 'LIST', 'optional' => true, 'children' => [
        ['name' => 'item', 'type' => 'STRING', 'optional' => true]
    ]],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'tags' => [["a", "b", "c"], [], null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
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

// Verify id column
echo "ids: ";
var_dump($data['id'][0] === 1 && $data['id'][1] === 2 && $data['id'][2] === 3);

$reader->close();
?>
--EXPECT--
row0 is array: bool(true)
row0 count: int(3)
row0 values: bool(true)
row1 is array: bool(true)
row1 count: int(0)
row2 is null: bool(true)
ids: bool(true)
