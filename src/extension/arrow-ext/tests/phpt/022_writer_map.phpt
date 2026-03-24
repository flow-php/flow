--TEST--
Writer roundtrips MAP(STRING, INT64) columns through Writer then Reader
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
    ['name' => 'props', 'type' => 'MAP', 'optional' => true, 'children' => [
        ['name' => 'keys', 'type' => 'STRING', 'optional' => false],
        ['name' => 'values', 'type' => 'INT64', 'optional' => true],
    ]],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'props' => [['p1' => 100, 'p2' => 200], ['p3' => 300], null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
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
