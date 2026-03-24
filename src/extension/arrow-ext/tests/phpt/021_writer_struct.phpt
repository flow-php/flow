--TEST--
Writer roundtrips STRUCT columns through Writer then Reader
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
    ['name' => 'metadata', 'type' => 'STRUCT', 'optional' => true, 'children' => [
        ['name' => 'key', 'type' => 'STRING', 'optional' => false],
        ['name' => 'value', 'type' => 'INT64', 'optional' => false],
    ]],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'metadata' => [['key' => 'x', 'value' => 10], null, ['key' => 'y', 'value' => 20]],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
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
