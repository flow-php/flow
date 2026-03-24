--TEST--
Writer preserves null values for multiple column types
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
    ['name' => 'col_int64', 'type' => 'INT64', 'optional' => true],
    ['name' => 'col_string', 'type' => 'STRING', 'optional' => true],
    ['name' => 'col_bool', 'type' => 'BOOLEAN', 'optional' => true],
    ['name' => 'col_double', 'type' => 'DOUBLE', 'optional' => true],
    ['name' => 'col_decimal', 'type' => 'DECIMAL', 'optional' => true, 'precision' => 10, 'scale' => 2],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'col_int64' => [null, 42, null, 99, null],
    'col_string' => ['hello', null, null, 'world', null],
    'col_bool' => [true, null, false, null, null],
    'col_double' => [null, null, 3.14, null, 2.71],
    'col_decimal' => ['1.23', null, null, '-4.56', null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

echo "int64: ";
var_dump(
    $data['col_int64'][0] === null
    && $data['col_int64'][1] === 42
    && $data['col_int64'][2] === null
    && $data['col_int64'][3] === 99
    && $data['col_int64'][4] === null
);

echo "string: ";
var_dump(
    $data['col_string'][0] === 'hello'
    && $data['col_string'][1] === null
    && $data['col_string'][2] === null
    && $data['col_string'][3] === 'world'
    && $data['col_string'][4] === null
);

echo "bool: ";
var_dump(
    $data['col_bool'][0] === true
    && $data['col_bool'][1] === null
    && $data['col_bool'][2] === false
    && $data['col_bool'][3] === null
    && $data['col_bool'][4] === null
);

echo "double: ";
var_dump(
    $data['col_double'][0] === null
    && $data['col_double'][1] === null
    && $data['col_double'][2] === 3.14
    && $data['col_double'][3] === null
    && $data['col_double'][4] === 2.71
);

echo "decimal: ";
var_dump(
    $data['col_decimal'][0] === 1.23
    && $data['col_decimal'][1] === null
    && $data['col_decimal'][2] === null
    && $data['col_decimal'][3] === -4.56
    && $data['col_decimal'][4] === null
);

$reader->close();
?>
--EXPECT--
int64: bool(true)
string: bool(true)
bool: bool(true)
double: bool(true)
decimal: bool(true)
