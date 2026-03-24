--TEST--
Writer roundtrips all 16 flat types through Writer then Reader
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
    ['name' => 'col_bool', 'type' => 'BOOLEAN', 'optional' => true],
    ['name' => 'col_int8', 'type' => 'INT8', 'optional' => true],
    ['name' => 'col_int16', 'type' => 'INT16', 'optional' => true],
    ['name' => 'col_int32', 'type' => 'INT32', 'optional' => true],
    ['name' => 'col_int64', 'type' => 'INT64', 'optional' => true],
    ['name' => 'col_uint8', 'type' => 'UINT8', 'optional' => true],
    ['name' => 'col_uint16', 'type' => 'UINT16', 'optional' => true],
    ['name' => 'col_uint32', 'type' => 'UINT32', 'optional' => true],
    ['name' => 'col_uint64', 'type' => 'UINT64', 'optional' => true],
    ['name' => 'col_float', 'type' => 'FLOAT', 'optional' => true],
    ['name' => 'col_double', 'type' => 'DOUBLE', 'optional' => true],
    ['name' => 'col_string', 'type' => 'STRING', 'optional' => true],
    ['name' => 'col_binary', 'type' => 'BINARY', 'optional' => true],
    ['name' => 'col_date', 'type' => 'DATE', 'optional' => true],
    ['name' => 'col_timestamp', 'type' => 'TIMESTAMP', 'optional' => true],
    ['name' => 'col_decimal', 'type' => 'DECIMAL', 'optional' => true, 'precision' => 10, 'scale' => 2],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'col_bool' => [true, false, null],
    'col_int8' => [1, -128, null],
    'col_int16' => [256, -32768, null],
    'col_int32' => [100000, -100000, null],
    'col_int64' => [1000000000, -1000000000, null],
    'col_uint8' => [0, 255, null],
    'col_uint16' => [0, 65535, null],
    'col_uint32' => [0, 4294967295, null],
    'col_uint64' => [0, "18446744073709551615", null],
    'col_float' => [1.5, -2.5, null],
    'col_double' => [3.14159, -2.71828, null],
    'col_string' => ['hello', 'world', null],
    'col_binary' => ["\x00\x01\x02", "\xff\xfe", null],
    'col_date' => [1641600000, 0, null],
    'col_timestamp' => [1700000000000000, 0, null],
    'col_decimal' => ['123.45', '-999.99', null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

echo "bool: ";
var_dump($data['col_bool'][0] === true && $data['col_bool'][1] === false && $data['col_bool'][2] === null);

echo "int8: ";
var_dump($data['col_int8'][0] === 1 && $data['col_int8'][1] === -128 && $data['col_int8'][2] === null);

echo "int16: ";
var_dump($data['col_int16'][0] === 256 && $data['col_int16'][1] === -32768 && $data['col_int16'][2] === null);

echo "int32: ";
var_dump($data['col_int32'][0] === 100000 && $data['col_int32'][1] === -100000 && $data['col_int32'][2] === null);

echo "int64: ";
var_dump($data['col_int64'][0] === 1000000000 && $data['col_int64'][1] === -1000000000 && $data['col_int64'][2] === null);

echo "uint8: ";
var_dump($data['col_uint8'][0] === 0 && $data['col_uint8'][1] === 255 && $data['col_uint8'][2] === null);

echo "uint16: ";
var_dump($data['col_uint16'][0] === 0 && $data['col_uint16'][1] === 65535 && $data['col_uint16'][2] === null);

echo "uint32: ";
var_dump($data['col_uint32'][0] === 0 && $data['col_uint32'][1] === 4294967295 && $data['col_uint32'][2] === null);

echo "uint64: ";
var_dump($data['col_uint64'][0] === 0 && $data['col_uint64'][1] === "18446744073709551615" && $data['col_uint64'][2] === null);

echo "float: ";
var_dump(is_float($data['col_float'][0]) && is_float($data['col_float'][1]) && $data['col_float'][2] === null);

echo "double: ";
var_dump($data['col_double'][0] === 3.14159 && $data['col_double'][1] === -2.71828 && $data['col_double'][2] === null);

echo "string: ";
var_dump($data['col_string'][0] === "hello" && $data['col_string'][1] === "world" && $data['col_string'][2] === null);

echo "binary: ";
var_dump($data['col_binary'][0] === "\x00\x01\x02" && $data['col_binary'][1] === "\xff\xfe" && $data['col_binary'][2] === null);

echo "date: ";
var_dump($data['col_date'][0] instanceof DateTimeImmutable && $data['col_date'][0]->getTimestamp() === 1641600000 && $data['col_date'][1] instanceof DateTimeImmutable && $data['col_date'][1]->getTimestamp() === 0 && $data['col_date'][2] === null);

echo "timestamp: ";
var_dump($data['col_timestamp'][0] instanceof DateTimeImmutable && $data['col_timestamp'][0]->getTimestamp() === 1700000000 && $data['col_timestamp'][1] instanceof DateTimeImmutable && $data['col_timestamp'][1]->getTimestamp() === 0 && $data['col_timestamp'][2] === null);

echo "decimal: ";
var_dump($data['col_decimal'][0] === 123.45 && $data['col_decimal'][1] === -999.99 && $data['col_decimal'][2] === null);

$reader->close();
?>
--EXPECT--
bool: bool(true)
int8: bool(true)
int16: bool(true)
int32: bool(true)
int64: bool(true)
uint8: bool(true)
uint16: bool(true)
uint32: bool(true)
uint64: bool(true)
float: bool(true)
double: bool(true)
string: bool(true)
binary: bool(true)
date: bool(true)
timestamp: bool(true)
decimal: bool(true)
