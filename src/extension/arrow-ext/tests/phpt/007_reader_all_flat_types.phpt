--TEST--
Reader converts all flat Arrow types correctly
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

$stream = new TestSourceStream(__DIR__ . '/../fixtures/all_flat_types.parquet');
$reader = new Flow\Arrow\Parquet\Reader($stream);
$data = $reader->readRowGroup();

// Boolean
echo "bool: ";
var_dump($data['col_bool'][0] === true && $data['col_bool'][1] === false && $data['col_bool'][2] === null);

// Integers
echo "int8: ";
var_dump($data['col_int8'][0] === 1 && $data['col_int8'][1] === -128 && $data['col_int8'][2] === null);

echo "int16: ";
var_dump($data['col_int16'][0] === 256 && $data['col_int16'][1] === -32768 && $data['col_int16'][2] === null);

echo "int32: ";
var_dump($data['col_int32'][0] === 100000 && $data['col_int32'][1] === -100000 && $data['col_int32'][2] === null);

echo "int64: ";
var_dump($data['col_int64'][0] === 1000000000 && $data['col_int64'][1] === -1000000000 && $data['col_int64'][2] === null);

// Unsigned integers
echo "uint8: ";
var_dump($data['col_uint8'][0] === 0 && $data['col_uint8'][1] === 255 && $data['col_uint8'][2] === null);

echo "uint16: ";
var_dump($data['col_uint16'][0] === 0 && $data['col_uint16'][1] === 65535 && $data['col_uint16'][2] === null);

echo "uint32: ";
var_dump($data['col_uint32'][0] === 0 && $data['col_uint32'][1] === 4294967295 && $data['col_uint32'][2] === null);

echo "uint64: ";
var_dump($data['col_uint64'][0] === 0 && $data['col_uint64'][1] === "18446744073709551615" && $data['col_uint64'][2] === null);

// Floats
echo "float: ";
var_dump(is_float($data['col_float'][0]) && is_float($data['col_float'][1]) && $data['col_float'][2] === null);

echo "double: ";
var_dump($data['col_double'][0] === 1.23456 && $data['col_double'][1] === -7.89012 && $data['col_double'][2] === null);

// String
echo "string: ";
var_dump($data['col_string'][0] === "hello" && $data['col_string'][1] === "world" && $data['col_string'][2] === null);

// Binary
echo "binary: ";
var_dump($data['col_binary'][0] === "\x00\x01\x02" && $data['col_binary'][1] === "\xff\xfe" && $data['col_binary'][2] === null);

// Date (19000 days since epoch = 2021-12-24)
echo "date: ";
var_dump($data['col_date'][0] instanceof DateTimeImmutable && $data['col_date'][0]->getTimestamp() === 1641600000 && $data['col_date'][1] instanceof DateTimeImmutable && $data['col_date'][1]->getTimestamp() === 0 && $data['col_date'][2] === null);

// Timestamp (1700000000000000 microseconds = 2023-11-14T22:13:20)
echo "timestamp: ";
var_dump($data['col_timestamp'][0] instanceof DateTimeImmutable && $data['col_timestamp'][0]->getTimestamp() === 1700000000 && $data['col_timestamp'][1] instanceof DateTimeImmutable && $data['col_timestamp'][1]->getTimestamp() === 0 && $data['col_timestamp'][2] === null);

// Decimal (string representation)
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
