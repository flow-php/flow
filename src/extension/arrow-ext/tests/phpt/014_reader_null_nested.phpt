--TEST--
Reader propagates nulls correctly in nested columns
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

// null list (row 2 tags) is PHP null, NOT empty array
echo "null list is null: ";
var_dump($data['tags'][2] === null);
echo "null list is not array: ";
var_dump(!is_array($data['tags'][2]));

// empty list (row 1 tags) is PHP empty array, NOT null
echo "empty list is array: ";
var_dump(is_array($data['tags'][1]));
echo "empty list is not null: ";
var_dump($data['tags'][1] !== null);
echo "empty list count: ";
var_dump(count($data['tags'][1]));

// null struct (row 1 metadata) is PHP null
echo "null struct is null: ";
var_dump($data['metadata'][1] === null);

// null map (row 2 props) is PHP null
echo "null map is null: ";
var_dump($data['props'][2] === null);

$reader->close();
?>
--EXPECT--
null list is null: bool(true)
null list is not array: bool(true)
empty list is array: bool(true)
empty list is not null: bool(true)
empty list count: int(0)
null struct is null: bool(true)
null map is null: bool(true)
