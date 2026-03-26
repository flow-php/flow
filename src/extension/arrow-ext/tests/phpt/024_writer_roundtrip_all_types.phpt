--TEST--
Writer roundtrips flat and nested types combined in a single batch
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
    ['name' => 'name', 'type' => 'STRING', 'optional' => true],
    ['name' => 'score', 'type' => 'DOUBLE', 'optional' => true],
    ['name' => 'tags', 'type' => 'LIST', 'optional' => true, 'children' => [
        ['name' => 'item', 'type' => 'STRING', 'optional' => true]
    ]],
    ['name' => 'meta', 'type' => 'STRUCT', 'optional' => true, 'children' => [
        ['name' => 'key', 'type' => 'STRING', 'optional' => false],
        ['name' => 'val', 'type' => 'INT64', 'optional' => true],
    ]],
    ['name' => 'attrs', 'type' => 'MAP', 'optional' => true, 'children' => [
        ['name' => 'keys', 'type' => 'STRING', 'optional' => false],
        ['name' => 'values', 'type' => 'STRING', 'optional' => true],
    ]],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'id' => [1, 2, 3],
    'name' => ['Alice', null, 'Charlie'],
    'score' => [9.5, 8.0, null],
    'tags' => [['php', 'rust'], [], null],
    'meta' => [['key' => 'role', 'val' => 1], null, ['key' => 'team', 'val' => null]],
    'attrs' => [['color' => 'red', 'size' => 'large'], ['color' => 'blue'], null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

// Flat columns
echo "id: ";
var_dump($data['id'][0] === 1 && $data['id'][1] === 2 && $data['id'][2] === 3);
echo "name: ";
var_dump($data['name'][0] === 'Alice' && $data['name'][1] === null && $data['name'][2] === 'Charlie');
echo "score: ";
var_dump($data['score'][0] === 9.5 && $data['score'][1] === 8.0 && $data['score'][2] === null);

// LIST
echo "tags0: ";
var_dump($data['tags'][0][0] === 'php' && $data['tags'][0][1] === 'rust');
echo "tags1: ";
var_dump(is_array($data['tags'][1]) && count($data['tags'][1]) === 0);
echo "tags2: ";
var_dump($data['tags'][2] === null);

// STRUCT
echo "meta0: ";
var_dump($data['meta'][0]['key'] === 'role' && $data['meta'][0]['val'] === 1);
echo "meta1: ";
var_dump($data['meta'][1] === null);
echo "meta2 key: ";
var_dump($data['meta'][2]['key'] === 'team');
echo "meta2 val: ";
var_dump($data['meta'][2]['val'] === null);

// MAP
echo "attrs0: ";
var_dump($data['attrs'][0]['color'] === 'red' && $data['attrs'][0]['size'] === 'large');
echo "attrs1: ";
var_dump($data['attrs'][1]['color'] === 'blue');
echo "attrs2: ";
var_dump($data['attrs'][2] === null);

$reader->close();
?>
--EXPECT--
id: bool(true)
name: bool(true)
score: bool(true)
tags0: bool(true)
tags1: bool(true)
tags2: bool(true)
meta0: bool(true)
meta1: bool(true)
meta2 key: bool(true)
meta2 val: bool(true)
attrs0: bool(true)
attrs1: bool(true)
attrs2: bool(true)
