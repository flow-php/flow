--TEST--
Writer roundtrips uuid and json nested in struct, list and map; reader returns canonical uuid strings at every depth
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

$uuid = 'f6d6e0e8-4b7e-4b0e-8d7a-ff0a0c9c9a5a';

$dest = new TestDestinationStream();
$schema = [
    ['name' => 'top_uuid', 'type' => 'UUID', 'optional' => true],
    ['name' => 'body', 'type' => 'STRUCT', 'optional' => true, 'children' => [
        ['name' => 'id', 'type' => 'UUID', 'optional' => true],
        ['name' => 'data', 'type' => 'JSON', 'optional' => true],
        ['name' => 'inner', 'type' => 'STRUCT', 'optional' => true, 'children' => [
            ['name' => 'deep', 'type' => 'UUID', 'optional' => true],
        ]],
    ]],
    ['name' => 'uuid_list', 'type' => 'LIST', 'optional' => true, 'children' => [
        ['name' => 'element', 'type' => 'UUID', 'optional' => true],
    ]],
    ['name' => 'uuid_map', 'type' => 'MAP', 'optional' => true, 'children' => [
        ['name' => 'key', 'type' => 'STRING', 'optional' => false],
        ['name' => 'value', 'type' => 'UUID', 'optional' => true],
    ]],
    ['name' => 'json_map', 'type' => 'MAP', 'optional' => true, 'children' => [
        ['name' => 'key', 'type' => 'STRING', 'optional' => false],
        ['name' => 'value', 'type' => 'JSON', 'optional' => true],
    ]],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');
$writer->writeBatch([
    'top_uuid' => [$uuid],
    'body' => [['id' => $uuid, 'data' => '{"a":1}', 'inner' => ['deep' => $uuid]]],
    'uuid_list' => [[$uuid, $uuid, null]],
    'uuid_map' => [['k' => $uuid]],
    'json_map' => [['k' => '{"d":4}']],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

echo "top_uuid: ";
var_dump($data['top_uuid'][0] === $uuid);
echo "struct id: ";
var_dump($data['body'][0]['id'] === $uuid);
echo "struct json: ";
var_dump($data['body'][0]['data'] === '{"a":1}');
echo "deep struct uuid: ";
var_dump($data['body'][0]['inner']['deep'] === $uuid);
echo "list: ";
var_dump($data['uuid_list'][0] === [$uuid, $uuid, null]);
echo "uuid map: ";
var_dump($data['uuid_map'][0] === ['k' => $uuid]);
echo "json map: ";
var_dump($data['json_map'][0] === ['k' => '{"d":4}']);
?>
--EXPECT--
top_uuid: bool(true)
struct id: bool(true)
struct json: bool(true)
deep struct uuid: bool(true)
list: bool(true)
uuid map: bool(true)
json map: bool(true)
