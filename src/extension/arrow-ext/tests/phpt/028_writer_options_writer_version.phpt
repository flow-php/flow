--TEST--
Writer respects WRITER_VERSION option for v1 and v2
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

$schema = [
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
    ['name' => 'name', 'type' => 'STRING', 'optional' => false],
];
$batch = [
    'id' => [1, 2, 3],
    'name' => ['Alice', 'Bob', 'Charlie'],
];

foreach ([1, 2] as $version) {
    $dest = new TestDestinationStream();
    $options = ['WRITER_VERSION' => $version];
    $writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'SNAPPY', $options);
    $writer->writeBatch($batch);
    $writer->close();

    $source = new TestSourceStream($dest->data);
    $reader = new Flow\Arrow\Parquet\Reader($source);
    $data = $reader->readRowGroup();

    $ok = $data['id'][0] === 1
        && $data['id'][1] === 2
        && $data['id'][2] === 3
        && $data['name'][0] === 'Alice'
        && $data['name'][1] === 'Bob'
        && $data['name'][2] === 'Charlie';

    echo "v$version: ";
    var_dump($ok);
    $reader->close();
}
?>
--EXPECT--
v1: bool(true)
v2: bool(true)
