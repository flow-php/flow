--TEST--
Reader silently ignores unknown options
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
];
$batch = [
    'id' => [1, 2, 3],
];

$dest = new TestDestinationStream();
$writer = new Flow\Arrow\Parquet\Writer($dest, $schema);
$writer->writeBatch($batch);
$writer->close();

$source = new TestSourceStream($dest->data);
$options = [
    'SOME_UNKNOWN_OPTION' => 'whatever',
    'ANOTHER_ONE' => 42,
];
$reader = new Flow\Arrow\Parquet\Reader($source, $options);
$data = $reader->readRowGroup();

echo "read_ok: ";
var_dump($data['id'][0] === 1 && $data['id'][1] === 2 && $data['id'][2] === 3);
$reader->close();
?>
--EXPECT--
read_ok: bool(true)
