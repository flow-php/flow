--TEST--
Writer rounds float DECIMAL values half away from zero
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
$writer = new Flow\Arrow\Parquet\Writer($dest, [
    ['name' => 'd', 'type' => 'DECIMAL', 'optional' => true, 'precision' => 9, 'scale' => 2],
], 'UNCOMPRESSED');
$writer->writeBatch(['d' => [0.125, 2.675, -0.125, 1.005, 9.995, -99.995]]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);

foreach ($reader->readRowGroup()['d'] as $value) {
    var_dump($value);
}
?>
--EXPECT--
float(0.13)
float(2.68)
float(-0.13)
float(1.01)
float(10)
float(-100)
