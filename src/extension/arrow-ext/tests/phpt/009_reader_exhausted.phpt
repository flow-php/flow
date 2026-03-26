--TEST--
Reader returns null when all row groups are consumed
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

$stream = new TestSourceStream(__DIR__ . '/../fixtures/simple.parquet');
$reader = new Flow\Arrow\Parquet\Reader($stream);

$rg1 = $reader->readRowGroup();
echo "first: ";
var_dump(is_array($rg1));

$rg2 = $reader->readRowGroup();
echo "second: ";
var_dump($rg2);

$rg3 = $reader->readRowGroup();
echo "third: ";
var_dump($rg3);

$reader->close();
?>
--EXPECT--
first: bool(true)
second: NULL
third: NULL
