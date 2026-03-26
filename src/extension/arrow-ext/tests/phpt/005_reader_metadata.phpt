--TEST--
Reader metadata returns file info
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
$meta = $reader->metadata();

var_dump($meta['rows']);
var_dump($meta['row_groups']);
var_dump(is_string($meta['created_by']));
var_dump(is_array($meta['metadata']));
$reader->close();
?>
--EXPECT--
int(3)
int(1)
bool(true)
bool(true)
