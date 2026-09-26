--TEST--
Reader converts FIXED_LEN_BYTE_ARRAY decimals of every width, Decimal256 included
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

$source = new TestSourceStream(file_get_contents(__DIR__ . '/../fixtures/decimals_fixed_len.parquet'));
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

foreach ([0, 1] as $row) {
    foreach (['dec9', 'dec18', 'dec38', 'dec50'] as $column) {
        var_dump($data[$column][$row]);
    }
}
?>
--EXPECT--
float(12345.67)
float(1234567890123456.8)
float(1.2345678901234568E+18)
float(1.2345678901234568E+39)
float(-12345.67)
float(-0.01)
float(-12345.67)
float(-12345.67)
