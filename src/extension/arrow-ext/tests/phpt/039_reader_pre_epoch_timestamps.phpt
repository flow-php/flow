--TEST--
Reader floors pre-epoch timestamps with a sub-second fraction
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
    ['name' => 'ts', 'type' => 'TIMESTAMP', 'optional' => true],
    ['name' => 'ts_list', 'type' => 'LIST', 'optional' => true, 'children' => [
        ['name' => 'item', 'type' => 'TIMESTAMP', 'optional' => true],
    ]],
], 'UNCOMPRESSED');
$writer->writeBatch([
    'ts' => [-1500000, -1, 1577934245678901],
    'ts_list' => [[-1500000, -1, 1577934245678901], null, null],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

foreach ($data['ts'] as $value) {
    echo $value->format('Y-m-d H:i:s.u'), "\n";
}

foreach ($data['ts_list'][0] as $value) {
    echo $value->format('Y-m-d H:i:s.u'), "\n";
}
?>
--EXPECT--
1969-12-31 23:59:58.500000
1969-12-31 23:59:59.999999
2020-01-02 03:04:05.678901
1969-12-31 23:59:58.500000
1969-12-31 23:59:59.999999
2020-01-02 03:04:05.678901
