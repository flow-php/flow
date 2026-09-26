--TEST--
Reader schema describes temporal units, the UTC flag and Decimal256 columns
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
    ['name' => 'ts_ms', 'type' => 'TIMESTAMP', 'optional' => true, 'unit' => 'MILLIS', 'utc' => false],
    ['name' => 'ts_ns', 'type' => 'TIMESTAMP', 'optional' => true, 'unit' => 'NANOS'],
    ['name' => 't_ms', 'type' => 'TIME', 'optional' => true, 'unit' => 'MILLIS'],
    ['name' => 't_ns', 'type' => 'TIME', 'optional' => true, 'unit' => 'NANOS'],
], 'UNCOMPRESSED');
$writer->writeBatch(['ts' => [0], 'ts_ms' => [0], 'ts_ns' => [0], 't_ms' => [0], 't_ns' => [0]]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);

foreach ($reader->schema() as $column) {
    echo $column['name'], ' ', $column['type'], ' ', $column['unit'], ' ', var_export($column['utc'] ?? null, true), "\n";
}

$fixture = new TestSourceStream(file_get_contents(__DIR__ . '/../fixtures/decimals_fixed_len.parquet'));
$decimals = new Flow\Arrow\Parquet\Reader($fixture);

foreach ($decimals->schema() as $column) {
    echo $column['name'], ' ', $column['type'], ' ', $column['precision'], ' ', $column['scale'], "\n";
}
?>
--EXPECT--
ts TIMESTAMP MICROS true
ts_ms TIMESTAMP MILLIS false
ts_ns TIMESTAMP NANOS true
t_ms TIME MILLIS NULL
t_ns TIME NANOS NULL
dec9 DECIMAL 9 2
dec18 DECIMAL 18 2
dec38 DECIMAL 38 10
dec50 DECIMAL 50 10
