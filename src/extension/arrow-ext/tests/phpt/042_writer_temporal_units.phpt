--TEST--
Writer honours the declared TIMESTAMP and TIME units
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

$time = new DateInterval('PT3H4M5S');
$time->f = 0.678901;

$dest = new TestDestinationStream();
$writer = new Flow\Arrow\Parquet\Writer($dest, [
    ['name' => 'ts_ms', 'type' => 'TIMESTAMP', 'optional' => true, 'unit' => 'MILLIS', 'utc' => false],
    ['name' => 'ts_ns', 'type' => 'TIMESTAMP', 'optional' => true, 'unit' => 'NANOS'],
    ['name' => 't_ms', 'type' => 'TIME', 'optional' => true, 'unit' => 'MILLIS'],
    ['name' => 't_ns', 'type' => 'TIME', 'optional' => true, 'unit' => 'NANOS'],
], 'UNCOMPRESSED');
$writer->writeBatch([
    'ts_ms' => [new DateTimeImmutable('2020-01-02 03:04:05.678901 UTC')],
    'ts_ns' => [new DateTimeImmutable('2020-01-02 03:04:05.678901 UTC')],
    't_ms' => [$time],
    't_ns' => [$time],
]);
$writer->close();

$source = new TestSourceStream($dest->data);
$reader = new Flow\Arrow\Parquet\Reader($source);
$data = $reader->readRowGroup();

echo $data['ts_ms'][0]->format('Y-m-d H:i:s.u'), "\n";
echo $data['ts_ns'][0]->format('Y-m-d H:i:s.u'), "\n";
echo $data['t_ms'][0]->format('%H:%I:%S.%F'), "\n";
echo $data['t_ns'][0]->format('%H:%I:%S.%F'), "\n";

try {
    $invalid = new TestDestinationStream();
    new Flow\Arrow\Parquet\Writer($invalid, [
        ['name' => 'ts', 'type' => 'TIMESTAMP', 'optional' => true, 'unit' => 'SECONDS'],
    ], 'UNCOMPRESSED');
} catch (Flow\Arrow\Parquet\Exception $e) {
    echo $e->getMessage(), "\n";
}
?>
--EXPECT--
2020-01-02 03:04:05.678000
2020-01-02 03:04:05.678901
03:04:05.678000
03:04:05.678901
Column 'ts': unsupported unit 'SECONDS', expected MILLIS, MICROS or NANOS
