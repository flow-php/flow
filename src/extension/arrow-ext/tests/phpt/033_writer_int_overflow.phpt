--TEST--
Writer throws exception when integer value overflows target type
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip"); ?>
--FILE--
<?php

require_once __DIR__ . '/../../php/Flow/Arrow/Parquet/Exception.php';
require_once __DIR__ . '/../../php/Flow/Arrow/OutputStream.php';

class TestDestinationStream implements Flow\Arrow\OutputStream {
    public string $data = '';
    public function append(string $data): self {
        $this->data .= $data;
        return $this;
    }
}

$dest = new TestDestinationStream();
$schema = [
    ['name' => 'col_int8', 'type' => 'INT8'],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');

try {
    $writer->writeBatch([
        'col_int8' => [256],
    ]);
    echo "ERROR: No exception thrown\n";
} catch (Flow\Arrow\Parquet\Exception $e) {
    echo "Caught expected exception\n";
    echo (str_contains($e->getMessage(), 'out of range') ? "Message contains 'out of range'" : "ERROR: unexpected message: " . $e->getMessage()) . "\n";
    echo (str_contains($e->getMessage(), '256') ? "Message contains the value" : "ERROR: message missing value") . "\n";
}

// Also test negative value in unsigned type
$dest2 = new TestDestinationStream();
$schema2 = [
    ['name' => 'col_uint64', 'type' => 'UINT64'],
];

$writer2 = new Flow\Arrow\Parquet\Writer($dest2, $schema2, 'UNCOMPRESSED');

try {
    $writer2->writeBatch([
        'col_uint64' => [-1],
    ]);
    echo "ERROR: No exception thrown\n";
} catch (Flow\Arrow\Parquet\Exception $e) {
    echo "Caught expected exception for negative uint64\n";
    echo (str_contains($e->getMessage(), 'out of range') ? "Message contains 'out of range'" : "ERROR: unexpected message: " . $e->getMessage()) . "\n";
}
?>
--EXPECT--
Caught expected exception
Message contains 'out of range'
Message contains the value
Caught expected exception for negative uint64
Message contains 'out of range'
