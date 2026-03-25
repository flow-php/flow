--TEST--
Writer throws exception when float value overflows Float32
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
    ['name' => 'col_float', 'type' => 'FLOAT'],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema, 'UNCOMPRESSED');

try {
    $writer->writeBatch([
        'col_float' => [1.0e39],
    ]);
    echo "ERROR: No exception thrown\n";
} catch (Flow\Arrow\Parquet\Exception $e) {
    echo "Caught expected exception\n";
    echo (str_contains($e->getMessage(), 'overflows') ? "Message contains 'overflows'" : "ERROR: unexpected message: " . $e->getMessage()) . "\n";
}
?>
--EXPECT--
Caught expected exception
Message contains 'overflows'
