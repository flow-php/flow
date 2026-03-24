--TEST--
Writer constructs, writes a minimal batch, and produces valid Parquet
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
    ['name' => 'id', 'type' => 'INT64', 'optional' => false],
];

$writer = new Flow\Arrow\Parquet\Writer($dest, $schema);
$writer->writeBatch([
    'id' => [1, 2, 3],
]);
$writer->close();

// Parquet files start and end with magic bytes "PAR1"
$magic = substr($dest->data, 0, 4);
$footer = substr($dest->data, -4);

echo "starts_with_PAR1: ";
var_dump($magic === "PAR1");
echo "ends_with_PAR1: ";
var_dump($footer === "PAR1");
echo "size_gt_zero: ";
var_dump(strlen($dest->data) > 0);
?>
--EXPECT--
starts_with_PAR1: bool(true)
ends_with_PAR1: bool(true)
size_gt_zero: bool(true)
