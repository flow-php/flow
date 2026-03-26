--TEST--
Reader constructor throws exception when size() returns null
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip"); ?>
--FILE--
<?php

require_once __DIR__ . '/../../php/Flow/Arrow/Parquet/Exception.php';
require_once __DIR__ . '/../../php/Flow/Arrow/RandomAccessFile.php';

class NullSizeStream implements Flow\Arrow\RandomAccessFile {
    public function read(int $length, int $offset): string {
        return '';
    }

    public function size(): ?int {
        return null;
    }
}

try {
    $stream = new NullSizeStream();
    new Flow\Arrow\Parquet\Reader($stream);
    echo "ERROR: no exception thrown\n";
} catch (Flow\Arrow\Parquet\Exception $e) {
    echo "Caught Flow\\Arrow\\Parquet\\Exception\n";
    var_dump(str_contains($e->getMessage(), 'null'));
} catch (\Exception $e) {
    echo "Caught Exception\n";
    var_dump(str_contains($e->getMessage(), 'null'));
}
?>
--EXPECT--
Caught Flow\Arrow\Parquet\Exception
bool(true)
