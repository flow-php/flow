--TEST--
arrow refuses Parquet modular encryption when the file is opened: an encrypted footer, and a plaintext footer holding an encrypted column chunk
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Engine\RustParquetFileReader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Thrift\{CompactProtocol, MemoryBuffer};
use Flow\Parquet\ThriftModel\{ColumnCryptoMetaData, EncryptionWithFooterKey, FileMetaData};

echo arrow_outcome(static fn() => new RustParquetFileReader(new MemorySourceStream("PAR1\0\0\0\0\4\0\0\0PARE"))), "\n";

$bytes = MemoryParquetFile::threeRowGroups();
$length = unpack('V', substr($bytes, -8, 4))[1];
$footer = new FileMetaData();
$footer->read(new CompactProtocol(new MemoryBuffer(substr($bytes, -8 - $length, $length))));
$footer->row_groups[1]->columns[0]->crypto_metadata = new ColumnCryptoMetaData(['ENCRYPTION_WITH_FOOTER_KEY' => new EncryptionWithFooterKey()]);
$footer->write(new CompactProtocol($encrypted = new MemoryBuffer()));
$file = substr($bytes, 0, -8 - $length) . $encrypted->data() . pack('V', strlen($encrypted->data())) . 'PAR1';

echo arrow_outcome(static fn() => new RustParquetFileReader(new MemorySourceStream($file))), "\n";
?>
--EXPECT--
Flow\Parquet\Exception\RuntimeException: arrow does not read encrypted Parquet files
Flow\Parquet\Exception\RuntimeException: arrow does not read encrypted Parquet files
