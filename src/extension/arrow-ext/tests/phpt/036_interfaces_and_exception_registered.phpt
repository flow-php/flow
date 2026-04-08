--TEST--
arrow extension registers OutputStream, RandomAccessFile interfaces and Parquet\Exception class
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip arrow extension not loaded"); ?>
--FILE--
<?php
var_dump(interface_exists('Flow\Arrow\OutputStream', false));
var_dump(interface_exists('Flow\Arrow\RandomAccessFile', false));
var_dump(class_exists('Flow\Arrow\Parquet\Exception', false));
var_dump(is_subclass_of('Flow\Arrow\Parquet\Exception', \Exception::class));

$r = new ReflectionClass('Flow\Arrow\OutputStream');
var_dump($r->isInterface());
var_dump($r->hasMethod('append'));

$r = new ReflectionClass('Flow\Arrow\RandomAccessFile');
var_dump($r->isInterface());
var_dump($r->hasMethod('read'));
var_dump($r->hasMethod('size'));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
