--TEST--
Extension phpinfo shows version and library info
--SKIPIF--
<?php if (!extension_loaded("arrow")) die("skip"); ?>
--FILE--
<?php

ob_start();
phpinfo(INFO_MODULES);
$info = ob_get_clean();

echo (str_contains($info, 'arrow.enabled') ? "arrow.enabled found" : "ERROR: arrow.enabled missing") . "\n";
echo (str_contains($info, 'arrow.extension_version') ? "arrow.extension_version found" : "ERROR: arrow.extension_version missing") . "\n";
echo (str_contains($info, 'arrow.library_version') ? "arrow.library_version found" : "ERROR: arrow.library_version missing") . "\n";
echo (str_contains($info, 'arrow.parquet_library_version') ? "arrow.parquet_library_version found" : "ERROR: arrow.parquet_library_version missing") . "\n";
?>
--EXPECT--
arrow.enabled found
arrow.extension_version found
arrow.library_version found
arrow.parquet_library_version found
