<?php

declare(strict_types=1);

/*
 * Run by `just gen-thrift` after the Thrift compiler: its PHP generator types every field as non-null, but a field the
 * IDL does not require is null when a file leaves it out. Rewrites `@var T` to `@var null|T` for every field whose
 * `$_TSPEC` entry is not required. Running it twice changes nothing.
 */

require __DIR__ . '/../../../../vendor/autoload.php';

$directory = __DIR__ . '/../src/Flow/Parquet/ThriftModel';

foreach (glob($directory . '/*.php') ?: [] as $file) {
    $class = 'Flow\\Parquet\\ThriftModel\\' . basename($file, '.php');

    if (!class_exists($class) || !property_exists($class, '_TSPEC')) {
        continue;
    }

    /** @var array<int, array{var: string, isRequired: bool}> $spec */
    $spec = (new ReflectionProperty($class, '_TSPEC'))->getValue();
    $source = (string) file_get_contents($file);
    $rewritten = $source;

    foreach ($spec as $field) {
        if ($field['isRequired']) {
            continue;
        }

        $rewritten = (string) preg_replace(
            '/(\* @var )(?!null\|)([^\n]+)(\n\s*\*\/\n\s*public \$' . preg_quote($field['var'], '/') . ' = null;)/',
            '$1null|$2$3',
            $rewritten,
        );
    }

    if ($rewritten !== $source) {
        file_put_contents($file, $rewritten);
    }
}
