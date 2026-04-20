<?php

declare(strict_types=1);

use function Flow\Filesystem\DSL\{fstab, path};

require __DIR__ . '/vendor/autoload.php';

$outputStream = fstab()->for('stdout')->writeTo(path('stdout://'));

$outputStream->append("Files List\n\n");

foreach (fstab()->for('file')->list(path(__DIR__ . '/*')) as $file) {
    $outputStream->append(($file->isFile() ? 'File' : 'Directory') . ': ' . $file->path->basename() . "\n");
}

$outputStream->close();
