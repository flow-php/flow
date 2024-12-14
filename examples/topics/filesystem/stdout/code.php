<?php

declare(strict_types=1);

use function Flow\Filesystem\DSL\{fstab, path, protocol};

require __DIR__ . '/../../../autoload.php';

$outputStream = fstab()->for(protocol('stdout'))->writeTo(path('stdout://'));

$outputStream->append("Files List\n\n");

foreach (fstab()->for(protocol('file'))->list(path(__DIR__ . '/*')) as $file) {
    $outputStream->append(($file->isFile() ? 'File' : 'Directory') . ': ' . $file->path->basename() . "\n");
}

$outputStream->close();
