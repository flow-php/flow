<?php

declare(strict_types=1);

use function Flow\Filesystem\DSL\{fstab, path, protocol};

require __DIR__ . '/../../../autoload.php';

$filesystem = fstab()->for(protocol('file'));
$outputStream = $filesystem->writeTo(path(__DIR__ . '/output.txt'));

$outputStream->append("Files List\n\n");

foreach ($filesystem->list(path(__DIR__ . '/*')) as $file) {
    $outputStream->append(($file->isFile() ? 'File' : 'Directory') . ': ' . $file->path->basename() . "\n");
}

$outputStream->close();
