<?php

declare(strict_types=1);

use function Flow\Filesystem\DSL\{fstab, path};

require __DIR__ . '/vendor/autoload.php';

$filesystem = fstab()->for('file');

foreach (['first.txt' => "one\n", 'second.txt' => "two\n"] as $name => $content) {
    $stream = $filesystem->writeTo(path(__DIR__ . '/listing/' . $name));
    $stream->append($content);
    $stream->close();
}

$outputStream = fstab()->for('stdout')->writeTo(path('stdout://'));

$outputStream->append("Files List\n\n");

foreach ($filesystem->list(path(__DIR__ . '/listing/*.txt')) as $file) {
    $outputStream->append(($file->isFile() ? 'File' : 'Directory') . ': ' . $file->path->basename() . "\n");
}

$outputStream->close();
