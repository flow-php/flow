<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Generator;
use Throwable;

use function extension_loaded;
use function str_starts_with;
use function strlen;
use function strspn;
use function substr;

final readonly class JsonSourceOpener
{
    public function __construct(
        private Filesystem $filesystem,
        private JsonFileReader $reader,
        private JsonFormat $format,
        private ?string $pointer,
    ) {}

    /**
     * Abandoning the generator closes the source.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows>
     */
    public function batches(SourceFile $source, Schema $schema, int $batchSize, Backend $backend): Generator
    {
        $open = $this->open($source);

        try {
            yield from $open->batches($schema, $batchSize, $backend);
        } finally {
            $open->close();
        }
    }

    public function open(SourceFile $source): JsonOpenSource
    {
        if (!extension_loaded('flow_php') || $this->pointer !== null) {
            return new PhpJsonOpenSource($this->reader, $source);
        }

        $stream = $this->filesystem->readFrom($source->path);

        try {
            $head = '';
            $offset = 0;

            if ($this->format === JsonFormat::Document) {
                // the first byte after JSON whitespace and one leading BOM: '[' is native; an object, a scalar or an
                // empty file is the PHP lane's (member iteration / JSON Machine's verdict)
                $first = '';

                for (; ($head = $stream->read(NativeJsonOpenSource::CHUNK, $offset)) !== ''; $offset += strlen($head)) {
                    $bytes = $offset === 0 && str_starts_with($head, "\u{FEFF}") ? substr($head, 3) : $head;
                    $skip = strspn($bytes, " \t\n\r");

                    if ($skip < strlen($bytes)) {
                        $first = $bytes[$skip];

                        break;
                    }
                }

                if ($first !== '[') {
                    $stream->close();

                    return new PhpJsonOpenSource($this->reader, $source);
                }
            }

            return new NativeJsonOpenSource(
                $stream,
                new NativeJsonReader($this->format === JsonFormat::Lines, $source->uri()),
                $head,
                $offset,
            );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }
}
