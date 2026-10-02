<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileReader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Iterator;
use Throwable;

use function extension_loaded;
use function str_starts_with;
use function strlen;
use function strspn;
use function substr;

final readonly class AdaptiveJsonOpenSource implements JsonOpenSource
{
    /**
     * flow_php's `source::CHUNK` (src/extension/flow-php-ext/src/source.rs): the peeked chunk is the first one the
     * Rust source feeds.
     */
    private const int CHUNK = 1 << 15;

    private JsonOpenSource $source;

    public function __construct(
        Filesystem $filesystem,
        JsonFileReader $reader,
        JsonFormat $format,
        ?string $pointer,
        SourceFile $source,
    ) {
        if (!extension_loaded('flow_php') || $pointer !== null) {
            $this->source = new PhpJsonOpenSource($reader, $source);

            return;
        }

        $stream = $filesystem->readFrom($source->path);

        try {
            $head = '';
            $offset = 0;

            if ($format === JsonFormat::Document) {
                // the first byte after JSON whitespace and one leading BOM: '[' is Rust's; an object, a scalar or an
                // empty file is the PHP source's (member iteration / JSON Machine's verdict)
                $first = '';

                for (; ($head = $stream->read(self::CHUNK, $offset)) !== ''; $offset += strlen($head)) {
                    $bytes = $offset === 0 && str_starts_with($head, "\u{FEFF}") ? substr($head, 3) : $head;
                    $skip = strspn($bytes, " \t\n\r");

                    if ($skip < strlen($bytes)) {
                        $first = $bytes[$skip];

                        break;
                    }
                }

                if ($first !== '[') {
                    $stream->close();
                    $this->source = new PhpJsonOpenSource($reader, $source);

                    return;
                }
            }

            $this->source = new RustJsonOpenSource(
                $stream,
                $format === JsonFormat::Lines,
                $source->uri(),
                $head,
                $offset,
            );
        } catch (Throwable $e) {
            $stream->close();

            throw $e;
        }
    }

    public function batches(Schema $schema, int $batchSize, Backend $backend): Iterator
    {
        return $this->source->batches($schema, $batchSize, $backend);
    }

    public function close(): void
    {
        $this->source->close();
    }
}
