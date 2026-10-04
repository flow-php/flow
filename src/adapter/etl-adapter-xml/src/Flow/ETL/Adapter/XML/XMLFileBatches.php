<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\Filesystem\Filesystem;
use Generator;

use function count;

final readonly class XMLFileBatches implements FileBatches
{
    /**
     * @param int<1, max> $bufferSize
     */
    public function __construct(
        private Filesystem $filesystem,
        private string $xmlNodePath,
        private int $bufferSize,
    ) {}

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        SourceFile $source,
        Schema $body,
        int $batchSize,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $batches = new XMLNodeBatches(new XMLNodes($this->xmlNodePath), $batchSize, $this->bufferSize);
        $node = $body->get('node');
        $stream = $this->filesystem->readFrom($source->path);

        try {
            yield from match (true) {
                $node instanceof XMLDefinition && count($body->definitions()) === 1 => $batches->physicals(
                    $stream,
                    $node,
                    $body,
                    $backend,
                ),
                $node instanceof StringDefinition => $batches->strings($stream, $body, $backend),
                default => $batches->documents($stream, $body, $backend),
            };
        } finally {
            $stream->close();
        }

        return 0;
    }
}
