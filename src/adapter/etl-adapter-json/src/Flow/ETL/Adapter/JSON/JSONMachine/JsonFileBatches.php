<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\AdaptiveJsonOpenSource;
use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Generator;

final readonly class JsonFileBatches implements FileBatches
{
    public function __construct(
        private Filesystem $filesystem,
        private JsonFileReader $reader,
        private JsonFormat $format,
        private ?string $pointer,
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
        $open = new AdaptiveJsonOpenSource($this->filesystem, $this->reader, $this->format, $this->pointer, $source);

        try {
            yield from $open->batches($body, $batchSize, $backend);
        } finally {
            $open->close();
        }

        return 0;
    }
}
