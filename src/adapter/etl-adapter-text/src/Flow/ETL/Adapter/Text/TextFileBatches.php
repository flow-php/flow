<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Column\Backend;
use Flow\ETL\Extractor\File\FileBatches;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Generator;

use function count;
use function rtrim;

final readonly class TextFileBatches implements FileBatches
{
    public function __construct(
        private Filesystem $filesystem,
    ) {}

    /**
     * The lines into the "text" column; any other column $body declares is padded as matchTo() pads it.
     *
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
        $text = new Schema($body->get('text'));
        $stream = $this->filesystem->readFrom($source->path);

        try {
            $lines = [];

            foreach ($stream->readLines() as $line) {
                $lines[] = rtrim($line);

                if (count($lines) >= $batchSize) {
                    yield $this->batch($text, $lines, $body, $backend);

                    $lines = [];
                }
            }

            if ($lines !== []) {
                yield $this->batch($text, $lines, $body, $backend);
            }
        } finally {
            $stream->close();
        }

        return 0;
    }

    /**
     * @param non-empty-list<string> $lines
     */
    public function batch(Schema $text, array $lines, Schema $body, Backend $backend): Rows
    {
        $builder = $backend->builder($text->get('text'));
        $builder->appendMany($lines);

        return Rows::fromColumns($text, ['text' => $builder->finish()], count($lines))->matchTo($body, $backend);
    }
}
