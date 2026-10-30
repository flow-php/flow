<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\IO\Extractor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileBatches;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function count;
use function rtrim;

final readonly class TextFileBatches implements FileBatches
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, []);
    }

    /**
     * The lines into the "text" column; any other column $body declares is padded as matchTo() pads it.
     *
     * @return Generator<int, Rows, mixed, int>
     */
    public function batches(
        FileRead $read,
        Criteria $criteria,
        SourceFile $source,
        Schema $body,
        Backend $backend,
        ReadWindow $window,
    ): Generator {
        $text = new Schema($body->get('text'));
        $stream = $read->files->filesystem->readFrom($source->path);

        try {
            $lines = [];

            foreach ($stream->readLines() as $line) {
                $lines[] = rtrim($line);

                if (count($lines) >= $read->batchSize) {
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

    public function close(): void {}
}
