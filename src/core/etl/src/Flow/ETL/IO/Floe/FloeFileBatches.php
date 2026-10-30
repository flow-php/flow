<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\OffsetSkippingFileBatches;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Storage\Floe\FloeStreamReader;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_diff;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;

final readonly class FloeFileBatches implements OffsetSkippingFileBatches
{
    /**
     * @param null|DerivedSchema $derived the schema every file must match; null under unionByName
     */
    public function __construct(
        private Codec $codec,
        private int $chunkSize,
        private ?DerivedSchema $derived,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $derived = type_optional(type_array())->assert($data['derived'] ?? null);

        return new self(
            new NoopCodec(),
            type_integer()->assert($data['chunk_size'] ?? null),
            $derived === null ? null : DerivedSchema::fromArray($derived),
        );
    }

    public function describe(): ?Description
    {
        if (!$this->codec instanceof NoopCodec) {
            return null;
        }

        return Description::compose(self::class, [
            'chunk_size' => $this->chunkSize,
            'derived' => $this->derived?->normalize(),
        ]);
    }

    /**
     * Skips the whole file when the window's offset covers it; otherwise reads from the offset. A split owns the
     * sections whose midpoint its byte window holds, and a window without a length owns every section.
     *
     * @throws InferredSchemaException
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
        $split = $criteria->split;
        $reader = new FloeStreamReader(
            $read->files->filesystem->readFrom($source->path),
            $this->codec,
            $this->chunkSize,
            $backend,
        );

        try {
            $file = new FloeSourceFile($reader->footer(), $source);
            $fileRows = $reader->totalRows();
            $owned = $split?->length === null ? null : $split->owns($file->units());

            if ($owned === null && $window->offset >= $fileRows) {
                return $fileRows;
            }

            if ($owned?->count === 0) {
                return 0;
            }

            $this->derived?->refuseDivergence($file->source()->uri(), $file->schema());

            $fileBody = $read->fileColumns->withoutTail($file->schema());
            $fileBody = $fileBody->gracefulRemove(...array_diff($fileBody->names(), $body->names()));

            $projects = !$fileBody->isSame($file->schema());
            $matches = !$fileBody->isSame($body);

            foreach ($reader->rows(
                $read->batchSize,
                $owned->offset ?? $window->offset,
                $owned->count ?? $window->limit,
            ) as $rows) {
                if ($projects) {
                    $rows = $rows->project($fileBody, $backend);
                }

                yield $matches ? $rows->matchTo($body, $backend) : $rows;
            }

            return $owned === null ? $window->offset : 0;
        } finally {
            $reader->close();
        }
    }

    public function close(): void {}
}
