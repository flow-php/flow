<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path;
use Generator;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function max;

final readonly class FileRead
{
    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public SourceFiles $files,
        public FileColumns $fileColumns,
        public Schema $schema,
        public int $batchSize,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize(), described
     */
    public static function fromArray(array $data, Rebuild $rebuild): self
    {
        return new self(
            new SourceFiles(
                $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
                Path::fromArray(type_array()->assert($data['source'] ?? null)),
            ),
            FileColumns::fromArray(type_array()->assert($data['file_columns'] ?? null)),
            Schema::fromArray(type_array()->assert($data['schema'] ?? null)),
            max(1, type_integer()->assert($data['batch_size'] ?? null)),
        );
    }

    /**
     * @return array{filesystem: ConfiguredFilesystem, source: array<string, mixed>, file_columns: array<string, mixed>, schema: array<array-key, mixed>, batch_size: int<1, max>}
     */
    public function normalize(): array
    {
        return [
            'filesystem' => new ConfiguredFilesystem($this->files->filesystem),
            'source' => $this->files->path->normalize(),
            'file_columns' => $this->fileColumns->normalize(),
            'schema' => $this->schema->normalize(),
            'batch_size' => $this->batchSize,
        ];
    }

    /**
     * The files $criteria reads, through $batches.
     *
     * @return Generator<int, CorruptedRecordException|Rows, null|Signal, void>
     */
    public function scan(Backend $backend, Criteria $criteria, FileBatches $batches): Generator
    {
        yield from (new FileReadLoop($this, $criteria))->read(
            $this->files->of($criteria),
            $batches,
            $backend,
            new ReadWindow($criteria->offset, $criteria->limit),
        );
    }
}
