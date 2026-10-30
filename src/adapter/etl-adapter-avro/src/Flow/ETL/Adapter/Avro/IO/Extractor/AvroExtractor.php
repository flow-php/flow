<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\IO\Extractor;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

final class AvroExtractor implements Extractor, FileExtractor
{
    private ?Schema $schema = null;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        throw new RuntimeException(
            'Avro integration was abandoned due to lack of availability of good Avro libraries.',
        );
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield Rows::empty(new Schema(), $backend);
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        throw SchemaNotDerivableException::extractor(self::class);
    }

    public function partitionSchema(): Schema
    {
        // the constructor always throws, so no instance ever lists a partition
        return new Schema();
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return false;
    }

    public function refresh(): void {}

    public function source(): Path
    {
        return $this->path;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function statistics(): Statistics
    {
        return new Statistics();
    }

    public function isRepeatable(): bool
    {
        return false;
    }
}
