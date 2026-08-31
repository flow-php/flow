<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Schema;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile;
use Generator;

use function count;

final readonly class FileSchemas
{
    public function __construct(
        private SchemaConverter $converter,
    ) {}

    /**
     * Abandons the generator after the first file, so a glob of any size costs one open.
     *
     * @param Generator<array{file: ParquetFile, stream: SourceStream}> $files
     * @param list<string> $columns
     */
    public function first(Generator $files, array $columns): Schema
    {
        foreach ($files as $fileData) {
            $schema = $this->converter->toFlow($fileData['file']->schema());
            $fileData['stream']->close();

            return count($columns) ? $schema->keep(...$columns) : $schema;
        }

        return new Schema();
    }

    /**
     * @param Generator<array{file: ParquetFile, stream: SourceStream}> $files
     * @param list<string> $columns
     */
    public function union(Generator $files, array $columns): Schema
    {
        $schema = new Schema();

        foreach ($files as $fileData) {
            $fileSchema = $this->converter->toFlow($fileData['file']->schema());
            $schema = $schema->merge(count($columns) ? $fileSchema->keep(...$columns) : $fileSchema);
            $fileData['stream']->close();
        }

        return $schema;
    }
}
