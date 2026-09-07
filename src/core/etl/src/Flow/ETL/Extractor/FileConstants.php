<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

final readonly class FileConstants
{
    /**
     * @param null|string $uri null when metadata columns are off
     * @param array<string, bool> $partitionNames
     * @param array<string, mixed> $partitionValues already the type the declared schema gives them
     */
    public function __construct(
        private PartitionColumns $partitionColumns,
        private ?string $uri,
        private array $partitionNames,
        private array $partitionValues,
    ) {}

    /**
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    public function fill(array $row): array
    {
        // plain assignment, unlike the partition columns below: a body column named _input_file_uri
        // keeps its position and is overwritten in place
        if ($this->uri !== null) {
            $row['_input_file_uri'] = $this->uri;
        }

        return $this->partitionColumns->fill($row, $this->partitionNames, $this->partitionValues);
    }
}
