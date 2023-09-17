<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Thrift\FileMetaData;

final class Metadata
{
    /**
     * @psalm-suppress MixedOperand
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedArgument
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly RowGroups $rowGroups,
        private readonly int $rowsNumber,
        private readonly int $version,
        private readonly ?string $createdBy,
    ) {
    }

    public static function fromThrift(FileMetaData $thrift) : self
    {
        return new self(
            Schema::fromThrift($thrift->schema),
            RowGroups::fromThrift($thrift->row_groups),
            $thrift->num_rows,
            $thrift->version,
            $thrift->created_by
        );
    }

    public function createdBy() : ?string
    {
        return $this->createdBy;
    }

    public function rowGroups() : RowGroups
    {
        return $this->rowGroups;
    }

    public function rowsNumber() : int
    {
        return $this->rowsNumber;
    }

    public function schema() : Schema
    {
        return $this->schema;
    }

    public function version() : int
    {
        return $this->version;
    }
}
