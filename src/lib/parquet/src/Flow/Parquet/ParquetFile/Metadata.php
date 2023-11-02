<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\Thrift\FileMetaData;

final class Metadata
{
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

    /**
     * @return array<ColumnChunk>
     */
    public function columnChunks() : array
    {
        $chunks = [];

        foreach ($this->rowGroups->all() as $rowGroup) {
            foreach ($rowGroup->columnChunks() as $columnChunk) {
                $chunks[] = $columnChunk;
            }
        }

        return $chunks;
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

    public function toThrift() : FileMetaData
    {
        return new FileMetaData([
            'version' => $this->version,
            'schema' => $this->schema->toThrift(),
            'num_rows' => $this->rowGroups->rowsCount(),
            'row_groups' => $this->rowGroups->toThrift(),
            'created_by' => $this->createdBy,
        ]);
    }

    public function version() : int
    {
        return $this->version;
    }
}
