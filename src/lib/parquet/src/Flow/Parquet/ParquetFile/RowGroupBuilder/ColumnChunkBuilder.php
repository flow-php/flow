<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    private array $data = [];

    public function __construct(private readonly FlatColumn $column)
    {
    }

    public function addRow(mixed $data) : void
    {
        $this->data[] = $data;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    public function flush(int $fileOffset) : ColumnChunkContainer
    {
        $pageContainer = (new DataPagesBuilder($this->data))->build($this->column);

        $this->data = [];

        return new ColumnChunkContainer(
            $pageContainer->pageHeaderBuffer . $pageContainer->dataBuffer,
            new ColumnChunk(
                $this->column->type(),
                Compressions::UNCOMPRESSED,
                /** @phpstan-ignore-next-line */
                $pageContainer->pageHeader->dataValuesCount(),
                $fileOffset,
                $this->column->path(),
                [
                    Encodings::PLAIN,
                ],
                \strlen($pageContainer->dataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                \strlen($pageContainer->dataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                dictionaryPageOffset: null,
                dataPageOffset: $fileOffset,
                indexPageOffset: null,
            )
        );
    }
}
