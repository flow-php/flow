<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnChunkBuilder
{
    private array $data = [];

    public function __construct(private readonly FlatColumn $column, private readonly DataConverter $dataConverter)
    {
    }

    public function addRow(mixed $data) : void
    {
        $this->data[] = $data;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     *
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset) : array
    {
        $offset = $fileOffset;
        $columnChunkContainers = [];
        $previousChunkData = null;

        foreach (\array_chunk($this->data, 1000) as $dataChunk) {
            $pageContainer = (new DataPagesBuilder($dataChunk))->build($this->column, $this->dataConverter);

            $columnChunkContainers[] = new ColumnChunkContainer(
                $pageContainer->pageHeaderBuffer . $pageContainer->dataBuffer,
                new ColumnChunk(
                    $this->column->type(),
                    Compressions::UNCOMPRESSED,
                    /** @phpstan-ignore-next-line */
                    $pageContainer->pageHeader->dataValuesCount(),
                    $offset,
                    $this->column->path(),
                    [
                        Encodings::PLAIN,
                    ],
                    \strlen($pageContainer->dataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                    \strlen($pageContainer->dataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                    dictionaryPageOffset: null,
                    dataPageOffset: $offset,
                    indexPageOffset: null,
                )
            );

            $offset += \strlen($pageContainer->pageHeaderBuffer) + \strlen($pageContainer->dataBuffer);
        }

        $this->data = [];

        return $columnChunkContainers;
    }
}
