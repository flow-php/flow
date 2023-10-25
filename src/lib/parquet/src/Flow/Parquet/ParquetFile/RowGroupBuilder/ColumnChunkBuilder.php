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
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset) : array
    {
        $offset = $fileOffset;
        $columnChunkContainers = [];

        $pageContainer = (new DataPagesBuilder($this->data))->build($this->column, $this->dataConverter);

        $columnChunkContainers[] = $this->createColumnChunkContainer($pageContainer, $offset);
        $offset += $pageContainer->size();

        $this->data = [];

        return $columnChunkContainers;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     */
    private function createColumnChunkContainer(PageContainer $pageContainer, int $offset) : ColumnChunkContainer
    {
        return new ColumnChunkContainer(
            $pageContainer->pageHeaderBuffer . $pageContainer->pageDataBuffer,
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
                \strlen($pageContainer->pageDataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                \strlen($pageContainer->pageDataBuffer) + \strlen($pageContainer->pageHeaderBuffer),
                dictionaryPageOffset: null,
                dataPageOffset: $offset,
                indexPageOffset: null,
            )
        );
    }
}
