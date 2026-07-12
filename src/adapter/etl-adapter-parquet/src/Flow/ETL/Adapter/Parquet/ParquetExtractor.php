<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\Reader;
use Generator;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Types\DSL\type_string;

final class ParquetExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN;

    /**
     * @var array<string>
     */
    private array $columns = [];

    private ?int $offset = null;

    private Options $options;

    private SchemaConverter $schemaConverter;

    private ValueHydrator $valueHydrator;

    /**
     * @param Path $path
     */
    public function __construct(
        private readonly Path $path,
    ) {
        $this->resetLimit();
        $this->schemaConverter = new SchemaConverter();
        $this->valueHydrator = new ValueHydrator();
        $this->options = Options::default();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        $fileOffset = $this->offset ?? 0;

        foreach ($this->readers($context) as $fileData) {
            $fileRows = $fileData['file']->metadata()->rowsNumber();

            if ($fileOffset > $fileRows) {
                $fileData['stream']->close();
                $fileOffset -= $fileRows;

                continue;
            }

            $flowSchema = $this->schemaConverter->toFlow($fileData['file']->schema());

            if (count($this->columns)) {
                $flowSchema = $flowSchema->keep(...$this->columns);
            }

            $uri = $fileData['stream']->path()->uri();

            foreach ($fileData['file']->values($this->columns, $this->limit(), $fileOffset) as $row) {
                $entries = [];

                if ($shouldPutInputIntoRows) {
                    $entries[] = $context->entryFactory()->createAs('_input_file_uri', $uri, type_string());
                }

                // @mago-ignore analysis:mixed-assignment
                foreach ($row as $entryName => $entryValue) {
                    $definition = $flowSchema->get(ref($entryName));

                    $entries[] = $context->entryFactory()->instantiate(
                        $entryName,
                        $this->valueHydrator->hydrate($entryValue, $definition),
                        $definition,
                    );
                }

                $signal = yield rows(row(...$entries));

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

                    return;
                }
            }

            $fileOffset = max($fileOffset - $fileRows, 0);
            $fileData['stream']->close();
        }
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withByteOrder(ByteOrder $byteOrder): self
    {
        $this->byteOrder = $byteOrder;

        return $this;
    }

    /**
     * @param array<string> $columns
     */
    public function withColumns(array $columns): self
    {
        $this->columns = $columns;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;

        return $this;
    }

    /**
     * @return \Generator<int, array{file: ParquetFile, stream: SourceStream}>
     */
    private function readers(FlowContext $context): Generator
    {
        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            yield [
                'file' => (new Reader(byteOrder: $this->byteOrder, options: $this->options))->readStream($stream),
                'stream' => $stream,
            ];
        }
    }
}
