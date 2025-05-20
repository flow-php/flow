<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\{Exception\InvalidArgumentException, Extractor, FlowContext};
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\ETL\Schema;
use Flow\Filesystem\Path;

final class CSVExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
    use PathFiltering;

    /**
     * @var null|int<1, max>
     */
    private ?int $charactersReadInLine = null;

    private bool $emptyToNull = true;

    private ?string $enclosure = null;

    private ?string $escape = null;

    private ?Schema $schema = null;

    private ?string $separator = null;

    private bool $withHeader = true;

    public function __construct(private readonly Path $path)
    {
        $this->resetLimit();
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {
            $option = csv_detect_separator($stream);

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;

            $headers = [];
            $headersCount = 0;

            foreach ($stream->readLines(length: $this->charactersReadInLine) as $csvLine) {
                /** @var non-empty-list<null|string> $rowData */
                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);
                $rowDataCount = \count($rowData);

                if ([] === $headers) {
                    if ($this->withHeader) {
                        /** @var array<string> $headers */
                        $headers = $this->mapHeaders($rowData);
                        $headersCount = $rowDataCount;

                        continue;
                    }

                    $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowData) - 1));
                    $headers = $this->mapHeaders($headers);
                    $headersCount = \count($headers);
                }

                // Expand columns to the size of the previous row
                for ($i = $rowDataCount; $i < $headersCount; $i++) {
                    $rowData[$i] = $this->emptyToNull ? null : '';
                }

                // Cut columns to the size of the header row
                if ($rowDataCount > $headersCount) {
                    $rowData = \array_slice($rowData, 0, $headersCount);
                }

                if ($this->emptyToNull) {
                    foreach ($rowData as $i => $data) {
                        if ($data === '') {
                            $rowData[$i] = null;
                        }
                    }
                }

                $row = \array_combine($headers, $rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $stream->path()->uri();
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $stream->path()->partitions(), $this->schema);
                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $context->streams()->closeStreams($this->path);

                    return;
                }
            }

            $stream->close();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    /**
     * @param int<1, max> $charactersReadInLine
     */
    public function withCharactersReadInLine(int $charactersReadInLine) : self
    {
        if ($charactersReadInLine < 1) {
            throw new InvalidArgumentException('Characters read in line must be greater than 0');
        }

        $this->charactersReadInLine = $charactersReadInLine;

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull) : self
    {
        $this->emptyToNull = $emptyToNull;

        return $this;
    }

    public function withEnclosure(string $enclosure) : self
    {
        $this->enclosure = $enclosure;

        return $this;
    }

    public function withEscape(string $escape) : self
    {
        $this->escape = $escape;

        return $this;
    }

    public function withHeader(bool $withHeader) : self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withSchema(Schema $schema) : self
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSeparator(string $separator) : self
    {
        $this->separator = $separator;

        return $this;
    }

    private function mapHeaders(array $headers) : array
    {
        $headers = \array_map(fn (string $header) : string => \trim($header), $headers);

        return \array_map(
            fn (string $header, int $index) : string => $header !== '' ? $header : 'e' . \str_pad(
                (string) $index,
                2,
                '0',
                STR_PAD_LEFT
            ),
            $headers,
            \array_keys($headers)
        );
    }
}
