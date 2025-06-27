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

    private bool $removeBOM = true;

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
            $streamUri = $shouldPutInputIntoRows ? $stream->path()->uri() : null;
            $partitions = $stream->path()->partitions();

            $csvLineReader = new CSVLineReader($enclosure, $this->charactersReadInLine, $this->removeBOM);
            $rowNormalizer = new CSVRowNormalizer($this->emptyToNull);

            foreach ($csvLineReader->readLines($stream) as $csvLine) {
                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);
                $rowDataCount = \count($rowData);

                if ([] === $headers) {
                    if ($this->withHeader) {
                        /** @var array<string> $headers */
                        $headers = $this->mapHeaders($rowData);
                        $headersCount = $rowDataCount;

                        continue;
                    }

                    $headers = $this->generateAutoHeaders($rowDataCount);
                    $headersCount = $rowDataCount;
                }

                $rowData = $rowNormalizer->normalize($rowData, $headersCount);

                $row = \array_combine($headers, $rowData);

                if ($streamUri !== null) {
                    $row['_input_file_uri'] = $streamUri;
                }

                $signal = yield array_to_rows($row, $context->entryFactory(), $partitions, $this->schema);
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

    public function withBOMRemoval(bool $removeBOM) : self
    {
        $this->removeBOM = $removeBOM;

        return $this;
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

    /**
     * @param Schema $schema
     */
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

    /**
     * @return array<int, string>
     */
    private function generateAutoHeaders(int $count) : array
    {
        $headers = [];

        for ($i = 0; $i < $count; $i++) {
            $headers[$i] = 'e' . \str_pad((string) $i, 2, '0', STR_PAD_LEFT);
        }

        return $headers;
    }

    /**
     * @param array<array-key, mixed> $headers
     *
     * @return array<int, string>
     */
    private function mapHeaders(array $headers) : array
    {
        $headers = \array_map(
            static fn (mixed $header) : string => \trim(
                match (true) {
                    \is_string($header) => $header,
                    \is_numeric($header) => (string) $header,
                    $header === null => '',
                    default => \is_scalar($header) ? (string) $header : '',
                }
            ),
            $headers
        );

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
