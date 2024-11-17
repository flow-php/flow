<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\ETL\Row\Schema;
use Flow\ETL\{Exception\InvalidArgumentException, Extractor, FlowContext};
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

            $option = \Flow\ETL\Adapter\CSV\csv_detect_separator($stream);

            $separator = $this->separator ?? $option->separator;
            $enclosure = $this->enclosure ?? $option->enclosure;
            $escape = $this->escape ?? $option->escape;

            $headers = [];

            foreach ($stream->readLines(length: $this->charactersReadInLine) as $csvLine) {
                if ($this->withHeader && \count($headers) === 0) {
                    /** @var array<string> $headers */
                    $headers = \str_getcsv($csvLine, $separator, $enclosure, $escape);

                    continue;
                }

                /** @var non-empty-list<null|string> $rowData */
                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);

                if (!\count($headers)) {
                    $headers = \array_map(fn (int $e) : string => 'e' . \str_pad((string) $e, 2, '0', STR_PAD_LEFT), \range(0, \count($rowData) - 1));
                }

                $headers = \array_map(fn (string $header) : string => \trim($header), $headers);
                $headers = \array_map(fn (string $header, int $index) : string => $header !== '' ? $header : 'e' . \str_pad((string) $index, 2, '0', STR_PAD_LEFT), $headers, \array_keys($headers));

                if (\count($headers) > \count($rowData)) {
                    \array_push(
                        $rowData,
                        ...\array_map(
                            fn (int $i) => ($this->emptyToNull ? null : ''),
                            \range(1, \count($headers) - \count($rowData))
                        )
                    );
                }

                if (\count($rowData) > \count($headers)) {
                    $rowData = \array_slice($rowData, 0, \count($headers));
                }

                if ($this->emptyToNull) {
                    foreach ($rowData as $i => $data) {
                        if ($data === '') {
                            $rowData[$i] = null;
                        }
                    }
                }

                if (\count($headers) !== \count($rowData)) {
                    continue;
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

                $rowData = \str_getcsv($csvLine, $separator, $enclosure, $escape);
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
}
