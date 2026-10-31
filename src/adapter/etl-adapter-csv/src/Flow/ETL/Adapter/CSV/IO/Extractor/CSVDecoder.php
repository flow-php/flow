<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\IO\Extractor\Grid\RecordDecoder;

use function str_getcsv;

final class CSVDecoder
{
    private readonly RecordDecoder $records;

    /**
     * @param null|list<string> $headers the names of a stream that carries no header record; withHeader must be false
     */
    public function __construct(
        bool $withHeader = true,
        private string $separator = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
        bool $emptyToNull = true,
        ?array $headers = null,
        bool $missingColumnsAsNull = false,
    ) {
        $this->records = new RecordDecoder($withHeader, $emptyToNull, $missingColumnsAsNull, $headers);
    }

    /**
     * @return list<?string>
     */
    public function cells(string $line): array
    {
        /** @var list<?string> */
        return str_getcsv($line, $this->separator, $this->enclosure, $this->escape);
    }

    /**
     * @param list<?string> $cells
     *
     * @return null|array<string, ?string>|string see RecordDecoder::decode()
     */
    public function decode(array $cells): array|string|null
    {
        /** @var null|array<string, ?string>|string */
        return $this->records->decode($cells);
    }

    /**
     * Null before the first decoded line: a 0-byte source resolves no header, which is not the same as a
     * header of zero columns.
     *
     * @return null|list<string>
     */
    public function headers(): ?array
    {
        return $this->records->headers();
    }
}
