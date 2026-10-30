<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\IO\Extractor\Grid\RecordDecoder;

use function array_map;
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
    ) {
        $this->records = new RecordDecoder($withHeader, $emptyToNull, headers: $headers);
    }

    /**
     * @param list<string> $batch
     *
     * @return list<array<string, ?string>>
     */
    public function decode(array $batch): array
    {
        /** @var list<array<string, ?string>> */
        return $this->records->decode(array_map(fn(string $line): array => str_getcsv(
            $line,
            $this->separator,
            $this->enclosure,
            $this->escape,
        ), $batch));
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
