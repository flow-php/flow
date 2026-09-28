<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function array_combine;
use function array_keys;
use function array_map;
use function array_values;
use function count;
use function is_numeric;
use function is_scalar;
use function is_string;
use function str_getcsv;
use function str_pad;
use function trim;

final class CSVDecoder
{
    private readonly CSVRowNormalizer $rowNormalizer;

    /**
     * @var null|list<string>
     */
    private ?array $headers = null;

    public function __construct(
        private readonly bool $withHeader = true,
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\',
        bool $emptyToNull = true,
    ) {
        $this->rowNormalizer = new CSVRowNormalizer($emptyToNull);
    }

    /**
     * @param list<string> $batch
     *
     * @return list<array<array-key, ?string>>
     */
    public function decode(array $batch): array
    {
        $maps = [];

        foreach ($batch as $line) {
            /** @var list<null|string> $fields */
            $fields = str_getcsv($line, $this->separator, $this->enclosure, $this->escape);

            if ($this->headers === null) {
                if ($this->withHeader) {
                    $this->headers = $this->mapHeaders($fields);

                    continue;
                }

                $this->headers = $this->generateAutoHeaders(count($fields));
            }

            $maps[] = array_combine($this->headers, $this->rowNormalizer->normalize($fields, count($this->headers)));
        }

        return $maps;
    }

    /**
     * Null before the first decoded line: a 0-byte source resolves no header, which is not the same as a
     * header of zero columns.
     *
     * @return null|list<string>
     */
    public function headers(): ?array
    {
        return $this->headers;
    }

    /**
     * @return list<string>
     */
    private function generateAutoHeaders(int $count): array
    {
        $headers = [];

        for ($i = 0; $i < $count; $i++) {
            $headers[] = 'e' . str_pad((string) $i, 2, '0', STR_PAD_LEFT);
        }

        return $headers;
    }

    /**
     * @param array<array-key, mixed> $headers
     *
     * @return list<string>
     */
    private function mapHeaders(array $headers): array
    {
        $headers = array_map(static fn(mixed $header): string => trim(match (true) {
            is_string($header) => $header,
            is_numeric($header) => (string) $header,
            $header === null => '',
            default => is_scalar($header) ? (string) $header : '',
        }), $headers);

        return array_values(array_map(
            static fn(string $header, int|string $index): string => $header !== ''
                ? $header
                : 'e' . str_pad((string) $index, 2, '0', STR_PAD_LEFT),
            $headers,
            array_keys($headers),
        ));
    }
}
