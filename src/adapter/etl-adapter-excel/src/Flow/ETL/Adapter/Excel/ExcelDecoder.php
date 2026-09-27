<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use Flow\ETL\Row\RawRowValues;

use function array_map;
use function array_values;
use function count;
use function is_scalar;
use function str_pad;

final class ExcelDecoder
{
    /**
     * @var null|list<string>
     */
    private ?array $headers = null;

    public function __construct(
        private readonly bool $withHeader = true,
        private readonly bool $convertEmptyToNull = true,
    ) {}

    /**
     * @param list<array<int, mixed>> $batch
     *
     * @return list<RawRowValues>
     */
    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $cells) {
            if ($this->headers === null) {
                if ($this->withHeader) {
                    $this->headers = $this->mapHeaders($cells);

                    continue;
                }

                $this->headers = $this->generateAutoHeaders(count($cells));
            }

            $values = [];

            foreach ($this->headers as $index => $name) {
                // @mago-ignore analysis:mixed-assignment
                $cell = $cells[$index] ?? null;
                $values[$name] = $this->convertEmptyToNull && '' === $cell ? null : $cell;
            }

            $decoded[] = new RawRowValues($values);
        }

        return $decoded;
    }

    /**
     * The names decode() resolved from the first row it saw; null before the first decode().
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
     * @param array<int, mixed> $cells
     *
     * @return list<string>
     */
    private function mapHeaders(array $cells): array
    {
        return array_values(array_map(static fn(mixed $header): string => is_scalar($header)
            ? (string) $header
            : '', $cells));
    }
}
