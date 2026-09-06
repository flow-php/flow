<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;

use function array_combine;
use function array_keys;
use function array_map;
use function array_slice;
use function count;
use function str_pad;

/**
 * @implements Encoder<array<array-key, mixed>>
 */
final class GoogleSheetEncoder implements Encoder
{
    /**
     * @var list<string>
     */
    private array $headers = [];

    private int $headersCount = 0;

    public function __construct(
        private readonly bool $withHeader = true,
        private readonly bool $dropExtraColumns = true,
        private readonly bool $emptyToNull = true,
    ) {}

    public function decode(array $batch): array
    {
        $maps = [];

        foreach ($batch as $rowData) {
            $rowDataCount = count($rowData);

            if ($this->withHeader) {
                if ([] === $this->headers) {
                    if ([] === $rowData) {
                        continue;
                    }

                    /** @var list<bool|float|int|string> $rowData */
                    $this->headers = array_map(
                        static fn(bool|float|int|string $header): string => (string) $header,
                        $rowData,
                    );
                    $this->headersCount = $rowDataCount;

                    continue;
                }
            } elseif (0 === $this->headersCount) {
                $this->headersCount = $rowDataCount;
            }

            for ($i = $rowDataCount; $i < $this->headersCount; $i++) {
                $rowData[$i] = null;
            }

            if ($rowDataCount > $this->headersCount) {
                if (!$this->dropExtraColumns) {
                    throw InvalidArgumentException::because(
                        'Row has more columns (%d) than headers (%d)',
                        $rowDataCount,
                        $this->headersCount,
                    );
                }

                $rowData = array_slice($rowData, 0, $this->headersCount);
            }

            if ($this->emptyToNull) {
                foreach (array_keys($rowData) as $i) {
                    if ($rowData[$i] === '') {
                        $rowData[$i] = null;
                    }
                }
            }

            $maps[] = new RawRowValues(array_combine(
                $this->withHeader ? $this->headers : $this->generateAutoHeaders(count($rowData)),
                $rowData,
            ));
        }

        return $maps;
    }

    public function encode(array $batch): array
    {
        throw new RuntimeException(
            'Google Sheet adapter is read-only, encoding rows back to sheet values is not supported',
        );
    }

    /**
     * The header the first decoded row consumed, or the generated `e00`... names under withHeader(false);
     * [] before any row.
     *
     * @return list<string>
     */
    public function headers(): array
    {
        return $this->withHeader ? $this->headers : $this->generateAutoHeaders($this->headersCount);
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
}
