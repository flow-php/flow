<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;

use function array_combine;
use function array_slice;
use function count;
use function str_pad;

/**
 * @implements Encoder<array<array-key, mixed>>
 */
final class GoogleSheetEncoder implements Encoder
{
    /**
     * @var array<string>
     */
    private array $headers = [];

    private int $headersCount = 0;

    public function __construct(
        private readonly bool $withHeader = true,
        private readonly bool $dropExtraColumns = true,
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

                    /** @var array<string> $rowData */
                    $this->headers = $rowData;
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
