<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

final readonly class CSVRowNormalizer
{
    public function __construct(private bool $emptyToNull = true)
    {
    }

    /**
     * Normalize CSV row data to match the expected number of headers.
     * - Expands rows with fewer columns by adding fill values
     * - Truncates rows with more columns to match header count
     * - Converts empty strings to null if emptyToNull is enabled.
     *
     * @param array<int, null|string> $rowData
     * @param int $headersCount
     *
     * @return array<int, null|string>
     */
    public function normalize(array $rowData, int $headersCount) : array
    {
        $rowDataCount = \count($rowData);

        if ($rowDataCount < $headersCount) {
            $fillValue = $this->emptyToNull ? null : '';

            for ($i = $rowDataCount; $i < $headersCount; $i++) {
                $rowData[$i] = $fillValue;
            }
        } elseif ($rowDataCount > $headersCount) {
            $rowData = \array_slice($rowData, 0, $headersCount, true);
        }

        if ($this->emptyToNull) {
            foreach ($rowData as $i => $data) {
                if ($data === '') {
                    $rowData[$i] = null;
                }
            }
        }

        return $rowData;
    }
}
