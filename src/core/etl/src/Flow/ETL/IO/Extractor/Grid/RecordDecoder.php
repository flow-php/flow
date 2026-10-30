<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\Grid;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function array_fill;
use function array_values;
use function count;

/**
 * Decodes the records of one file or stream: the header it resolves from the first record is that source's.
 */
final class RecordDecoder
{
    /**
     * @var null|list<string>
     */
    private ?array $headers = null;

    /**
     * @param null|list<string> $headers the names of a stream that carries no header record - a window cut past a
     *                                   file's first record; withHeader must be false
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly bool $withHeader,
        private readonly bool $emptyToNull,
        private readonly bool $dropExtraColumns = true,
        ?array $headers = null,
    ) {
        if ($headers !== null && $withHeader) {
            throw new InvalidArgumentException('Preset headers replace the header record, withHeader must be false');
        }

        $this->headers = $headers;
    }

    /**
     * Records before the header is resolved that hold no cell are skipped. A cell a record lacks is null; a present
     * '' cell is null under emptyToNull.
     *
     * @param list<array<array-key, mixed>> $batch
     *
     * @throws InvalidArgumentException a record with more cells than headers, unless extra columns are dropped
     *
     * @return list<array<string, mixed>>
     */
    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $record) {
            $cells = array_values($record);

            if ($this->headers === null) {
                if ($cells === []) {
                    continue;
                }

                $this->headers = (new HeaderNames())->of(
                    $this->withHeader ? $cells : array_fill(0, count($cells), null),
                );

                if ($this->withHeader) {
                    continue;
                }
            }

            if (!$this->dropExtraColumns && count($cells) > count($this->headers)) {
                throw InvalidArgumentException::because(
                    'Row has more columns (%d) than headers (%d)',
                    count($cells),
                    count($this->headers),
                );
            }

            $values = [];

            foreach ($this->headers as $index => $name) {
                // @mago-ignore analysis:mixed-assignment
                $cell = $cells[$index] ?? null;
                $values[$name] = $this->emptyToNull && $cell === '' ? null : $cell;
            }

            $decoded[] = $values;
        }

        return $decoded;
    }

    /**
     * @return null|list<string> null before the first decode() resolved them
     */
    public function headers(): ?array
    {
        return $this->headers;
    }
}
