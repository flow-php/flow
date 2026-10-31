<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\Grid;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function array_fill;
use function array_values;
use function count;
use function sprintf;

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
        private readonly bool $missingColumnsAsNull = false,
        ?array $headers = null,
    ) {
        if ($headers !== null && $withHeader) {
            throw new InvalidArgumentException('Preset headers replace the header record, withHeader must be false');
        }

        $this->headers = $headers;
    }

    /**
     * An extra empty cell ('' or null) loses nothing, so only a non-empty one makes a record corrupted. A missing cell
     * makes it corrupted unless missingColumnsAsNull; a present '' cell is null under emptyToNull.
     *
     * @param array<array-key, mixed> $cells
     *
     * @return null|array<string, mixed>|string null for a record that is no row (the header, an empty record), why
     *                                          the record is corrupted, or the row's values
     */
    public function decode(array $cells): array|string|null
    {
        $cells = array_values($cells);

        if ($cells === [] || $cells === [null]) {
            return null;
        }

        if ($this->headers === null) {
            $this->headers = (new HeaderNames())->of($this->withHeader ? $cells : array_fill(0, count($cells), null));

            if ($this->withHeader) {
                return null;
            }
        }

        $headers = count($this->headers);
        $fields = count($cells);

        $corrupted = $fields < $headers && !$this->missingColumnsAsNull;

        for ($index = $headers; !$corrupted && $index < $fields; $index++) {
            $corrupted = $cells[$index] !== null && $cells[$index] !== '';
        }

        if ($corrupted) {
            return sprintf('%d fields, the header has %d', $fields, $headers);
        }

        $values = [];

        foreach ($this->headers as $index => $name) {
            // @mago-ignore analysis:mixed-assignment
            $cell = $cells[$index] ?? null;
            $values[$name] = $this->emptyToNull && $cell === '' ? null : $cell;
        }

        return $values;
    }

    /**
     * @return null|list<string> null before the first decode() resolved them
     */
    public function headers(): ?array
    {
        return $this->headers;
    }
}
