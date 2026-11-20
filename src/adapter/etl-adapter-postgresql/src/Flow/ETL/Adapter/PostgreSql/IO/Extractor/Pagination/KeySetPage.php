<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\IO\Extractor\Pagination;

use Flow\ETL\Foundation\Exception\RuntimeException;

use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function is_scalar;
use function sprintf;

final readonly class KeySetPage
{
    /**
     * @param list<array<string, mixed>> $rows
     */
    private function __construct(
        public array $rows,
        public ?KeyValues $cursor,
        public ?KeyValues $lookahead,
    ) {}

    /**
     * @param list<array<string, mixed>> $fetched at most $size + 1 rows, the last one being the lookahead
     *
     * @throws RuntimeException
     */
    public static function of(KeySet $keySet, array $fetched, int $size, ?KeyValues $expectedHead): self
    {
        $names = array_map(static fn(Key $key): string => $key->name(), $keySet->keys);
        $tieIndex = null;

        foreach ($fetched as $index => $row) {
            $tie = $index > 0;

            foreach ($names as $name) {
                if (!array_key_exists($name, $row) || !is_scalar($row[$name])) {
                    KeyValues::of($keySet, $row);
                }

                $tie = $tie && $row[$name] === $fetched[$index - 1][$name];
            }

            if ($tie && $tieIndex === null) {
                $tieIndex = $index;
            }
        }

        if (
            $expectedHead !== null
            && ($fetched === [] || !KeyValues::of($keySet, $fetched[0])->equals($expectedHead))
        ) {
            throw new RuntimeException(sprintf(
                'Keyset pagination expected the next page to start at the key %s, but it did not. Either the database '
                . 'compares two different key values as equal (a case- or accent-insensitive collation, numeric scale '
                . 'such as 1.0 and 1.00), or rows changed between pages; use a unique key with an exact comparison, or '
                . 'read inside a transaction',
                $expectedHead->describe(),
            ));
        }

        if ($tieIndex !== null) {
            throw new RuntimeException(sprintf(
                'Keyset pagination requires unique keys, but two rows share the key %s; add a unique column (for '
                . 'example the primary key) to the key set as its least significant key',
                KeyValues::of($keySet, $fetched[$tieIndex])->describe(),
            ));
        }

        return count($fetched) <= $size
            ? new self($fetched, null, null)
            : new self(
                array_slice($fetched, 0, $size),
                KeyValues::of($keySet, $fetched[$size - 1]),
                KeyValues::of($keySet, $fetched[$size]),
            );
    }

    public function isLast(): bool
    {
        return $this->cursor === null;
    }
}
