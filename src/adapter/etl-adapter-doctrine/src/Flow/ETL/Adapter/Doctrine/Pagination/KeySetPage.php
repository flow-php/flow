<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

use Flow\ETL\Exception\RuntimeException;

use function array_map;
use function array_slice;
use function count;
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
     * @param list<string> $aliases the key_<sha1> alias of every key, by key index
     * @param list<array<string, mixed>> $fetched at most $size + 1 rows, the last one being the lookahead
     *
     * @throws RuntimeException
     */
    public static function of(KeySet $keySet, array $aliases, array $fetched, int $size, ?KeyValues $expectedHead): self
    {
        $keys = array_map(static fn(array $row): KeyValues => KeyValues::of($keySet, $aliases, $row), $fetched);

        if ($expectedHead !== null && ($keys === [] || !$keys[0]->equals($expectedHead))) {
            throw new RuntimeException(sprintf(
                'Keyset pagination expected the next page to start at the key %s, but it did not. Either the database '
                . 'compares two different key values as equal (a case- or accent-insensitive collation, numeric scale '
                . 'such as 1.0 and 1.00), or rows changed between pages; use a unique key with an exact comparison, or '
                . 'read inside a transaction',
                $expectedHead->describe(),
            ));
        }

        for ($index = 1, $count = count($keys); $index < $count; $index++) {
            if ($keys[$index]->equals($keys[$index - 1])) {
                throw new RuntimeException(sprintf(
                    'Keyset pagination requires unique keys, but two rows share the key %s; add a unique column (for '
                    . 'example the primary key) to the key set as its least significant key',
                    $keys[$index]->describe(),
                ));
            }
        }

        return count($fetched) <= $size
            ? new self($fetched, null, null)
            : new self(array_slice($fetched, 0, $size), $keys[$size - 1], $keys[$size]);
    }

    public function isLast(): bool
    {
        return $this->cursor === null;
    }
}
