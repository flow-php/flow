<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Pagination;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\PostgreSql\AST\Transformers\KeysetColumn;

use function array_map;
use function array_values;
use function count;

final readonly class KeySet
{
    /**
     * @var array<Key>
     */
    public array $keys;

    public function __construct(Key ...$keys)
    {
        if (count($keys) === 0) {
            throw new InvalidArgumentException('KeySet requires at least one key');
        }

        $this->keys = $keys;
    }

    /**
     * @return list<KeysetColumn>
     */
    public function toKeysetColumns(): array
    {
        return array_values(array_map(static fn(Key $key): KeysetColumn => $key->toKeysetColumn(), $this->keys));
    }
}
