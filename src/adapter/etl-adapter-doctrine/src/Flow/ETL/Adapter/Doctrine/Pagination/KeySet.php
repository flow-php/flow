<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

use Flow\ETL\Exception\InvalidArgumentException;

use function array_reverse;
use function array_values;

final readonly class KeySet
{
    /**
     * @var non-empty-list<Key>
     */
    public array $keys;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(Key ...$keys)
    {
        $reversed = array_values(array_reverse($keys));

        if ($reversed === []) {
            throw new InvalidArgumentException('KeySet requires at least one key');
        }

        $this->keys = $reversed;
    }
}
