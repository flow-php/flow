<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Pagination;

final readonly class KeySet
{
    /**
     * @var array<Key>
     */
    public array $keys;

    public function __construct(Key ...$keys)
    {
        $this->keys = \array_reverse($keys);
    }
}
