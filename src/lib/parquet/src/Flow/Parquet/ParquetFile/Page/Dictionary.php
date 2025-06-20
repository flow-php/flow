<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page;

final class Dictionary
{
    /**
     * @param array<array-key, mixed> $values
     */
    public function __construct(public array $values)
    {
    }
}
