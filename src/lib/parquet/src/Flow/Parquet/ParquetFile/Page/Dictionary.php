<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page;

final class Dictionary
{
    public function __construct(public array $values)
    {
    }
}
