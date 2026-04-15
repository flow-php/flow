<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake;

final readonly class RecordedResult
{
    /**
     * @param array<string, mixed> $row
     */
    public function __construct(public array $row)
    {
    }
}
