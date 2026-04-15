<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake;

use Flow\PostgreSql\Client\RowMapper;

/**
 * Records every row it receives and returns it wrapped as an object so callers
 * can distinguish the chained output from the intermediate cast result.
 *
 * @implements RowMapper<RecordedResult>
 */
final class SpyRowMapper implements RowMapper
{
    /**
     * @var list<array<string, mixed>>
     */
    public array $receivedRows = [];

    public function map(array $row) : mixed
    {
        $this->receivedRows[] = $row;

        return new RecordedResult($row);
    }
}
