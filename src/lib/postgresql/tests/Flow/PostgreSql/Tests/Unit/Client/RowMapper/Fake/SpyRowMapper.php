<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake;

use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\RowMapper\Context;

/**
 * Records every row and Context it receives and returns the row wrapped as an
 * object so callers can distinguish the chained output from the intermediate
 * cast result.
 *
 * @implements RowMapper<RecordedResult>
 */
final class SpyRowMapper implements RowMapper
{
    /**
     * @var list<Context>
     */
    public array $receivedContexts = [];

    /**
     * @var list<array<string, mixed>>
     */
    public array $receivedRows = [];

    public function map(array $row, Context $context): mixed
    {
        $this->receivedRows[] = $row;
        $this->receivedContexts[] = $context;

        return new RecordedResult($row);
    }
}
