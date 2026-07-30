<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

final readonly class RecordedQuery
{
    /**
     * @param list<mixed> $parameters values bound to $1, $2, ... placeholders
     * @param null|int $rowCount affected rows for writes, returned rows for reads, null when unknown (e.g. cursors)
     * @param string $connection name of the connection the query ran on
     * @param null|string $caller "file:line" of the first application frame that issued the query, null when undetected
     * @param bool $parametersTruncated parameters were dropped to bound memory, as opposed to the query having none
     * @param bool $statementTruncated the statement was cut to bound memory, so it is no longer runnable
     */
    public function __construct(
        public string $sql,
        public array $parameters,
        public float $durationMs,
        public ?int $rowCount,
        public bool $failed,
        public ?string $error,
        public string $connection = 'default',
        public ?string $caller = null,
        public bool $parametersTruncated = false,
        public bool $statementTruncated = false,
    ) {}
}
