<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

/**
 * Parameters already in PostgreSQL's text form - each what a ValueConverter's toDatabase() returns - that
 * Client::execute() sends as they are, without running a converter per value.
 */
final readonly class ConvertedParameters
{
    /**
     * @param list<null|string> $values bound by position to $1, $2, ... placeholders
     */
    public function __construct(
        public array $values,
    ) {}
}
