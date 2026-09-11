<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class RewrittenSql
{
    /**
     * @param int<0, max> $parameters how many placeholders the statement carries, never their values
     */
    public function __construct(
        public string $sql,
        public int $parameters,
    ) {}
}
