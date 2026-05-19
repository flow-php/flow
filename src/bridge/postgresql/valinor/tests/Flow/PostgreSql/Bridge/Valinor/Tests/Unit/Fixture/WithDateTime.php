<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture;

use DateTimeImmutable;

final readonly class WithDateTime
{
    public function __construct(
        public int $id,
        public DateTimeImmutable $createdAt,
    ) {}
}
