<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture;

final readonly class SimpleDto
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {
    }
}
