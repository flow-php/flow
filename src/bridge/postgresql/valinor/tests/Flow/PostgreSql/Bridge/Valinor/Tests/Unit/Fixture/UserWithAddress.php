<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture;

final readonly class UserWithAddress
{
    public function __construct(
        public int $id,
        public string $name,
        public Address $address,
    ) {
    }
}
