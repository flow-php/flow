<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture;

final readonly class Address
{
    public function __construct(
        public string $street,
        public string $city,
    ) {
    }
}
