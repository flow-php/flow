<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Types\Type;
use Throwable;

/**
 * @implements Type<mixed>
 */
final readonly class ThrowingType implements Type
{
    public function __construct(
        private Throwable $throwable,
    ) {}

    public function assert(mixed $value): mixed
    {
        throw $this->throwable;
    }

    public function cast(mixed $value): mixed
    {
        throw $this->throwable;
    }

    public function isValid(mixed $value): bool
    {
        return false;
    }

    public function normalize(): array
    {
        return ['type' => 'throwing'];
    }

    public function toString(): string
    {
        return 'throwing';
    }
}
