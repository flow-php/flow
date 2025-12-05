<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Type;

/**
 * @implements Type<mixed>
 */
final class MixedType implements Type
{
    #[\Override]
    public function assert(mixed $value): mixed
    {
        return $value;
    }

    #[\Override]
    public function cast(mixed $value): mixed
    {
        return $value;
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return true;
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'mixed',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'mixed';
    }
}
