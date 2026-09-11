<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Type;

/**
 * @template T of mixed
 *
 * @implements Type<T>
 */
final class MixedType implements Type
{
    public function assert(mixed $value): mixed
    {
        // the mixed type accepts and returns anything: there is no narrower type to return
        // @mago-ignore analysis:mixed-return-statement
        return $value;
    }

    public function cast(mixed $value): mixed
    {
        // @mago-ignore analysis:mixed-return-statement
        return $value;
    }

    public function isValid(mixed $value): bool
    {
        return true;
    }

    public function normalize(): array
    {
        return [
            'type' => 'mixed',
        ];
    }

    public function toString(): string
    {
        return 'mixed';
    }
}
