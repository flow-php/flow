<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function Flow\Types\DSL\get_type;
use function Flow\Types\DSL\type_uuid;

final class InstanceOfTypeNarrowerTest extends TestCase
{
    public function test_narrow_with_custom_object_returns_detected_type(): void
    {
        $object = new class {
            public string $property = 'value';
        };

        $narrower = new InstanceOfTypeNarrower();

        static::assertEquals(get_type($object), $narrower->narrow($object));
    }

    public function test_narrow_with_non_object_returns_detected_type(): void
    {
        $narrower = new InstanceOfTypeNarrower();

        static::assertEquals(get_type('string'), $narrower->narrow('string'));
        static::assertEquals(get_type(123), $narrower->narrow(123));
        static::assertEquals(get_type(12.5), $narrower->narrow(12.5));
        static::assertEquals(get_type(true), $narrower->narrow(true));
        static::assertEquals(get_type([]), $narrower->narrow([]));
        static::assertEquals(get_type(null), $narrower->narrow(null));
    }

    public function test_narrow_with_ramsey_uuid_returns_uuid_type(): void
    {
        $uuid = RamseyUuid::uuid4();
        $narrower = new InstanceOfTypeNarrower();

        static::assertEquals(type_uuid(), $narrower->narrow($uuid));
    }

    public function test_narrow_with_symfony_uuid_returns_uuid_type(): void
    {
        $uuid = SymfonyUuid::v4();
        $narrower = new InstanceOfTypeNarrower();

        static::assertEquals(type_uuid(), $narrower->narrow($uuid));
    }

    public function test_narrow_with_unknown_object_returns_detected_type(): void
    {
        $object = new \stdClass();
        $narrower = new InstanceOfTypeNarrower();

        static::assertEquals(get_type($object), $narrower->narrow($object));
    }
}
