<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Native;

use function Flow\ETL\DSL\{type_float, type_resource};
use function Flow\ETL\DSL\{type_map, type_string};
use Flow\ETL\Tests\FlowTestCase;

final class ResourceTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            (type_resource(false))->isEqual(type_resource(false))
        );
        self::assertFalse(
            (type_resource(false))->isEqual(type_map(type_string(), type_float()))
        );
        self::assertFalse(
            (type_resource(false))->isEqual(type_float())
        );
        self::assertFalse(
            (type_resource(false))->isEqual(type_resource())
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'resource',
            (type_resource(false))->toString()
        );
        self::assertSame(
            '?resource',
            (type_resource())->toString()
        );
    }

    public function test_valid() : void
    {
        $handle = \fopen('php://temp/max', 'r+b');
        self::assertTrue(
            (type_resource(false))->isValid($handle)
        );
        \fclose($handle);
        self::assertTrue(
            type_resource(true)->isValid(null)
        );
        self::assertFalse(
            (type_resource(false))->isValid('one')
        );
        self::assertFalse(
            (type_resource(false))->isValid([1, 2])
        );
        self::assertFalse(
            (type_resource(false))->isValid(123)
        );
    }
}
