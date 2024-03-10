<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_int, type_xml};
use PHPUnit\Framework\TestCase;

final class XMLTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_xml()->isEqual(type_xml())
        );
        self::assertFalse(
            type_xml()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_xml()->isValid(new \DOMDocument()));
        self::assertFalse(type_xml()->isValid('<xml></xml>'));
        self::assertFalse(type_xml()->isValid('2020-01-01'));
        self::assertFalse(type_xml()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'xml',
            type_xml()->toString()
        );
        self::assertSame(
            '?xml',
            type_xml(true)->toString()
        );
    }
}
