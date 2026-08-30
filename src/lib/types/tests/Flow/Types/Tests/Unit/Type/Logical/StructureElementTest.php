<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use Flow\Types\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class StructureElementTest extends TestCase
{
    public function test_rejects_empty_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure element name cannot be empty.');

        structure_element('', type_string());
    }

    public function test_string_name(): void
    {
        $element = structure_element('id', type_integer());

        static::assertSame('id', $element->name);
        static::assertEquals(type_integer(), $element->type);
        static::assertFalse($element->optional);
    }

    public function test_integer_name(): void
    {
        $element = structure_element(0, type_string(), optional: true);

        static::assertSame(0, $element->name);
        static::assertTrue($element->optional);
    }
}
