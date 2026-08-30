<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types\Tests\Unit;

use Flow\Bridge\Mago\Types\ElementShape;
use Flow\Bridge\Mago\Types\Tests\Mother\InvocationMother;
use Mago\Sdk\Analyzer\Type;
use Mago\Sdk\Analyzer\Type\ArrayItem;
use PHPUnit\Framework\TestCase;

final class ElementShapeTest extends TestCase
{
    public function test_a_plain_flow_type_derives_a_required_member(): void
    {
        $member = Type::int();

        $derived = (new ElementShape())->derive(InvocationMother::item('id', InvocationMother::flowType($member)));

        static::assertNotNull($derived);
        static::assertFalse($derived->optional);
        static::assertSame($member, $derived->type);
    }

    public function test_a_marker_with_literal_true_derives_an_optional_member(): void
    {
        $member = Type::string();

        $derived = (new ElementShape())->derive(InvocationMother::item('nick', InvocationMother::marker(
            $member,
            Type::true(),
        )));

        static::assertNotNull($derived);
        static::assertTrue($derived->optional);
        static::assertSame($member, $derived->type);
    }

    public function test_a_marker_without_flag_or_with_a_non_true_flag_stays_required(): void
    {
        $elementShape = new ElementShape();

        foreach ([null, Type::false(), Type::bool()] as $flag) {
            $derived = $elementShape->derive(InvocationMother::item('a', InvocationMother::marker(
                Type::string(),
                $flag,
            )));

            static::assertNotNull($derived);
            static::assertFalse($derived->optional);
        }
    }

    public function test_an_already_optional_item_stays_optional(): void
    {
        $item = InvocationMother::item('a', InvocationMother::flowType(Type::string()));

        $derived = (new ElementShape())->derive(new ArrayItem($item->key, true, $item->type));

        static::assertNotNull($derived);
        static::assertTrue($derived->optional);
    }

    public function test_a_value_that_is_not_a_named_object_refuses_to_derive(): void
    {
        static::assertNull((new ElementShape())->derive(InvocationMother::item('id', Type::string())));
    }
}
