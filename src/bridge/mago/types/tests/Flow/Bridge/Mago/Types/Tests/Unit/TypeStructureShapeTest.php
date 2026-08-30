<?php

declare(strict_types=1);

namespace Flow\Bridge\Mago\Types\Tests\Unit;

use Flow\Bridge\Mago\Types\Tests\Context\DerivedShapeContext;
use Flow\Bridge\Mago\Types\Tests\Mother\InvocationMother;
use Flow\Bridge\Mago\Types\TypeStructureShape;
use Mago\Sdk\Analyzer\Type;
use PHPUnit\Framework\TestCase;

final class TypeStructureShapeTest extends TestCase
{
    public function test_plain_map_derives_required_keys(): void
    {
        $id = Type::int();
        $name = Type::string();

        $shape = DerivedShapeContext::shape((new TypeStructureShape())->derive(InvocationMother::typeStructure(InvocationMother::sealedMap(
            InvocationMother::item('id', InvocationMother::flowType($id)),
            InvocationMother::item('name', InvocationMother::flowType($name)),
        ))));

        static::assertSame(['id', 'name'], array_keys($shape));
        static::assertSame([false, $id], $shape['id']);
        static::assertSame([false, $name], $shape['name']);
    }

    public function test_marker_with_literal_true_derives_an_optional_key(): void
    {
        $nick = Type::string();

        $shape = DerivedShapeContext::shape((new TypeStructureShape())->derive(InvocationMother::typeStructure(InvocationMother::sealedMap(
            InvocationMother::item('id', InvocationMother::flowType(Type::int())),
            InvocationMother::item('nick', InvocationMother::marker($nick, Type::true())),
        ))));

        static::assertSame([true, $nick], $shape['nick']);
        static::assertFalse($shape['id'][0]);
    }

    public function test_marker_without_flag_or_with_false_stays_required(): void
    {
        $shape = DerivedShapeContext::shape((new TypeStructureShape())->derive(InvocationMother::typeStructure(InvocationMother::sealedMap(
            InvocationMother::item('a', InvocationMother::marker(Type::string(), null)),
            InvocationMother::item('b', InvocationMother::marker(Type::string(), Type::false())),
            InvocationMother::item('c', InvocationMother::marker(Type::string(), Type::bool())),
        ))));

        static::assertSame([false, false, false], [$shape['a'][0], $shape['b'][0], $shape['c'][0]]);
    }

    public function test_literal_allow_extra_unseals_the_shape(): void
    {
        $derived = (new TypeStructureShape())->derive(InvocationMother::typeStructure(
            InvocationMother::sealedMap(InvocationMother::item('id', InvocationMother::flowType(Type::int()))),
            Type::true(),
        ));

        $keyedArray = DerivedShapeContext::keyedArray($derived);

        static::assertNotNull($keyedArray->keyType);
        static::assertNotNull($keyedArray->valueType);
    }

    public function test_non_literal_allow_extra_refuses_to_derive(): void
    {
        static::assertNull((new TypeStructureShape())->derive(InvocationMother::typeStructure(
            InvocationMother::sealedMap(InvocationMother::item('id', InvocationMother::flowType(Type::int()))),
            Type::bool(),
        )));
    }

    public function test_unsealed_elements_refuse_to_derive(): void
    {
        static::assertNull((new TypeStructureShape())->derive(InvocationMother::typeStructure(Type::array(
            Type::string(),
            Type::mixed(),
        ))));
    }

    public function test_a_value_that_is_not_a_named_object_refuses_to_derive(): void
    {
        static::assertNull((new TypeStructureShape())->derive(InvocationMother::typeStructure(InvocationMother::sealedMap(InvocationMother::item(
            'id',
            Type::string(),
        )))));
    }
}
