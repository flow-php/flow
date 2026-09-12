<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\Types\DSL\type_is_nullable;

final class StructureTest extends FlowTestCase
{
    public function test_eval_preserves_key_order(): void
    {
        static::assertSame(
            ['second' => 'x', 'first' => 1],
            structure(['second' => ref('b'), 'first' => ref('a')])->eval(row([
                'a' => 1,
                'b' => 'x',
                'q' => null,
            ]), flow_context()),
        );
    }

    public function test_eval_keeps_null_under_its_key(): void
    {
        static::assertSame(
            ['id' => 'x', 'quantity' => null],
            structure(['id' => ref('b'), 'quantity' => ref('q')])->eval(row([
                'a' => 1,
                'b' => 'x',
                'q' => null,
            ]), flow_context()),
        );
    }

    public function test_eval_with_numeric_keys(): void
    {
        static::assertSame(
            [5 => 1, 7 => 'x'],
            structure([5 => ref('a'), 7 => ref('b')])->eval(row(['a' => 1, 'b' => 'x', 'q' => null]), flow_context()),
        );
    }

    public function test_list_shaped_keys_are_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Structure keys cannot be a list (0, 1, 2, ...). Name the elements or use non-sequential keys.',
        );

        structure([ref('a'), ref('b')]);
    }

    public function test_eval_nested_structure(): void
    {
        static::assertSame(
            ['user' => ['id' => 1]],
            structure(['user' => structure(['id' => ref('a')])])->eval(row([
                'a' => 1,
                'b' => 'x',
                'q' => null,
            ]), flow_context()),
        );
    }

    public function test_returns_carries_element_types_and_nullability(): void
    {
        static::assertSame(
            'structure{id: string, quantity: ?integer}',
            (new ReferenceResolver())
                ->resolve(
                    structure(['id' => ref('line_id'), 'quantity' => ref('quantity')]),
                    schema(str_schema('line_id'), int_schema('quantity', nullable: true)),
                )
                ->returns()
                ->toString(),
        );
    }

    public function test_returns_is_never_nullable(): void
    {
        $returns = (new ReferenceResolver())
            ->resolve(
                structure(['id' => ref('line_id'), 'quantity' => ref('quantity')]),
                schema(str_schema('line_id', true), int_schema('quantity', true)),
            )
            ->returns();

        static::assertSame('structure{id: ?string, quantity: ?integer}', $returns->toString());
        static::assertFalse(type_is_nullable($returns));
    }

    public function test_returns_with_numeric_keys(): void
    {
        static::assertSame(
            'structure{5: integer, 7: string}',
            (new ReferenceResolver())
                ->resolve(structure([5 => ref('a'), 7 => ref('b')]), schema(int_schema('a'), str_schema('b')))
                ->returns()
                ->toString(),
        );
    }

    public function test_children_are_a_list_in_key_order(): void
    {
        $a = ref('a');
        $b = ref('b');

        static::assertSame([$a, $b], structure(['x' => $a, 'y' => $b])->children());
    }

    public function test_with_children_keeps_keys(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'x'],
            structure(['id' => ref('a'), 'name' => ref('b')])
                ->withChildren([lit(1), lit('x')])
                ->eval(row(['a' => 1, 'b' => 'x', 'q' => null]), flow_context()),
        );
    }

    public function test_with_children_rejects_arity_change(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Structure expects 2 children, got 0.');

        structure(['id' => ref('line_id'), 'quantity' => ref('quantity')])->withChildren([]);
    }

    public function test_empty_elements_are_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Structure requires at least one element.');

        structure([]);
    }

    public function test_resolved_only_after_every_element_resolves(): void
    {
        $function = structure(['id' => ref('line_id'), 'quantity' => ref('quantity')]);

        static::assertFalse($function->resolved());
        static::assertTrue(
            (new ReferenceResolver())
                ->resolve($function, schema(str_schema('line_id'), int_schema('quantity', nullable: true)))
                ->resolved(),
        );
    }
}
