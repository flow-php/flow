<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\combine;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class CombineTest extends FlowTestCase
{
    public function test_operands_that_are_not_lists_are_refused_at_bind(): void
    {
        /** @var ScalarFunction $resolved */
        $resolved = (new ReferenceResolver())->resolve(
            combine(ref('keys'), ref('values')),
            schema(list_schema('keys', type_list(type_string())), str_schema('values')),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('both operands must declare list types');

        $resolved->returns();
    }

    public function test_array_combine(): void
    {
        static::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c']), lit([1, 2, 3]))->eval(row([]), flow_context()),
        );
    }

    public function test_array_combine_when_arrays_are_empty(): void
    {
        static::assertSame([], combine(lit([]), lit([]))->eval(row([]), flow_context()));
    }

    public function test_array_combine_when_keys_are_not_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        combine(lit('a'), lit([1, 2, 3]))->eval(row([]), flow_context());
    }

    public function test_array_combine_when_keys_are_not_unique(): void
    {
        static::assertSame(
            ['a' => 4, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c', 'a']), lit([1, 2, 3, 4]))->eval(row([]), flow_context()),
        );
    }

    public function test_array_combine_when_one_of_arrays_is_empty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Combine function requires keys and values arrays to have the same length');

        combine(lit(['a', 'b', 'c']), lit([]))->eval(row([]), flow_context());
    }
}
