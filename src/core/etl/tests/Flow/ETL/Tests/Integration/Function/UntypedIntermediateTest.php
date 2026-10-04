<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_string;

final class UntypedIntermediateTest extends FlowTestCase
{
    /**
     * An inner node that cannot name its type (array_get() over array<mixed>) still hands its values to a parent
     * that can.
     *
     * @return Generator<string, array{ScalarFunction, string, mixed}>
     */
    public static function trees(): Generator
    {
        yield 'upper of an array_get' => [ref('j')->jsonDecode()->arrayGet('name')->upper(), 'string', 'ANN'];
        yield 'is null of an array_get' => [ref('j')->jsonDecode()->arrayGet('name')->isNull(), 'boolean', false];
        yield 'size of an array_get' => [ref('j')->jsonDecode()->arrayGet('tags')->size(), 'integer', 2];
        yield 'cast of an array_get' => [ref('j')->jsonDecode()->arrayGet('n')->cast(type_string()), 'string', '3'];
        yield 'size of array_keys' => [ref('j')->jsonDecode()->arrayKeys()->size(), 'integer', 3];
    }

    #[DataProvider('trees')]
    public function test_a_parent_of_an_untyped_node_runs(ScalarFunction $tree, string $type, mixed $value): void
    {
        $rows = data_frame()
            ->read(from_array([['j' => '{"name":"ann","tags":["a","b"],"n":3}']], schema(json_schema('j'))))
            ->withEntry('x', $tree)
            ->fetch();

        static::assertSame($type, $rows->schema()->get('x')->type()->toString());
        static::assertSame($value, $rows->values(0)['x']);
    }

    public function test_storing_an_untyped_node_is_refused_at_bind(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        data_frame()
            ->read(from_array([['j' => '{"name":"ann"}']], schema(json_schema('j'))))
            ->withEntry('x', ref('j')->jsonDecode()->arrayGet('name'))
            ->fetch();
    }
}
