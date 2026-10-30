<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Closure;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function iterator_to_array;

final class AliasedReferenceTest extends FlowIntegrationTestCase
{
    /**
     * @return Generator<string, array{Closure(DataFrame, Reference): mixed}>
     */
    public static function operators_reading_a_reference(): Generator
    {
        yield 'dropDuplicates' => [
            static fn(DataFrame $df, Reference $ref): array => $df->dropDuplicates($ref)->fetch()->toArray(),
        ];
        yield 'repartition' => [
            static fn(DataFrame $df, Reference $ref): array => array_map(
                static fn($rows): array => $rows->toArray(),
                iterator_to_array($df->repartition($ref)->get(), false),
            ),
        ];
        yield 'drop' => [
            static fn(DataFrame $df, Reference $ref): array => $df->drop($ref)->fetch()->toArray(),
        ];
        yield 'groupBy' => [
            static fn(DataFrame $df, Reference $ref): array => $df
                ->groupBy($ref)
                ->aggregate(count(ref('b')))
                ->fetch()
                ->toArray(),
        ];
    }

    /**
     * @param Closure(DataFrame, Reference): mixed $operator
     */
    #[DataProvider('operators_reading_a_reference')]
    public function test_an_alias_never_picks_the_column_read(Closure $operator): void
    {
        $rows = [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'x'], ['a' => 1, 'b' => 'y']];

        static::assertSame(
            $operator(df()->read(from_array($rows)), ref('a')),
            $operator(df()->read(from_array($rows)), ref('a')->as('b')),
        );
    }

    public function test_select_renames_an_aliased_column(): void
    {
        static::assertSame(
            [['b' => 1], ['b' => 2]],
            df()
                ->read(from_array([['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']]))
                ->select(ref('a')->as('b'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_select_refuses_an_alias_that_names_another_selected_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('select() would output column "b" twice');

        df()
            ->read(from_array([['a' => 1, 'b' => 'x']]))
            ->select(ref('a')->as('b'), ref('b'))
            ->fetch();
    }
}
