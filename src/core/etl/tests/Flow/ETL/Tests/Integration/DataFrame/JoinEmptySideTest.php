<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use PHPUnit\Framework\Attributes\TestWith;
use SensitiveParameter;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;

final class JoinEmptySideTest extends FlowTestCase
{
    public function test_cross_join_against_an_empty_side_yields_no_rows_under_the_cross_schema(): void
    {
        $joined = df()
            ->read(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))))
            ->crossJoin(df()->process(rows(schema(str_schema('code')))))
            ->fetch();

        static::assertSame([], $joined->toArray());
        static::assertSame(['id', 'code'], $joined->schema()->names());
    }

    /**
     * A single bucket forces Joiner::joinBuildingLeft(), 64 takes the default path.
     */
    public function test_left_join_against_an_empty_right_side_keeps_the_right_columns(): void
    {
        foreach ([$this->leftJoinedAgainstNothing(1), $this->leftJoinedAgainstNothing(64)] as $joined) {
            static::assertSame(['id', 'r'], $joined->schema()->names());
            static::assertSame([['id' => 1, 'r' => null], ['id' => 2, 'r' => null]], $joined->toArray());
        }
    }

    public function test_right_join_against_an_empty_left_side_keeps_the_left_columns(): void
    {
        foreach ([$this->rightJoinedAgainstNothing(1), $this->rightJoinedAgainstNothing(64)] as $joined) {
            // a RIGHT join drops the left side's duplicate join column, so `id` comes from the right
            static::assertSame(['l', 'id'], $joined->schema()->names());
            static::assertSame([['l' => null, 'id' => 1]], $joined->toArray());
        }
    }

    /**
     * @param int<1, max> $buckets
     */
    public function leftJoinedAgainstNothing(int $buckets): Rows
    {
        return df()
            ->read(from_rows(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')))))
            ->join(
                df()->process(rows(schema(int_schema('id'), str_schema('r')))),
                join_on(['id' => 'id']),
                JoinType::left,
                hash_join()->bucketsCount($buckets),
            )
            ->fetch();
    }

    /**
     * @param int<1, max> $buckets
     */
    public function rightJoinedAgainstNothing(int $buckets): Rows
    {
        return df()
            ->read(from_rows(rows(schema(int_schema('id'), str_schema('l')))))
            ->join(
                df()->process(array_to_rows([['id' => 1]], schema(int_schema('id')))),
                join_on(['id' => 'id']),
                JoinType::right,
                hash_join()->bucketsCount($buckets),
            )
            ->fetch();
    }

    #[TestWith([false])]
    #[TestWith([true])]
    public function test_an_unbound_left_join_against_an_empty_right_side_keeps_the_right_columns(bool $rightWrites): void
    {
        // binds nothing: the left side refuses, so the join runs unbound with the right side's declared columns
        $refusing = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('never described');
            }

            public function describe(): ?Description
            {
                return null;
            }

            public function bind(Schema $input): BoundStep
            {
                throw SchemaNotDerivableException::function('test', 'the left side refuses');
            }

            public function transform(Rows $rows, Backend $backend): Rows
            {
                return $rows;
            }
        };
        $right = df()->read(from_rows(rows(schema(int_schema('id'), str_schema('name')))));

        $joined = df()
            ->read(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))))
            ->with($refusing)
            ->join(
                $rightWrites ? $right->write(to_memory(new ArrayMemory())) : $right,
                join_on(['id' => 'id'], 'r_'),
                JoinType::left,
            )
            ->fetch();

        static::assertSame(['id', 'r_id', 'r_name'], $joined->schema()->names());
        static::assertSame([['id' => 1, 'r_id' => null, 'r_name' => null]], $joined->toArray());
    }
}
