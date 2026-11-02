<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Grouping;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Tests\Context\BackendsContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\DataProviderExternal;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

use const NAN;

final class GroupTableTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Backend, Schema, list<array<string, mixed>>, list<int>}>
     */
    public static function keys(): Generator
    {
        $utc = new DateTimeZone('UTC');

        foreach (BackendsContext::backends() as $name => [$backend]) {
            yield "{$name}: equal ints share an id" => [
                $backend,
                schema(int_schema('k')),
                [['k' => 1], ['k' => 2], ['k' => 1]],
                [0, 1, 0],
            ];
            yield "{$name}: zero and negative zero" => [
                $backend,
                schema(float_schema('k')),
                [['k' => 0.0], ['k' => -0.0], ['k' => 1.5]],
                [0, 0, 1],
            ];
            yield "{$name}: NaNs share an id" => [
                $backend,
                schema(float_schema('k')),
                [['k' => NAN], ['k' => 1.0], ['k' => NAN]],
                [0, 1, 0],
            ];
            yield "{$name}: strings" => [
                $backend,
                schema(str_schema('k')),
                [['k' => 'a'], ['k' => 'b'], ['k' => 'a']],
                [0, 1, 0],
            ];
            yield "{$name}: bools" => [
                $backend,
                schema(bool_schema('k')),
                [['k' => true], ['k' => false], ['k' => true]],
                [0, 1, 0],
            ];
            yield "{$name}: null is neither an empty string nor zero" => [
                $backend,
                schema(str_schema('k', nullable: true)),
                [['k' => null], ['k' => ''], ['k' => '0'], ['k' => null]],
                [0, 1, 2, 0],
            ];
            yield "{$name}: datetimes by instant" => [
                $backend,
                schema(datetime_schema('k')),
                [
                    ['k' => new DateTimeImmutable('2024-01-01 12:00:00', $utc)],
                    ['k' => new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('+01:00'))],
                    ['k' => new DateTimeImmutable('2024-01-01 13:00:00', $utc)],
                ],
                [0, 0, 1],
            ];
            yield "{$name}: dates by day" => [
                $backend,
                schema(date_schema('k')),
                [
                    ['k' => new DateTimeImmutable('2024-01-01')],
                    ['k' => new DateTimeImmutable('2024-01-02')],
                    ['k' => new DateTimeImmutable('2024-01-01')],
                ],
                [0, 1, 0],
            ];
            yield "{$name}: uuids" => [
                $backend,
                schema(uuid_schema('k')),
                [
                    ['k' => '00000000-0000-4000-8000-000000000001'],
                    ['k' => '00000000-0000-4000-8000-000000000002'],
                    ['k' => '00000000-0000-4000-8000-000000000001'],
                ],
                [0, 1, 0],
            ];
            yield "{$name}: enums" => [
                $backend,
                schema(enum_schema('k', BackedStringEnum::class)),
                [['k' => BackedStringEnum::one], ['k' => BackedStringEnum::two], ['k' => BackedStringEnum::one]],
                [0, 1, 0],
            ];
            yield "{$name}: times" => [
                $backend,
                schema(time_schema('k')),
                [
                    ['k' => new DateInterval('PT1H')],
                    ['k' => new DateInterval('PT2H')],
                    ['k' => new DateInterval('PT60M')],
                ],
                [0, 1, 0],
            ];
            yield "{$name}: lists" => [
                $backend,
                schema(list_schema('k', type_list(type_integer()))),
                [['k' => [1, 2]], ['k' => [2, 1]], ['k' => [1, 2]]],
                [0, 1, 0],
            ];
            yield "{$name}: xml documents by markup" => [
                $backend,
                schema(xml_schema('k')),
                [['k' => '<a>1</a>'], ['k' => '<a>2</a>'], ['k' => '<a>1</a>']],
                [0, 1, 0],
            ];
            yield "{$name}: a two-column key never runs one value into the next" => [
                $backend,
                schema(str_schema('a'), str_schema('b')),
                [['a' => 'ab', 'b' => ''], ['a' => 'a', 'b' => 'b'], ['a' => 'ab', 'b' => '']],
                [0, 1, 0],
            ];
        }
    }

    /**
     * @param list<array<string, mixed>> $rows
     * @param list<int> $ids
     */
    #[DataProvider('keys')]
    public function test_rows_with_equal_keys_share_an_id_in_first_appearance_order(
        Backend $backend,
        Schema $schema,
        array $rows,
        array $ids,
    ): void {
        static::assertSame(
            $ids,
            $backend->groupTable($schema->names(), $schema)->assign(array_to_rows($rows, $schema))->ids->values(),
        );
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_ids_stay_stable_across_batches_and_name_the_first_row_of_each_new_group(Backend $backend): void
    {
        $schema = schema(str_schema('k'));
        $table = $backend->groupTable(['k'], $schema);
        $table->assign(array_to_rows([['k' => 'x'], ['k' => 'y']], $schema));

        $second = $table->assign(array_to_rows([['k' => 'y'], ['k' => 'z'], ['k' => 'w'], ['k' => 'z']], $schema));

        static::assertSame([1, 2, 3, 2], $second->ids->values());
        static::assertSame([1, 2], $second->firstRows);
        static::assertSame(4, $table->count());
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_a_column_the_batch_lacks_under_a_nullable_declaration_is_a_null_key(Backend $backend): void
    {
        $declared = schema(int_schema('id'), str_schema('k', nullable: true));
        $table = $backend->groupTable(['k'], $declared);

        static::assertSame(
            [0, 0],
            $table->assign(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))))->ids->values(),
        );
        static::assertSame(
            [1, 0],
            $table
                ->assign(array_to_rows([['id' => 3, 'k' => 'a'], ['id' => 4, 'k' => null]], $declared))
                ->ids->values(),
        );
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_batches_built_by_either_backend_share_ids(Backend $backend): void
    {
        $schema = schema(int_schema('k'), str_schema('s'));
        $table = $backend->groupTable(['k', 's'], $schema);
        $table->assign(array_to_rows([['k' => 1, 's' => 'a'], ['k' => 2, 's' => 'b']], $schema, new PhpBackend()));

        static::assertSame(
            [1, 0, 2],
            $table
                ->assign(array_to_rows(
                    [['k' => 2, 's' => 'b'], ['k' => 1, 's' => 'a'], ['k' => 1, 's' => 'b']],
                    $schema,
                    $backend,
                ))
                ->ids->values(),
        );
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_a_date_and_a_datetime_at_the_same_instant_share_an_id(Backend $backend): void
    {
        $table = $backend->groupTable(['k'], schema(date_schema('k')));
        $table->assign(array_to_rows([['k' => new DateTimeImmutable('2024-01-01')]], schema(date_schema('k'))));

        static::assertSame(
            [0, 1],
            $table
                ->assign(array_to_rows([
                    ['k' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('UTC'))],
                    ['k' => new DateTimeImmutable('2024-01-01 00:00:01', new DateTimeZone('UTC'))],
                ], schema(datetime_schema('k'))))
                ->ids->values(),
        );
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_a_column_the_batch_lacks_under_a_required_declaration_is_refused(Backend $backend): void
    {
        $table = $backend->groupTable(['k'], schema(int_schema('id'), str_schema('k')));

        $this->expectExceptionMessage('"k"');

        $table->assign(array_to_rows([['id' => 1]], schema(int_schema('id'))));
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_a_null_list_key_is_one_group_whether_the_batch_lacks_the_column_or_holds_null(Backend $backend): void
    {
        $declared = schema(int_schema('id'), list_schema('k', type_list(type_string()), nullable: true));
        $table = $backend->groupTable(['k'], $declared);
        $table->assign(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')), $backend));

        static::assertSame(
            [1, 0],
            $table
                ->assign(array_to_rows([['id' => 3, 'k' => ['a']], ['id' => 4, 'k' => null]], $declared, $backend))
                ->ids->values(),
        );
        static::assertSame(2, $table->count());
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_an_xml_key_and_a_string_key_of_the_same_text_are_one_group(Backend $backend): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a>x</a>');
        $table = $backend->groupTable(['k'], schema(str_schema('k')->merge(xml_schema('k'))));
        $table->assign(array_to_rows(
            [['k' => "<?xml version=\"1.0\"?>\n<a>x</a>\n"]],
            schema(str_schema('k')),
            $backend,
        ));

        static::assertSame(
            [0],
            $table->assign(array_to_rows([['k' => $document]], schema(xml_schema('k')), $backend))->ids->values(),
        );
        static::assertSame(1, $table->count());
    }

    #[DataProviderExternal(BackendsContext::class, 'backends')]
    public function test_an_empty_batch_assigns_nothing(Backend $backend): void
    {
        $assigned = $backend
            ->groupTable(['k'], schema(int_schema('k')))
            ->assign(Rows::empty(schema(int_schema('k')), $backend));

        static::assertSame([], $assigned->ids->values());
        static::assertSame([], $assigned->firstRows);
    }
}
