<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Exception\SchemaValidationException;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_selective_validator;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class StructureSchemaMatchTest extends FlowIntegrationTestCase
{
    public static function provideMatrixCases(): Generator
    {
        $withNickname = ['id' => 1, 'email' => 'a@b.c', 'nickname' => 'norbert'];
        $withoutNickname = ['id' => 1, 'email' => 'a@b.c'];

        yield 'declared required, key present' => [
            type_structure(['id' => type_integer(), 'email' => type_string(), 'nickname' => type_string()]),
            $withNickname,
            true,
        ];

        yield 'declared required, key absent' => [
            type_structure(['id' => type_integer(), 'email' => type_string(), 'nickname' => type_string()]),
            $withoutNickname,
            false,
        ];

        yield 'declared optional, key present' => [
            type_structure(['id' => type_integer(), 'email' => type_string()], ['nickname' => type_string()]),
            $withNickname,
            true,
        ];

        yield 'declared optional, key absent' => [
            type_structure(['id' => type_integer(), 'email' => type_string()], ['nickname' => type_string()]),
            $withoutNickname,
            true,
        ];

        yield 'allow extra, undeclared key present' => [
            type_structure(['id' => type_integer(), 'email' => type_string()], [], true),
            $withNickname,
            true,
        ];

        yield 'allow extra, undeclared key absent' => [
            type_structure(['id' => type_integer(), 'email' => type_string()], [], true),
            $withoutNickname,
            true,
        ];

        yield 'without allow extra, undeclared key present' => [
            type_structure(['id' => type_integer(), 'email' => type_string()], [], false),
            $withNickname,
            false,
        ];

        yield 'declared nullable element, given null' => [
            type_structure([
                'id' => type_integer(),
                'email' => type_string(),
                'nickname' => type_optional(type_string()),
            ]),
            ['id' => 1, 'email' => 'a@b.c', 'nickname' => null],
            true,
        ];

        yield 'declared non nullable element, given null' => [
            type_structure(['id' => type_integer(), 'email' => type_string(), 'nickname' => type_string()]),
            ['id' => 1, 'email' => 'a@b.c', 'nickname' => null],
            false,
        ];
    }

    /**
     * @param Type<array<string, mixed>> $declared
     */
    #[DataProvider('provideMatrixCases')]
    public function test_match_accepts_what_is_valid_accepts(Type $declared, mixed $value, bool $expected): void
    {
        static::assertSame(
            $expected,
            $declared->isValid($value),
            'isValid() disagrees with the expectation, the matrix row itself is wrong',
        );

        $matched = true;

        try {
            data_frame()
                ->read(from_array([['user' => $value]]))
                ->match(schema(structure_schema('user', $declared)), schema_selective_validator())
                ->run();
        } catch (SchemaValidationException) {
            $matched = false;
        }

        static::assertSame($expected, $matched);
    }

    public function test_mixed_shapes_in_one_batch_stay_a_structure(): void
    {
        $declared = type_structure(['id' => type_integer(), 'email' => type_string()], ['nickname' => type_string()]);

        data_frame()
            ->read(from_array([
                ['user' => ['id' => 1, 'email' => 'a@b.c', 'nickname' => 'norbert']],
                ['user' => ['id' => 2, 'email' => 'c@d.e']],
            ]))
            ->match(schema(structure_schema('user', $declared)), schema_selective_validator())
            ->run();

        static::assertSame(
            'structure{id: integer, email: string, nickname?: string}',
            data_frame()
                ->read(from_array([
                    ['user' => ['id' => 1, 'email' => 'a@b.c', 'nickname' => 'norbert']],
                    ['user' => ['id' => 2, 'email' => 'c@d.e']],
                ]))
                ->schema()
                ->get('user')
                ->type()
                ->toString(),
        );
    }
}
