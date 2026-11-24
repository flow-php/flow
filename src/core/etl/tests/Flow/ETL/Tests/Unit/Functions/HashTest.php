<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTimeImmutable;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Schema;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class HashTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Schema, mixed, HashAlgorithm, ?string}>
     */
    public static function provider_hash_output_per_value_kind_and_algorithm(): Generator
    {
        foreach ([
            'string' => [
                schema(str_schema('value')),
                'test',
                [
                    'xxh128' => '6c78e0e3bd51d358d01e758642b85fb8',
                    'md5' => '098f6bcd4621d373cade4e832627b4f6',
                    'sha256' => '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
                ],
            ],
            'empty string' => [
                schema(str_schema('value')),
                '',
                [
                    'xxh128' => '99aa06d3014798d86001c324468d497f',
                    'md5' => 'd41d8cd98f00b204e9800998ecf8427e',
                    'sha256' => 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                ],
            ],
            'utf-8 string' => [
                schema(str_schema('value')),
                'zażółć',
                [
                    'xxh128' => 'bdd8ac7ac90b11bb31701c8e12e9349d',
                    'md5' => '884c73f2b5319ab2bbda84aacb14ec68',
                    'sha256' => '7eefe5e3046c86cec19ce1215e0a04724124d948468005a1ebc4adf2643bea13',
                ],
            ],
            'integer' => [
                schema(int_schema('value')),
                1,
                [
                    'xxh128' => 'df3ce784d856334d65cd25028f98f158',
                    'md5' => 'c4ca4238a0b923820dcc509a6f75849b',
                    'sha256' => '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b',
                ],
            ],
            'float' => [
                schema(float_schema('value')),
                1.5,
                [
                    'xxh128' => '72e27827e1082b043d006affbf7f50ee',
                    'md5' => '6008647277c4454cecd97d33c069f0ca',
                    'sha256' => '9f29a130438b81170b92a42650f9a94291ecad60bd47af2a3886e75f7f728725',
                ],
            ],
            'true' => [
                schema(bool_schema('value')),
                true,
                [
                    'xxh128' => 'df3ce784d856334d65cd25028f98f158',
                    'md5' => 'c4ca4238a0b923820dcc509a6f75849b',
                    'sha256' => '6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b',
                ],
            ],
            'false' => [
                schema(bool_schema('value')),
                false,
                [
                    'xxh128' => '99aa06d3014798d86001c324468d497f',
                    'md5' => 'd41d8cd98f00b204e9800998ecf8427e',
                    'sha256' => 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
                ],
            ],
            'list' => [
                schema(list_schema('value', type_list(type_string()))),
                ['test'],
                [
                    'xxh128' => '4450cf82dc53848e2bbe9798b70b0a6a',
                    'md5' => 'acc75d777f16492f95ba7c572335b7f7',
                    'sha256' => '18f4cf4cf6ad53618cd47d604ef6a6f9c0cbc6755d6caa31f333e851b3671328',
                ],
            ],
            'datetime' => [
                schema(datetime_schema('value')),
                new DateTimeImmutable('2021-01-01'),
                [
                    'xxh128' => '5347d10de38eb5570c044eb710a5120a',
                    'md5' => '7a0362b9779aebdc1d5899683970157f',
                    'sha256' => 'c7db551aff37ffed2f35e6a5b8449499bef83a5036f84847f03f9c9eedadb05e',
                ],
            ],
            'json' => [
                schema(json_schema('value')),
                '{"a":1}',
                [
                    'xxh128' => '1922c6c4ef7a35d299a4567fb5a80d47',
                    'md5' => '38bd6ceec415d6d69c5d5f29bbfbf480',
                    'sha256' => '506c2c44e41105954056801d93014e5603eb2b3acaa4bfee9e07eb0a5e072ff0',
                ],
            ],
            'null' => [
                schema(str_schema('value', nullable: true)),
                null,
                [
                    'xxh128' => null,
                    'md5' => null,
                    'sha256' => null,
                ],
            ],
        ] as $kind => [$schema, $value, $digests]) {
            foreach (HashAlgorithm::cases() as $algorithm) {
                yield "{$kind} {$algorithm->value}" => [$schema, $value, $algorithm, $digests[$algorithm->value]];
            }
        }
    }

    #[DataProvider('provider_hash_output_per_value_kind_and_algorithm')]
    public function test_hash_output_is_pinned_per_value_kind_and_algorithm(
        Schema $schema,
        mixed $value,
        HashAlgorithm $algorithm,
        ?string $expected,
    ): void {
        static::assertSame($expected, (new FunctionContext(flow_context()))->eval(
            ref('value')->hash($algorithm),
            ['value' => $value],
            $schema,
        ));
    }

    public function test_hashing_concat(): void
    {
        static::assertSame('5d22af35c78f30e5d85f3b65d988f8fb', (new FunctionContext(flow_context()))->eval(
            hash(concat(ref('value'), lit('_'), ref('value')), HashAlgorithm::xxh128),
            [
                'value' => 'test',
            ],
            schema(str_schema('value')),
        ));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = hash(ref('string'), HashAlgorithm::sha256);
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }

    public function test_the_default_and_xxh128_give_todays_hash(): void
    {
        $context = new FunctionContext(flow_context());
        $schema = schema(str_schema('a'));

        static::assertSame([
            '6c78e0e3bd51d358d01e758642b85fb8',
            '6c78e0e3bd51d358d01e758642b85fb8',
            '6c78e0e3bd51d358d01e758642b85fb8',
        ], [
            $context->eval(hash(ref('a')), ['a' => 'test'], $schema),
            $context->eval(hash(ref('a'), HashAlgorithm::xxh128), ['a' => 'test'], $schema),
            $context->eval(ref('a')->hash(), ['a' => 'test'], $schema),
        ]);
    }
}
