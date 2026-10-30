<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTimeImmutable;
use Flow\ETL\Functions\Hash;
use Flow\ETL\Kernels\Hash\NativePHPHash;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class HashTest extends FlowTestCase
{
    public function test_hashing_array_value(): void
    {
        static::assertSame('4450cf82dc53848e2bbe9798b70b0a6a', (new FunctionContext(flow_context()))->eval(
            ref('value')->hash(),
            ['value' => [
                'test',
            ]],
            schema(list_schema('value', type_list(type_string()))),
        ));
    }

    public function test_hashing_concat(): void
    {
        static::assertSame(
            (new NativePHPHash())->hash('test_test'),
            (new FunctionContext(flow_context()))->eval(
                hash(concat(ref('value'), lit('_'), ref('value')), new NativePHPHash('xxh128')),
                [
                    'value' => 'test',
                ],
                schema(str_schema('value')),
            ),
        );
    }

    public function test_hashing_datetime(): void
    {
        static::assertSame('5347d10de38eb5570c044eb710a5120a', (new FunctionContext(flow_context()))->eval(
            ref('value')->hash(),
            [
                'value' => new DateTimeImmutable('2021-01-01'),
            ],
            schema(datetime_schema('value')),
        ));
    }

    public function test_hashing_null_value(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->hash(),
            ['value' => null],
            schema(str_schema('value', nullable: true)),
        ));
    }

    public function test_hashing_string_value(): void
    {
        static::assertSame('6c78e0e3bd51d358d01e758642b85fb8', (new FunctionContext(flow_context()))->eval(
            ref('value')->hash(),
            ['value' => 'test'],
            schema(str_schema('value')),
        ));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new Hash(ref('string'), new NativePHPHash('sha256'));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
