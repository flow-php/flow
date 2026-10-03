<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\Tests\Mother\JsonEncoderMother;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\TextWriterBatches;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Throwable;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_PRETTY_PRINT;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

#[RequiresPhpExtension('flow_php')]
final class RustJsonEncoderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function batches(): Generator
    {
        foreach (TextWriterBatches::names() as $name) {
            yield $name => [$name];
        }
    }

    #[DataProvider('batches')]
    public function test_encode_returns_what_the_php_encoder_returns(string $batch): void
    {
        [$schema, $rows] = TextWriterBatches::of($batch);

        foreach ([
            [JSON_THROW_ON_ERROR, DateTimeInterface::ATOM, 'Y-m-d', "\n"],
            [0, 'D, d M Y H:i:s', 'd/m/Y', ','],
            [
                JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
                'Y/m/d H:i:s.u T e',
                'Ymd',
                ",\n",
            ],
        ] as [$flags, $dateTimeFormat, $dateFormat, $separator]) {
            $outcomes = [];

            foreach ([
                'php' => [JsonEncoderMother::php($flags, $dateTimeFormat, $dateFormat), new PhpBackend()],
                'native' => [JsonEncoderMother::native($flags, $dateTimeFormat, $dateFormat), new AdaptiveBackend()],
                'native over adopted php columns' => [
                    JsonEncoderMother::native($flags, $dateTimeFormat, $dateFormat),
                    new PhpBackend(),
                ],
            ] as $lane => [$encoder, $backend]) {
                try {
                    $outcomes[$lane] = $encoder->encode(array_to_rows($rows, $schema, $backend), $separator);
                } catch (Throwable $refusal) {
                    $outcomes[$lane] = $refusal::class . ': ' . $refusal->getMessage();
                }
            }

            static::assertSame($outcomes['php'], $outcomes['native'], $dateTimeFormat);
            static::assertSame($outcomes['php'], $outcomes['native over adopted php columns'], $dateTimeFormat);
        }
    }

    public function test_a_batch_under_another_schema_is_rendered_by_that_schema(): void
    {
        $encoder = JsonEncoderMother::native();

        static::assertSame(['{"v":"a b"}', '{"v":{"a":{}}}', '{"v":1.5}'], [
            $encoder->encode(array_to_rows([['v' => 'a b']], schema(str_schema('v'))), "\n"),
            $encoder->encode(array_to_rows([['v' => '{"a": {}}']], schema(json_schema('v'))), "\n"),
            $encoder->encode(array_to_rows([['v' => 1.5]], schema(float_schema('v'))), "\n"),
        ]);
    }

    public function test_the_writer_refuses_a_flag_it_does_not_render(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('flow_php JSON writer supports only the JSON_THROW_ON_ERROR');

        JsonEncoderMother::native(JSON_PRETTY_PRINT);
    }
}
