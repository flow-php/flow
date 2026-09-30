<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\NativeJsonWriter;
use Flow\ETL\Adapter\JSON\Tests\Mother\JSONEncoderMother;
use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\TextWriterBatches;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
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
final class NativeJSONEncoderTest extends FlowTestCase
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
                'php' => [JSONEncoderMother::php($flags, $dateTimeFormat, $dateFormat), new PhpBackend()],
                'native' => [JSONEncoderMother::native($flags, $dateTimeFormat, $dateFormat), new DefaultBackend()],
                'native over adopted php columns' => [
                    JSONEncoderMother::native($flags, $dateTimeFormat, $dateFormat),
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

    /**
     * @param list<string> $unrendered
     */
    #[TestWith(['scalars with nulls', 'Y-m-d', []])]
    #[TestWith(['temporal kinds', 'Y-m-d\TH:i:sP', []])]
    #[TestWith(['temporal kinds', 'D, d M Y', ['at', 'offset', 'on']])]
    #[TestWith(['a json column', 'Y-m-d', ['j']])]
    #[TestWith(['markup columns', 'Y-m-d', ['x', 'e']])]
    #[TestWith(['nested kinds rendered natively', 'Y-m-d', []])]
    #[TestWith(['a list of json documents', 'Y-m-d', ['l']])]
    #[TestWith(['nested markup leaves', 'Y-m-d', ['l', 's']])]
    public function test_the_columns_the_writer_leaves_to_php_fragments(
        string $batch,
        string $format,
        array $unrendered,
    ): void {
        static::assertSame(
            $unrendered,
            (new NativeJsonWriter(JSON_THROW_ON_ERROR, $format, $format))->unrendered(TextWriterBatches::of($batch)[0]),
        );
    }

    public function test_a_batch_under_another_schema_is_rendered_by_that_schema(): void
    {
        $encoder = JSONEncoderMother::native();

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

        new NativeJsonWriter(JSON_PRETTY_PRINT, DateTimeInterface::ATOM, 'Y-m-d');
    }
}
