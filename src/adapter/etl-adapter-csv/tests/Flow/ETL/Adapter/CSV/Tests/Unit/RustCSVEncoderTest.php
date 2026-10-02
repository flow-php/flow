<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\Tests\Mother\CSVEncoderMother;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Context\TextWriterBatches;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use Throwable;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;

#[RequiresPhpExtension('flow_php')]
final class RustCSVEncoderTest extends FlowTestCase
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
            new CSVWriteOptions(newLineSeparator: "\n"),
            new CSVWriteOptions(';', "'", '', "\r\n", 'D, d M Y H:i:s', 'd/m/Y'),
            new CSVWriteOptions('.', '"', '/', "\n", 'Y-m-d H:i:s.u T e', 'Ymd'),
        ] as $options) {
            $outcomes = [];

            foreach ([
                'php' => [CSVEncoderMother::php($options), new PhpBackend()],
                'native' => [CSVEncoderMother::native($options), new AdaptiveBackend()],
                'native over adopted php columns' => [CSVEncoderMother::native($options), new PhpBackend()],
            ] as $lane => [$encoder, $backend]) {
                try {
                    $outcomes[$lane] = $encoder->encode(array_to_rows($rows, $schema, $backend));
                } catch (Throwable $refusal) {
                    $outcomes[$lane] = $refusal::class . ': ' . $refusal->getMessage();
                }
            }

            static::assertSame($outcomes['php'], $outcomes['native'], $options->dateTimeFormat);
            static::assertSame(
                $outcomes['php'],
                $outcomes['native over adopted php columns'],
                $options->dateTimeFormat,
            );
        }
    }

    public function test_the_not_finite_floats_are_written_by_name(): void
    {
        [$schema, $rows] = TextWriterBatches::of('not finite floats');

        static::assertSame("1,NAN\n2,INF\n3,-INF\n", CSVEncoderMother::native()->encode(array_to_rows($rows, $schema)));
    }

    public function test_a_batch_under_another_schema_is_rendered_by_that_schema(): void
    {
        $encoder = CSVEncoderMother::native();

        static::assertSame(["\"a b\"\n", "<a><b>1</b></a>\n", "1.0\n"], [
            $encoder->encode(array_to_rows([['v' => 'a b']], schema(str_schema('v')))),
            $encoder->encode(array_to_rows([['v' => '<a><b>1</b></a>']], schema(xml_schema('v')))),
            $encoder->encode(array_to_rows([['v' => 1.0]], schema(float_schema('v')))),
        ]);
    }

    public function test_the_header_is_the_php_encoders(): void
    {
        $options = new CSVWriteOptions(separator: ';', enclosure: "'", newLineSeparator: '|');

        static::assertSame(
            CSVEncoderMother::php($options)->encodeHeader(['id', 'first name', "it's"]),
            CSVEncoderMother::native($options)->encodeHeader(['id', 'first name', "it's"]),
        );
    }
}
