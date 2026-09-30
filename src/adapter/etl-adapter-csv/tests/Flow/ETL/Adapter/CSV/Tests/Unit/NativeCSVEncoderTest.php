<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\Tests\Mother\CSVEncoderMother;
use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Context\TextWriterBatches;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\Attributes\TestWith;
use Throwable;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;

#[RequiresPhpExtension('flow_php')]
final class NativeCSVEncoderTest extends FlowTestCase
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
                'native' => [CSVEncoderMother::native($options), new DefaultBackend()],
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

    /**
     * @param list<string> $unrendered
     */
    #[TestWith(['scalars with nulls', 'Y-m-d', []])]
    #[TestWith(['temporal kinds', 'Y-m-d\TH:i:sP', []])]
    #[TestWith(['temporal kinds', 'D, d M Y', ['at', 'offset', 'on']])]
    #[TestWith(['a json column', 'Y-m-d', []])]
    #[TestWith(['markup columns', 'Y-m-d', ['x', 'e']])]
    #[TestWith(['nested kinds rendered natively', 'Y-m-d', []])]
    #[TestWith(['nested kinds rendered natively', 'l', ['l', 's']])]
    #[TestWith(['a list of json documents', 'Y-m-d', ['l']])]
    #[TestWith(['nested markup leaves', 'Y-m-d', ['l', 's']])]
    public function test_the_columns_the_writer_leaves_to_php_cells(
        string $batch,
        string $format,
        array $unrendered,
    ): void {
        static::assertSame(
            $unrendered,
            CSVEncoderMother::writer(new CSVWriteOptions(dateTimeFormat: $format, dateFormat: $format))->unrendered(
                TextWriterBatches::of($batch)[0],
            ),
        );
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
