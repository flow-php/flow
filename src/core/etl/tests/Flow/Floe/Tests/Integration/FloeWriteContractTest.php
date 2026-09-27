<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class FloeWriteContractTest extends FlowIntegrationTestCase
{
    public static function rejected_values(): array
    {
        return [
            'string into int' => [
                schema(int_schema('id')),
                rows(schema(str_schema('id')), row(['id' => 'AB-1'])),
                'expected: id<integer>, given: id<string>',
            ],
            'float into int' => [
                schema(int_schema('id')),
                rows(schema(float_schema('id')), row(['id' => 1.5])),
                'expected: id<integer>, given: id<float>',
            ],
            'int into string' => [
                schema(str_schema('name')),
                rows(schema(int_schema('name')), row(['name' => 1000])),
                'expected: name<string>, given: name<integer>',
            ],
            'null into non nullable' => [
                schema(str_schema('name')),
                rows(schema(str_schema('name', nullable: true)), row(['name' => null])),
                'expected: name<string>, given: name<?string>',
            ],
            'undeclared column' => [
                schema(int_schema('id')),
                rows(schema(int_schema('id'), int_schema('extra')), row(['id' => 1, 'extra' => 2])),
                'new column "extra"',
            ],
        ];
    }

    #[DataProvider('rejected_values')]
    public function test_the_writer_rejects_the_value_with_the_message(
        Schema $schema,
        Rows $batch,
        string $expectedMessage,
    ): void {
        $path = $this->cacheDir->suffix('contract-' . md5($expectedMessage) . '.floe');
        $writer = new FloeWriter($this->fs(), $schema, new Options());
        $writer->create($path);

        try {
            $writer->write($batch);
            static::fail('the writer accepted a value it must reject');
        } catch (IncompatibleSchemaException $e) {
            static::assertStringContainsString($expectedMessage, $e->getMessage());
        }

        $writer->close();

        static::assertSame(
            [],
            FloeStreamReaderContext::readAll($this->fs(), $path)->toArray(),
            'the writer emitted bytes for a rejected batch',
        );
    }

    public function test_a_rejected_batch_leaves_previously_written_rows_readable(): void
    {
        $path = $this->cacheDir->suffix('contract-survivor.floe');
        $writer = new FloeWriter($this->fs(), schema(int_schema('id')), new Options());
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])));

        try {
            $writer->write(rows(schema(str_schema('id')), row(['id' => 'AB-1'])));
            static::fail('rejected batch was accepted');
        } catch (IncompatibleSchemaException) {
        }

        $writer->close();

        static::assertSame([['id' => 1], ['id' => 2]], FloeStreamReaderContext::readAll($this->fs(), $path)->toArray());
    }

    /**
     * SCHEMA EVOLUTION: the reverse - a first batch WITHOUT the column and a later one WITH it -
     * is rejected as a new column. Once append supports adding an optional column, that case
     * belongs here too.
     */
    public function test_a_batch_omitting_a_nullable_column_still_writes(): void
    {
        $path = $this->cacheDir->suffix('contract-absent.floe');
        $writer = new FloeWriter(
            $this->fs(),
            schema(int_schema('id'), str_schema('name', nullable: true)),
            new Options(),
        );
        $writer->create($path);
        $writer->write(rows(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            row(['id' => 1, 'name' => 'a']),
        ));
        $writer->write(rows(schema(int_schema('id')), row(['id' => 2])));
        $writer->close();

        static::assertSame(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            FloeStreamReaderContext::readAll($this->fs(), $path)->toArray(),
        );
    }

    /**
     * b57's writer half: the old check walked the row's own values, so a NOT NULL column the batch
     * simply omitted was never looked at and was encoded as VALUE_ABSENT.
     */
    public function test_a_batch_omitting_a_not_null_column_is_refused(): void
    {
        $path = $this->cacheDir->suffix('contract-absent-not-null.floe');
        $writer = new FloeWriter($this->fs(), schema(int_schema('id'), str_schema('name')), new Options());
        $writer->create($path);
        $writer->write(rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Missing Definitions');

        $writer->write(rows(schema(int_schema('id')), row(['id' => 2])));
    }
}
