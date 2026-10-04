<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Integration;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\FloeWriter;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class FloeWriteContractTest extends FlowIntegrationTestCase
{
    public static function rejected_values(): array
    {
        return [
            'string into int' => [
                schema(int_schema('id')),
                array_to_rows([['id' => 'AB-1']], schema(str_schema('id'))),
                'expected: id<integer>, given: id<string>',
            ],
            'float into int' => [
                schema(int_schema('id')),
                array_to_rows([['id' => 1.5]], schema(float_schema('id'))),
                'expected: id<integer>, given: id<float>',
            ],
            'int into string' => [
                schema(str_schema('name')),
                array_to_rows([['name' => 1000]], schema(int_schema('name'))),
                'expected: name<string>, given: name<integer>',
            ],
            'null into non nullable' => [
                schema(str_schema('name')),
                array_to_rows([['name' => null]], schema(str_schema('name', nullable: true))),
                'expected: name<string>, given: name<?string>',
            ],
            'undeclared column' => [
                schema(int_schema('id')),
                array_to_rows([['id' => 1, 'extra' => 2]], schema(int_schema('id'), int_schema('extra'))),
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
        $writer = new FloeWriter($this->fs(), $schema, new AdaptiveBackend(), new Options());
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
        $writer = new FloeWriter($this->fs(), schema(int_schema('id')), new AdaptiveBackend(), new Options());
        $writer->create($path);
        $writer->write(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));

        try {
            $writer->write(array_to_rows([['id' => 'AB-1']], schema(str_schema('id'))));
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
            new AdaptiveBackend(),
            new Options(),
        );
        $writer->create($path);
        $writer->write(array_to_rows(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        ));
        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
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
        $writer = new FloeWriter(
            $this->fs(),
            schema(int_schema('id'), str_schema('name')),
            new AdaptiveBackend(),
            new Options(),
        );
        $writer->create($path);
        $writer->write(array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))));

        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('Missing Definitions');

        $writer->write(array_to_rows([['id' => 2]], schema(int_schema('id'))));
    }
}
