<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileSchemaCommand;
use Flow\CLI\Tests\Context\SchemaInferenceFixtureContext;
use Flow\ETL\Tests\CommandOutputNormalizer;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class FileSchemaCommandTest extends TestCase
{
    use CommandOutputNormalizer;

    public function test_run_schema(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv']);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            [{"ref":"order_id","type":{"type":"uuid"},"nullable":true,"metadata":[]},{"ref":"created_at","type":{"type":"datetime"},"nullable":true,"metadata":[]},{"ref":"updated_at","type":{"type":"datetime"},"nullable":true,"metadata":[]},{"ref":"discount","type":{"type":"float"},"nullable":true,"metadata":[]},{"ref":"address","type":{"type":"json"},"nullable":true,"metadata":[]},{"ref":"notes","type":{"type":"json"},"nullable":true,"metadata":[]},{"ref":"items","type":{"type":"json"},"nullable":true,"metadata":[]}]

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_all_strings_wins_over_sample_size(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::wideningPath(),
            '--output-ascii' => true,
            '--schema-all-strings' => true,
            '--schema-sample-size' => 1,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- a: ?string

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_as_ascii(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--output-ascii' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- order_id: ?uuid
            |-- created_at: ?datetime
            |-- updated_at: ?datetime
            |-- discount: ?float
            |-- address: ?json
            |-- notes: ?json
            |-- items: ?json

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_options_are_ignored_by_a_self_describing_source(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.parquet',
            '--output-table' => true,
            '--schema-all-strings' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputEquals(<<<'OUTPUT'
            +------------+--------------+----------+----------+
            |       name |         type | nullable | metadata |
            +------------+--------------+----------+----------+
            |   order_id |         uuid |    false |       [] |
            | created_at |     datetime |    false |       [] |
            | updated_at |     datetime |     true |       [] |
            |   discount |        float |     true |       [] |
            |      email |       string |    false |       [] |
            |   customer |       string |    false |       [] |
            |    address | structure_v2 |    false |       [] |
            |      notes |         list |    false |       [] |
            |      items |         list |    false |       [] |
            +------------+--------------+----------+----------+
            9 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_a_sample_size_the_file_outgrows_reports_the_sampled_schema(): void
    {
        // schema() answers from the plan and reads no row, so the value the sample does not fit
        // ('xyz' against ?integer) is never hydrated here - `flow read` is where it still fails
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::outgrownPath(),
            '--output-ascii' => true,
            '--schema-sample-size' => 1,
        ]);

        $tester->assertCommandIsSuccessful();
        self::assertCommandOutputIdentical("schema\n|-- a: ?integer\n", $tester->getDisplay());
    }

    public function test_run_schema_with_all_strings(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--output-table' => true,
            '--schema-all-strings' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputEquals(<<<'OUTPUT'
            +------------+--------+----------+----------+
            |       name |   type | nullable | metadata |
            +------------+--------+----------+----------+
            |   order_id | string |     true |       [] |
            | created_at | string |     true |       [] |
            | updated_at | string |     true |       [] |
            |   discount | string |     true |       [] |
            |    address | string |     true |       [] |
            |      notes | string |     true |       [] |
            |      items | string |     true |       [] |
            +------------+--------+----------+----------+
            7 rows

            OUTPUT, $tester->getDisplay());
    }

    #[TestWith([0])]
    #[TestWith([-2])]
    public function test_run_schema_with_an_invalid_files_to_sniff_is_refused(int $filesToSniff): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--schema-files-to-sniff' => $filesToSniff,
        ]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString(
            'Schema files to sniff must be greater than 0, or -1 for all sources.',
            $tester->getDisplay(),
        );
    }

    #[TestWith([0])]
    #[TestWith([-2])]
    public function test_run_schema_with_an_invalid_sample_size_is_refused(int $sampleSize): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--schema-sample-size' => $sampleSize]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString(
            'Schema sample size must be greater than 0, or -1 for all rows.',
            $tester->getDisplay(),
        );
    }

    public function test_run_schema_with_files_to_sniff_bounds_the_sources_read(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::unionGlob(),
            '--output-ascii' => true,
            '--schema-union-by-name' => true,
            '--schema-files-to-sniff' => 1,
        ]);

        $tester->assertCommandIsSuccessful();

        // Only the first source is opened, so the second file's "c" column is never seen
        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- a: ?integer
            |-- b: ?integer

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_large_offset(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 1000,
            '--output-ascii' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        // the schema comes from the source, not from the rows an offset happens to leave, so it no
        // longer depends on --input-file-offset
        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- order_id: ?uuid
            |-- created_at: ?datetime
            |-- updated_at: ?datetime
            |-- discount: ?float
            |-- address: ?json
            |-- notes: ?json
            |-- items: ?json
            OUTPUT . "\n", $tester->getDisplay());
    }

    public function test_run_schema_with_offset(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 5,
            '--output-ascii' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        // Schema should be the same regardless of offset since schema is inferred from structure
        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- order_id: ?uuid
            |-- created_at: ?datetime
            |-- updated_at: ?datetime
            |-- discount: ?float
            |-- address: ?json
            |-- notes: ?json
            |-- items: ?json

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_offset_and_limit(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 2,
            '--input-file-limit' => 3,
            '--output-table' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        // Schema should be the same even with offset and limit
        self::assertCommandOutputEquals(<<<'OUTPUT'
            +------------+----------+----------+----------+
            |       name |     type | nullable | metadata |
            +------------+----------+----------+----------+
            |   order_id |     uuid |     true |       [] |
            | created_at | datetime |     true |       [] |
            | updated_at | datetime |     true |       [] |
            |   discount |    float |     true |       [] |
            |    address |     json |     true |       [] |
            |      notes |     json |     true |       [] |
            |      items |     json |     true |       [] |
            +------------+----------+----------+----------+
            7 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_php_output(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--output-php' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            \Flow\ETL\DSL\schema(
                \Flow\ETL\DSL\uuid_schema("order_id", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\datetime_schema("created_at", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\datetime_schema("updated_at", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\float_schema("discount", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\json_schema("address", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\json_schema("notes", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\json_schema("items", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
            );

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_pretty_output(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--output-pretty' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            [
                {
                    "ref": "order_id",
                    "type": {
                        "type": "uuid"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "created_at",
                    "type": {
                        "type": "datetime"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "updated_at",
                    "type": {
                        "type": "datetime"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "discount",
                    "type": {
                        "type": "float"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "address",
                    "type": {
                        "type": "json"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "notes",
                    "type": {
                        "type": "json"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "items",
                    "type": {
                        "type": "json"
                    },
                    "nullable": true,
                    "metadata": []
                }
            ]

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_sample_size_freezes_the_narrower_type(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::wideningPath(),
            '--output-ascii' => true,
            '--schema-sample-size' => 1,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- a: ?integer

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--output-table' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputEquals(<<<'OUTPUT'
            +------------+----------+----------+----------+
            |       name |     type | nullable | metadata |
            +------------+----------+----------+----------+
            |   order_id |     uuid |     true |       [] |
            | created_at | datetime |     true |       [] |
            | updated_at | datetime |     true |       [] |
            |   discount |    float |     true |       [] |
            |    address |     json |     true |       [] |
            |      notes |     json |     true |       [] |
            |      items |     json |     true |       [] |
            +------------+----------+----------+----------+
            7 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_and_limit_5(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--output-table' => true,
            '--input-file-limit' => 5,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +------------+----------+----------+----------+
            |       name |     type | nullable | metadata |
            +------------+----------+----------+----------+
            |   order_id |     uuid |     true |       [] |
            | created_at | datetime |     true |       [] |
            | updated_at | datetime |     true |       [] |
            |   discount |    float |     true |       [] |
            |    address |     json |     true |       [] |
            |      notes |     json |     true |       [] |
            |      items |     json |     true |       [] |
            +------------+----------+----------+----------+
            7 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_on_excel(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xlsx', '--output-table' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputEquals(<<<'OUTPUT'
            +------------+--------+----------+----------+
            |       name |   type | nullable | metadata |
            +------------+--------+----------+----------+
            |   order_id |   uuid |     true |       [] |
            | created_at | string |     true |       [] |
            | updated_at | string |     true |       [] |
            |   discount | string |     true |       [] |
            |    address |   json |     true |       [] |
            |      notes |   json |     true |       [] |
            |      items |   json |     true |       [] |
            +------------+--------+----------+----------+
            7 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_on_json(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.json',
            '--output-table' => true,
            '--input-file-limit' => 5,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +--------------+--------------+----------+----------+
            |         name |         type | nullable | metadata |
            +--------------+--------------+----------+----------+
            |     order_id |       string |     true |       [] |
            |   created_at |       string |     true |       [] |
            |   updated_at |       string |     true |       [] |
            | cancelled_at |       string |     true |       [] |
            |  total_price |        float |     true |       [] |
            |     discount |        float |     true |       [] |
            |     customer | structure_v2 |     true |       [] |
            |      address | structure_v2 |     true |       [] |
            |        notes |         list |     true |       [] |
            +--------------+--------------+----------+----------+
            9 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_on_parquet(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.parquet',
            '--output-table' => true,
            '--input-file-limit' => 5,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +------------+--------------+----------+----------+
            |       name |         type | nullable | metadata |
            +------------+--------------+----------+----------+
            |   order_id |         uuid |    false |       [] |
            | created_at |     datetime |    false |       [] |
            | updated_at |     datetime |     true |       [] |
            |   discount |        float |     true |       [] |
            |      email |       string |    false |       [] |
            |   customer |       string |    false |       [] |
            |    address | structure_v2 |    false |       [] |
            |      notes |         list |    false |       [] |
            |      items |         list |    false |       [] |
            +------------+--------------+----------+----------+
            9 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_on_txt(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.txt',
            '--output-table' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +------+--------+----------+----------+
            | name |   type | nullable | metadata |
            +------+--------+----------+----------+
            | text | string |    false |       [] |
            +------+--------+----------+----------+
            1 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_table_output_on_xml(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.xml',
            '--input-xml-node-path' => 'root/row',
            '--output-table' => true,
            '--input-file-limit' => 5,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +------+------+----------+----------+
            | name | type | nullable | metadata |
            +------+------+----------+----------+
            | node |  xml |    false |       [] |
            +------+------+----------+----------+
            1 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_union_by_name_unions_the_column_sets(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::unionGlob(),
            '--output-ascii' => true,
            '--schema-union-by-name' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- a: ?integer
            |-- b: ?integer
            |-- c: ?integer

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_with_zero_offset(): void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 0,
            '--output-ascii' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        // Zero offset should behave same as no offset
        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- order_id: ?uuid
            |-- created_at: ?datetime
            |-- updated_at: ?datetime
            |-- discount: ?float
            |-- address: ?json
            |-- notes: ?json
            |-- items: ?json

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_without_union_by_name_reports_the_sniffed_columns(): void
    {
        // columnsDiverge() fires while rows are read, and schema() reads none - the divergence
        // refusal is still raised by `flow read` over the same glob
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute([
            'input-file' => SchemaInferenceFixtureContext::unionGlob(),
            '--output-ascii' => true,
        ]);

        $tester->assertCommandIsSuccessful();
        self::assertCommandOutputIdentical("schema\n|-- a: ?integer\n|-- b: ?integer\n", $tester->getDisplay());
    }

    public function test_file_schema_command_registers_the_schema_alias(): void
    {
        $application = new Application();
        $application->addCommands([new FileSchemaCommand()]);

        static::assertInstanceOf(FileSchemaCommand::class, $application->find('schema'));
    }
}
