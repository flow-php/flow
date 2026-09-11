<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\SchemaFormatCommand;
use Flow\ETL\Tests\CommandOutputNormalizer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;

final class SchemaFormatCommandTest extends TestCase
{
    use CommandOutputNormalizer;

    public function test_run_schema_format(): void
    {
        $tester = new CommandTester(new SchemaFormatCommand('schema:format'));

        $tester->execute(['input-schema-file' => __DIR__ . '/Fixtures/schema.json']);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            [
                {
                    "ref": "order_id",
                    "type": {
                        "type": "uuid"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "created_at",
                    "type": {
                        "type": "datetime"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "updated_at",
                    "type": {
                        "type": "datetime"
                    },
                    "nullable": false,
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
                    "ref": "email",
                    "type": {
                        "type": "string"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "customer",
                    "type": {
                        "type": "string"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "address",
                    "type": {
                        "type": "structure_v2",
                        "fields": [
                            {
                                "name": "street",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            },
                            {
                                "name": "city",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            },
                            {
                                "name": "zip",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            },
                            {
                                "name": "country",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            }
                        ],
                        "allow_extra": false
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "notes",
                    "type": {
                        "type": "list",
                        "element": {
                            "type": "string"
                        }
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "items",
                    "type": {
                        "type": "list",
                        "element": {
                            "type": "structure_v2",
                            "fields": [
                                {
                                    "name": "sku",
                                    "type": {
                                        "type": "string"
                                    },
                                    "optional": false
                                },
                                {
                                    "name": "quantity",
                                    "type": {
                                        "type": "integer"
                                    },
                                    "optional": false
                                },
                                {
                                    "name": "price",
                                    "type": {
                                        "type": "float"
                                    },
                                    "optional": false
                                }
                            ],
                            "allow_extra": false
                        }
                    },
                    "nullable": false,
                    "metadata": []
                }
            ]

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_format_ascii(): void
    {
        $tester = new CommandTester(new SchemaFormatCommand('schema:format'));

        $tester->execute(['input-schema-file' => __DIR__ . '/Fixtures/schema.json', '--output-ascii' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            schema
            |-- order_id: uuid
            |-- created_at: datetime
            |-- updated_at: datetime
            |-- discount: ?float
            |-- email: string
            |-- customer: string
            |-- address: structure
            |    |-- street: string
            |    |-- city: string
            |    |-- zip: string
            |    |-- country: string
            |-- notes: list<string>
            |-- items: list<structure{sku: string, quantity: integer, price: float}>

            OUTPUT, $tester->getDisplay());
    }

    public function test_run_schema_format_php(): void
    {
        $tester = new CommandTester(new SchemaFormatCommand('schema:format'));

        $tester->execute(['input-schema-file' => __DIR__ . '/Fixtures/schema.json', '--output-php' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'PHP'
            \Flow\ETL\DSL\schema(
                \Flow\ETL\DSL\uuid_schema("order_id", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\datetime_schema("created_at", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\datetime_schema("updated_at", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\float_schema("discount", nullable: true, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\string_schema("email", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\string_schema("customer", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\structure_schema("address", type: \Flow\Types\DSL\type_structure(elements: ["street" => \Flow\Types\DSL\type_string(), "city" => \Flow\Types\DSL\type_string(), "zip" => \Flow\Types\DSL\type_string(), "country" => \Flow\Types\DSL\type_string()]), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\list_schema("notes", type: \Flow\Types\DSL\type_list(element: \Flow\Types\DSL\type_string()), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
                \Flow\ETL\DSL\list_schema("items", type: \Flow\Types\DSL\type_list(element: \Flow\Types\DSL\type_structure(elements: ["sku" => \Flow\Types\DSL\type_string(), "quantity" => \Flow\Types\DSL\type_integer(), "price" => \Flow\Types\DSL\type_float()])), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
            );

            PHP, $tester->getDisplay());
    }

    public function test_run_schema_format_table(): void
    {
        $tester = new CommandTester(new SchemaFormatCommand('schema:format'));

        $tester->execute(['input-schema-file' => __DIR__ . '/Fixtures/schema.json', '--output-table' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +------------+--------------+----------+----------+
            |       name |         type | nullable | metadata |
            +------------+--------------+----------+----------+
            |   order_id |         uuid |    false |       [] |
            | created_at |     datetime |    false |       [] |
            | updated_at |     datetime |    false |       [] |
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

    public function test_schema_format_command_registers_the_format_alias(): void
    {
        $application = new Application();
        $application->addCommands([new SchemaFormatCommand()]);

        static::assertInstanceOf(SchemaFormatCommand::class, $application->find('format'));
    }
}
