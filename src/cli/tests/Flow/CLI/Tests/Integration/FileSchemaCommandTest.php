<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileSchemaCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileSchemaCommandTest extends TestCase
{
    public function test_run_schema() : void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['source' => __DIR__ . '/Fixtures/orders.csv']);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
[{"ref":"order_id","type":{"type":"uuid","nullable":false},"metadata":[]},{"ref":"created_at","type":{"type":"scalar","scalar_type":"string","nullable":false},"metadata":[]},{"ref":"updated_at","type":{"type":"scalar","scalar_type":"string","nullable":false},"metadata":[]},{"ref":"discount","type":{"type":"scalar","scalar_type":"string","nullable":true},"metadata":[]},{"ref":"address","type":{"type":"json","nullable":false},"metadata":[]},{"ref":"notes","type":{"type":"json","nullable":false},"metadata":[]},{"ref":"items","type":{"type":"json","nullable":false},"metadata":[]}]

OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_run_schema_with_pretty_output() : void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['source' => __DIR__ . '/Fixtures/orders.csv', '--pretty' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
[
    {
        "ref": "order_id",
        "type": {
            "type": "uuid",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "created_at",
        "type": {
            "type": "scalar",
            "scalar_type": "string",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "updated_at",
        "type": {
            "type": "scalar",
            "scalar_type": "string",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "discount",
        "type": {
            "type": "scalar",
            "scalar_type": "string",
            "nullable": true
        },
        "metadata": []
    },
    {
        "ref": "address",
        "type": {
            "type": "json",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "notes",
        "type": {
            "type": "json",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "items",
        "type": {
            "type": "json",
            "nullable": false
        },
        "metadata": []
    }
]

OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_run_schema_with_table_output() : void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['source' => __DIR__ . '/Fixtures/orders.csv', '--table' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
+------------+--------+----------+-------------+----------+
|       name |   type | nullable | scalar_type | metadata |
+------------+--------+----------+-------------+----------+
|   order_id |   uuid |    false |             |       [] |
| created_at | scalar |    false |      string |       [] |
| updated_at | scalar |    false |      string |       [] |
|   discount | scalar |     true |      string |       [] |
|    address |   json |    false |             |       [] |
|      notes |   json |    false |             |       [] |
|      items |   json |    false |             |       [] |
+------------+--------+----------+-------------+----------+
7 rows

OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_run_schema_with_table_output_and_autocast() : void
    {
        $tester = new CommandTester(new FileSchemaCommand('file:schema'));

        $tester->execute(['source' => __DIR__ . '/Fixtures/orders.csv', '--table' => true, '--auto-cast' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
+------------+----------+----------+-------------+----------+
|       name |     type | nullable | scalar_type | metadata |
+------------+----------+----------+-------------+----------+
|   order_id |     uuid |    false |             |       [] |
| created_at | datetime |    false |             |       [] |
| updated_at | datetime |    false |             |       [] |
|   discount |   scalar |     true |      string |       [] |
|    address |     json |    false |             |       [] |
|      notes |     json |    false |             |       [] |
|      items |     json |    false |             |       [] |
+------------+----------+----------+-------------+----------+
7 rows

OUTPUT,
            $tester->getDisplay()
        );
    }
}
