<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\{FileReadCommand};
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileReadCommandTest extends TestCase
{
    public function test_read_rows_csv() : void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----------------------+----------------------+----------------------+----------+----------------------+----------------------+----------------------+
|             order_id |           created_at |           updated_at | discount |              address |                notes |                items |
+----------------------+----------------------+----------------------+----------+----------------------+----------------------+----------------------+
| e13d7098-5a78-3389-9 | 2024-06-17T19:24:49+ | 2024-06-17T19:24:49+ |    12.45 | {"street":"9742 Jask | ["Doloremque cum et  | [{"sku":"SKU_0003"," |
| 947df050-3abb-3f5a-9 | 2024-02-23T19:18:53+ | 2024-02-23T19:18:53+ |          | {"street":"37051 Ale | ["Neque dolor et min | [{"sku":"SKU_0004"," |
| 6315f9e2-86bf-3321-a | 2024-04-02T11:30:25+ | 2024-04-02T11:30:25+ |     47.1 | {"street":"792 Golda | ["Et porro fugiat fu | [{"sku":"SKU_0003"," |
| 4cccb632-fade-34e2-8 | 2024-05-06T00:17:57+ | 2024-05-06T00:17:57+ |    19.76 | {"street":"30203 Wal | ["Aliquam saepe iste | [{"sku":"SKU_0004"," |
| 82384f8c-9adb-38be-9 | 2024-05-10T11:17:41+ | 2024-05-10T11:17:41+ |          | {"street":"757 Tobin | ["Beatae nesciunt au | [{"sku":"SKU_0005"," |
+----------------------+----------------------+----------------------+----------+----------------------+----------------------+----------------------+
5 rows
OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_read_rows_json() : void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.json', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----------------------+----------------------+----------------------+--------------+-------------+----------+----------------------+----------------------+----------------------+
|             order_id |           created_at |           updated_at | cancelled_at | total_price | discount |             customer |              address |                notes |
+----------------------+----------------------+----------------------+--------------+-------------+----------+----------------------+----------------------+----------------------+
| e5e0299f-152e-4c1b-b | 2023-10-02T23:59:16+ | 2023-10-10T11:43:41+ |              |      170.05 |    32.09 | {"name":"Adah","last | {"street":"73121 Swi | ["Sit dolor quas aut |
| 139aa6b6-872b-47a8-b | 2023-05-20T08:59:30+ | 2023-10-12T03:24:25+ |              |      239.94 |    47.79 | {"name":"Kasandra"," | {"street":"5864 Kael | ["Architecto quod cu |
| 35d90c5c-c524-4b24-a | 2023-05-13T13:56:02+ | 2023-09-28T10:27:33+ |              |      148.38 |     3.08 | {"name":"Shaina","la | {"street":"651 Okune | ["Sit voluptates sin |
| e84a65ff-4438-4275-8 | 2023-10-03T00:27:46+ | 2023-10-10T07:59:28+ |              |      384.49 |     7.88 | {"name":"Dane","last | {"street":"7465 Spor | ["Id illo autem eaqu |
| 86f3d0ca-a047-4866-9 | 2023-08-06T21:54:08+ | 2023-10-05T13:15:17+ |              |      265.44 |    32.37 | {"name":"Mireille"," | {"street":"671 Korbi | ["Dolorem accusantiu |
+----------------------+----------------------+----------------------+--------------+-------------+----------+----------------------+----------------------+----------------------+
5 rows
OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_read_rows_parquet() : void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.parquet', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----------------------+----------------------+----------------------+--------------+-------------+------------+----------------------+----------------------+----------------------+
|             order_id |           created_at |           updated_at | cancelled_at | total_price |   discount |             customer |              address |                notes |
+----------------------+----------------------+----------------------+--------------+-------------+------------+----------------------+----------------------+----------------------+
| 9354bda0-02b7-3820-9 | 2023-10-02T23:59:16+ | 2023-10-10T11:43:41+ |              | 170.0500031 | 32.0900002 | {"name":"Adah","last | {"street":"73121 Swi |     [null,null,null] |
| 8a7c3f29-e669-3aba-8 | 2023-05-20T08:59:30+ | 2023-10-12T03:24:25+ |              | 239.9400024 | 47.7900009 | {"name":"Kasandra"," | {"street":"5864 Kael |     [null,null,null] |
| fd35921c-85ca-30c1-a | 2023-05-13T13:56:02+ | 2023-09-28T10:27:33+ |              | 148.3800049 |  3.0799999 | {"name":"Shaina","la | {"street":"651 Okune |               [null] |
| a86b1747-73d4-3ed8-b | 2023-10-03T00:27:46+ | 2023-10-10T07:59:28+ |              | 384.4899902 |  7.8800001 | {"name":"Dane","last | {"street":"7465 Spor | [null,null,null,null |
| 10544dfc-405a-3913-9 | 2023-08-06T21:54:08+ | 2023-10-05T13:15:17+ |              | 265.4400024 | 32.3699989 | {"name":"Mireille"," | {"street":"671 Korbi |               [null] |
+----------------------+----------------------+----------------------+--------------+-------------+------------+----------------------+----------------------+----------------------+
5 rows
OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_read_rows_text() : void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.txt', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----------------------+
|                 text |
+----------------------+
| order_id,created_at, |
| e13d7098-5a78-3389-9 |
| 947df050-3abb-3f5a-9 |
| 6315f9e2-86bf-3321-a |
| 4cccb632-fade-34e2-8 |
+----------------------+
5 rows
OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_read_rows_xml() : void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xml', '--input-file-limit' => 5, '--input-xml-node-path' => 'root/row']);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----------------------+
|                 node |
+----------------------+
| <row>     <order_id/ |
| <row>     <order_id/ |
| <row>     <order_id/ |
| <row>     <order_id/ |
| <row>     <order_id/ |
+----------------------+
5 rows
OUTPUT,
            $tester->getDisplay()
        );
    }
}
