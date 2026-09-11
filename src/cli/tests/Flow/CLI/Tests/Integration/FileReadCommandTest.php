<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\FileReadCommand;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\CommandOutputNormalizer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class FileReadCommandTest extends TestCase
{
    use CommandOutputNormalizer;

    public function test_read_rows_csv(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.csv', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            +----------------------+----------------------+----------------------+-----------+----------------------+----------------------+----------------------+
            |             order_id |           created_at |           updated_at |  discount |              address |                notes |                items |
            +----------------------+----------------------+----------------------+-----------+----------------------+----------------------+----------------------+
            | e13d7098-5a78-3389-9 | 2024-06-17T19:24:49+ | 2024-06-17T19:24:49+ | 12.450000 | {"street":"9742 Jask | ["Doloremque cum et  | [{"sku":"SKU_0003"," |
            | 947df050-3abb-3f5a-9 | 2024-02-23T19:18:53+ | 2024-02-23T19:18:53+ |           | {"street":"37051 Ale | ["Neque dolor et min | [{"sku":"SKU_0004"," |
            | 6315f9e2-86bf-3321-a | 2024-04-02T11:30:25+ | 2024-04-02T11:30:25+ | 47.100000 | {"street":"792 Golda | ["Et porro fugiat fu | [{"sku":"SKU_0003"," |
            | 4cccb632-fade-34e2-8 | 2024-05-06T00:17:57+ | 2024-05-06T00:17:57+ | 19.760000 | {"street":"30203 Wal | ["Aliquam saepe iste | [{"sku":"SKU_0004"," |
            | 82384f8c-9adb-38be-9 | 2024-05-10T11:17:41+ | 2024-05-10T11:17:41+ |           | {"street":"757 Tobin | ["Beatae nesciunt au | [{"sku":"SKU_0005"," |
            +----------------------+----------------------+----------------------+-----------+----------------------+----------------------+----------------------+
            5 rows
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_csv_with_all_strings(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 2,
            '--output-truncate' => 10,
            '--schema-all-strings' => true,
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            +------------+------------+------------+----------+------------+------------+------------+
            |   order_id | created_at | updated_at | discount |    address |      notes |      items |
            +------------+------------+------------+----------+------------+------------+------------+
            | e13d7098-5 | 2024-06-17 | 2024-06-17 |    12.45 | {"street": | ["Doloremq | [{"sku":"S |
            | 947df050-3 | 2024-02-23 | 2024-02-23 |          | {"street": | ["Neque do | [{"sku":"S |
            +------------+------------+------------+----------+------------+------------+------------+
            2 rows
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_csv_with_sample_size(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/inference/widening.csv',
            '--schema-sample-size' => 1,
        ]);

        $tester->assertCommandIsSuccessful();

        // The frozen ?integer schema truncates 1.5 - integer::cast(1.5) is 1
        self::assertCommandOutputContains(<<<'OUTPUT'
            +---+
            | a |
            +---+
            | 1 |
            | 1 |
            +---+
            2 rows
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_excel(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.xlsx', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
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
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_json(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.json', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +----------------------+----------------------+----------------------+--------------+-------------+-----------+----------------------+----------------------+----------------------+
            |             order_id |           created_at |           updated_at | cancelled_at | total_price |  discount |             customer |              address |                notes |
            +----------------------+----------------------+----------------------+--------------+-------------+-----------+----------------------+----------------------+----------------------+
            | e5e0299f-152e-4c1b-b | 2023-10-02T23:59:16+ | 2023-10-10T11:43:41+ |              |  170.050000 | 32.090000 | {"name":"Adah","last | {"street":"73121 Swi | ["Sit dolor quas aut |
            | 139aa6b6-872b-47a8-b | 2023-05-20T08:59:30+ | 2023-10-12T03:24:25+ |              |  239.940000 | 47.790000 | {"name":"Kasandra"," | {"street":"5864 Kael | ["Architecto quod cu |
            | 35d90c5c-c524-4b24-a | 2023-05-13T13:56:02+ | 2023-09-28T10:27:33+ |              |  148.380000 |  3.080000 | {"name":"Shaina","la | {"street":"651 Okune | ["Sit voluptates sin |
            | e84a65ff-4438-4275-8 | 2023-10-03T00:27:46+ | 2023-10-10T07:59:28+ |              |  384.490000 |  7.880000 | {"name":"Dane","last | {"street":"7465 Spor | ["Id illo autem eaqu |
            | 86f3d0ca-a047-4866-9 | 2023-08-06T21:54:08+ | 2023-10-05T13:15:17+ |              |  265.440000 | 32.370000 | {"name":"Mireille"," | {"street":"671 Korbi | ["Dolorem accusantiu |
            +----------------------+----------------------+----------------------+--------------+-------------+-----------+----------------------+----------------------+----------------------+
            5 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_parquet(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.parquet', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputIdentical(<<<'OUTPUT'
            +----------------------+----------------------+----------------------+-----------+----------------------+------------------+----------------------+----------------------+----------------------+
            |             order_id |           created_at |           updated_at |  discount |                email |         customer |              address |                notes |                items |
            +----------------------+----------------------+----------------------+-----------+----------------------+------------------+----------------------+----------------------+----------------------+
            | 4e93f175-0c89-30df-a | 2026-01-29T13:48:47+ | 2026-02-15T13:48:47+ | 27.969999 | fahey.aurelie@denesi |     Jerod Abbott | {"street":"5567 Grim | ["Maxime et sed impe | [{"sku":"SKU_0004"," |
            | db560d65-3a26-359e-8 | 2026-02-15T18:39:41+ |                      | 35.990002 |   dusty70@howell.com |    Omari McGlynn | {"street":"86184 Mck | ["Mollitia eos optio | [{"sku":"SKU_0001"," |
            | f24d7f7d-1615-3b5b-a | 2026-01-26T05:23:13+ | 2026-02-01T05:23:13+ | 42.669998 |   linnea91@gmail.com | Kendall Weissnat | {"street":"65268 Apr | ["Ut nesciunt volupt | [{"sku":"SKU_0003"," |
            | 78fe7069-f06f-3081-b | 2026-03-12T13:08:56+ |                      |           | glubowitz@morissette | Roman Balistreri | {"street":"1722 Hall | ["Velit vero invento | [{"sku":"SKU_0005"," |
            | e3fb781c-34ff-380f-8 | 2026-01-10T18:34:34+ | 2026-02-06T18:34:34+ |           |    walton60@hand.com | Sedrick Ondricka | {"street":"201 Mosci | ["Doloremque culpa i | [{"sku":"SKU_0004"," |
            +----------------------+----------------------+----------------------+-----------+----------------------+------------------+----------------------+----------------------+----------------------+
            5 rows

            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_text(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/orders.txt', '--input-file-limit' => 5]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
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
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_with_large_offset(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-offset' => 1000,
        ]);

        $tester->assertCommandIsSuccessful();

        $output = $tester->getDisplay();

        // Should show empty output as offset is larger than file
        static::assertEmpty(trim($output));
    }

    public function test_read_rows_with_offset_and_columns(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 4,
            '--input-file-offset' => 1,
            '--output-columns' => ['order_id', 'discount'],
        ]);

        $tester->assertCommandIsSuccessful();

        $output = $tester->getDisplay();

        // Should skip first row and show next 3 rows with only order_id and discount columns
        static::assertStringNotContainsString('e13d7098-5a78-3389-9', $output); // First row should not be displayed
        self::assertCommandOutputContains('947df050-3abb-3f5a-9', $output); // Second row should be first displayed
        self::assertCommandOutputContains('6315f9e2-86bf-3321-a', $output); // Third row should be displayed
        self::assertCommandOutputContains('4cccb632-fade-34e2-8', $output); // Fourth row should be displayed
        self::assertCommandOutputContains('3 rows', $output); // Should show 3 rows

        // Should only show selected columns
        self::assertCommandOutputContains('order_id', $output);
        self::assertCommandOutputContains('discount', $output);
        static::assertStringNotContainsString('created_at', $output);
        static::assertStringNotContainsString('address', $output);
    }

    public function test_read_rows_with_offset_csv(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 3,
            '--input-file-offset' => 2,
        ]);

        $tester->assertCommandIsSuccessful();

        $output = $tester->getDisplay();

        // Should display rows starting from offset 2 (third row)
        self::assertCommandOutputContains('6315f9e2-86bf-3321-a', $output); // Third row should be first displayed
        static::assertStringNotContainsString('e13d7098-5a78-3389-9', $output); // First row should not be displayed
        static::assertStringNotContainsString('947df050-3abb-3f5a-9', $output); // Second row should not be displayed
        self::assertCommandOutputContains('1 rows', $output); // Only 1 row should be displayed (limit 3 - offset 2)
    }

    public function test_read_rows_with_offset_zero(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 2,
            '--input-file-offset' => 0,
        ]);

        $tester->assertCommandIsSuccessful();

        $output = $tester->getDisplay();

        // Should behave same as no offset - show first 2 rows
        self::assertCommandOutputContains('e13d7098-5a78-3389-9', $output); // First row should be displayed
        self::assertCommandOutputContains('947df050-3abb-3f5a-9', $output); // Second row should be displayed
        self::assertCommandOutputContains('2 rows', $output);
    }

    public function test_read_rows_with_output_columns_empty_maintains_all_columns(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 2,
        ]);

        $tester->assertCommandIsSuccessful();

        // Should contain all original columns (order_id, created_at, updated_at, discount, address, notes, items)
        $output = $tester->getDisplay();
        self::assertCommandOutputContains('order_id', $output);
        self::assertCommandOutputContains('created_at', $output);
        self::assertCommandOutputContains('updated_at', $output);
        self::assertCommandOutputContains('discount', $output);
        self::assertCommandOutputContains('address', $output);
        self::assertCommandOutputContains('notes', $output);
        self::assertCommandOutputContains('items', $output);
        self::assertCommandOutputContains('2 rows', $output);
    }

    public function test_read_rows_with_output_columns_multiple_columns(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 3,
            '--output-columns' => ['order_id', 'discount', 'created_at'],
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            +----------------------+-----------+----------------------+
            |             order_id |  discount |           created_at |
            +----------------------+-----------+----------------------+
            | e13d7098-5a78-3389-9 | 12.450000 | 2024-06-17T19:24:49+ |
            | 947df050-3abb-3f5a-9 |           | 2024-02-23T19:18:53+ |
            | 6315f9e2-86bf-3321-a | 47.100000 | 2024-04-02T11:30:25+ |
            +----------------------+-----------+----------------------+
            3 rows
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_with_output_columns_nonexistent_column(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        // selecting a column the schema does not declare is an error, not an empty column - there is
        // no type to give it and no reader could see a value that row storage does not carry
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "nonexistent_column" not found');

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 2,
            '--output-columns' => ['nonexistent_column'],
        ]);
    }

    public function test_read_rows_with_output_columns_single_column(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.csv',
            '--input-file-limit' => 3,
            '--output-columns' => ['order_id'],
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            +----------------------+
            |             order_id |
            +----------------------+
            | e13d7098-5a78-3389-9 |
            | 947df050-3abb-3f5a-9 |
            | 6315f9e2-86bf-3321-a |
            +----------------------+
            3 rows
            OUTPUT, $tester->getDisplay());
    }

    public function test_read_rows_xml(): void
    {
        $tester = new CommandTester(new FileReadCommand('read'));

        $tester->execute([
            'input-file' => __DIR__ . '/Fixtures/orders.xml',
            '--input-file-limit' => 5,
            '--input-xml-node-path' => 'root/row',
        ]);

        $tester->assertCommandIsSuccessful();

        self::assertCommandOutputContains(<<<'OUTPUT'
            +----------------------+
            |                 node |
            +----------------------+
            | <row>    <order_id/> |
            | <row>    <order_id/> |
            | <row>    <order_id/> |
            | <row>    <order_id/> |
            | <row>    <order_id/> |
            +----------------------+
            5 rows
            OUTPUT, $tester->getDisplay());
    }
}
