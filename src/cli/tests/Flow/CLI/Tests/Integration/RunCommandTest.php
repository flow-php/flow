<?php

declare(strict_types=1);

namespace Flow\Cli\Tests\Integration;

use Flow\CLI\Command\RunCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Exception\RuntimeException;
use Symfony\Component\Console\Tester\CommandTester;

final class RunCommandTest extends TestCase
{
    public function test_run_command() : void
    {
        $tester = new CommandTester(new RunCommand('run'));

        ob_start();

        $tester->execute(['input-file' => __DIR__ . '/Fixtures/pipeline.php']);

        $output = ob_get_clean();

        $tester->assertCommandIsSuccessful();

        self::assertSame(
            <<<'OUTPUT'
+----+---------+--------+
| id |    name | active |
+----+---------+--------+
|  1 | User 01 |   true |
|  2 | User 02 |  false |
|  3 | User 03 |   true |
+----+---------+--------+
3 rows

OUTPUT,
            $output
        );
    }

    public function test_run_command_without_pipeline_input_file_provided() : void
    {
        $tester = new CommandTester(new RunCommand('run'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not enough arguments (missing: "input-file").');

        $tester->execute([]);
    }
}
