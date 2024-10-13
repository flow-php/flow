<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Integration;

use Flow\CLI\Command\PipelineRunCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Tester\CommandTester;

final class PipelineRunCommandTest extends TestCase
{
    public function test_run_and_analyze_command() : void
    {
        $tester = new CommandTester(new PipelineRunCommand('run'));

        $tester->execute(['pipeline-file' => __DIR__ . '/Fixtures/pipeline.php', '--analyze' => true]);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
            <<<'OUTPUT'
+----+---------+--------+
| id |    name | active |
+----+---------+--------+
|  1 | User 01 |   true |
|  2 | User 02 |  false |
|  3 | User 03 |   true |
+----+---------+--------+
3 rows

Total Processed Rows: 3
OUTPUT,
            $tester->getDisplay()
        );
    }

    public function test_run_command() : void
    {
        $tester = new CommandTester(new PipelineRunCommand('run'));

        $tester->execute(['pipeline-file' => __DIR__ . '/Fixtures/pipeline.php']);

        $tester->assertCommandIsSuccessful();

        self::assertStringContainsString(
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
            $tester->getDisplay()
        );
    }

    public function test_run_command_without_pipeline_input_file_provided() : void
    {
        $tester = new CommandTester(new PipelineRunCommand('run'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument \'pipeline-file\' is required.');

        $tester->execute([]);
    }
}
