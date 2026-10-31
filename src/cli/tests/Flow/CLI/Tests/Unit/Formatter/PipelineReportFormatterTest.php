<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit\Formatter;

use Flow\CLI\Command\PipelineRunCommand;
use Flow\CLI\Formatter\PipelineReportFormatter;
use Flow\ETL\Runtime\Metrics\Processes;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ReportMother;
use Flow\ETL\Values\Memory\Unit;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Style\SymfonyStyle;

final class PipelineReportFormatterTest extends FlowTestCase
{
    public function test_a_launched_run_prints_the_worker_peak_and_the_peak_sum(): void
    {
        $input = new ArrayInput(['pipeline-file' => 'pipeline.php'], (new PipelineRunCommand('run'))->getDefinition());
        $output = new BufferedOutput();

        (new PipelineReportFormatter(ReportMother::of(new Processes(Unit::fromMb(25), [
            Unit::fromMb(40),
            Unit::fromMb(41),
        ])), new SymfonyStyle($input, $output), $input))->format();

        $display = $output->fetch();
        static::assertMatchesRegularExpression('/Heap Peak\s+[\d.]+ MB/', $display);
        static::assertMatchesRegularExpression('/Process Peak\s+25 MB/', $display);
        static::assertMatchesRegularExpression('/Worker Peak\s+41 MB/', $display);
        static::assertMatchesRegularExpression('/Peak Sum\s+106 MB/', $display);
    }

    public function test_a_serial_run_prints_no_worker_rows(): void
    {
        $input = new ArrayInput(['pipeline-file' => 'pipeline.php'], (new PipelineRunCommand('run'))->getDefinition());
        $output = new BufferedOutput();

        (new PipelineReportFormatter(
            ReportMother::of(new Processes(Unit::fromMb(25), [])),
            new SymfonyStyle($input, $output),
            $input,
        ))->format();

        $display = $output->fetch();
        static::assertMatchesRegularExpression('/Process Peak\s+25 MB/', $display);
        static::assertStringNotContainsString('Worker Peak', $display);
        static::assertStringNotContainsString('Peak Sum', $display);
    }
}
