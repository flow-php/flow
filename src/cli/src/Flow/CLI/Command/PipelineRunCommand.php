<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\option_bool;
use function Flow\ETL\DSL\analyze;
use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\Command\Traits\{ConfigOptions, StatisticsOptions};
use Flow\CLI\Formatter\PipelineReportFormatter;
use Flow\CLI\Options\ConfigOption;
use Flow\CLI\PipelineFactory;
use Flow\ETL\Exception\{Exception};
use Flow\ETL\{Config};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class PipelineRunCommand extends Command
{
    use ConfigOptions;
    use StatisticsOptions;

    private ?Config $flowConfig = null;

    private ?Path $pipelinePath = null;

    public function configure() : void
    {
        $this
            ->setName('run')
            ->setDescription('Execute data processing pipeline from a php file.')
            ->setHelp(
                <<<'HELP'
<info>pipeline-file</info> argument must point to a valid php file that returns DataFrame instance.
<comment>Make sure to not execute run() or any other trigger function.</comment>

<fg=blue>Example of pipeline.php:</>
<?php
return df()
    ->read(from_array([
        ['id' => 1, 'name' => 'User 01', 'active' => true],
        ['id' => 2, 'name' => 'User 02', 'active' => false],
        ['id' => 3, 'name' => 'User 03', 'active' => true],
    ]))
    ->collect()
    ->write(to_output());
HELP
            )
            ->addArgument('pipeline-file', InputArgument::REQUIRED, 'Path to a php/json with DataFrame definition.')
            ->addOption('analyze', null, InputOption::VALUE_OPTIONAL, 'Collect processing statistics and print them.', false);

        $this->addConfigOptions($this);
        $this->addStatisticsOptions($this);
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        $analyze = option_bool('analyze', $input) ? analyze() : false;

        if ($analyze && option_bool('stats-schema', $input)) {
            $analyze->withSchema();
        }

        if ($analyze && option_bool('stats-columns', $input)) {
            $analyze->withColumnStatistics();
        }

        try {
            ob_start();
            $df = match ($this->pipelinePath->extension()) {
                'php' => (new PipelineFactory($this->pipelinePath))->fromPHP(),
            };
            $report = $df->run(analyze: $analyze);

            $style->writeln(ob_get_clean());

            if ($report !== null) {
                (new PipelineReportFormatter($report, $style, $input))->format();
            }

        } catch (Exception $exception) {
            $style->error($exception->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = (new ConfigOption('config'))->get($input);
        $this->pipelinePath = (new FilePathArgument('pipeline-file'))->getExisting($input, $this->flowConfig);
    }
}
