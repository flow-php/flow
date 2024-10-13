<?php

declare(strict_types=1);

namespace Flow\CLI\Command;

use function Flow\CLI\option_bool;
use function Flow\ETL\DSL\config_builder;
use Flow\CLI\Arguments\FilePathArgument;
use Flow\CLI\PipelineFactory;
use Flow\ETL\Exception\{Exception};
use Flow\ETL\{Config};
use Flow\Filesystem\Path;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final class PipelineRunCommand extends Command
{
    private ?Config $flowConfig = null;

    private ?Path $pipelinePath = null;

    public function configure() : void
    {
        $this
            ->setName('run')
            ->setDescription('Execute ETL pipeline from a php/json file.')
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
            ->addOption('analyze', null, InputArgument::OPTIONAL, 'Collect processing statistics and print them.', false);
    }

    public function execute(InputInterface $input, OutputInterface $output) : int
    {
        $style = new SymfonyStyle($input, $output);

        try {
            ob_start();
            $df = match ($this->pipelinePath->extension()) {
                'php' => (new PipelineFactory($this->pipelinePath, $this->flowConfig))->fromPHP(),
                'json' => (new PipelineFactory($this->pipelinePath, $this->flowConfig))->fromJson(),
            };
            $report = $df->run(analyze: option_bool('analyze', $input));

            $style->writeln(ob_get_clean());

            if ($report !== null) {
                $style->writeln('Total Processed Rows: <info>' . \number_format($report->statistics()->totalRows()) . '</info>');
            }

        } catch (Exception $exception) {
            $style->error($exception->getMessage());

            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }

    protected function initialize(InputInterface $input, OutputInterface $output) : void
    {
        $this->flowConfig = config_builder()->build();
        $this->pipelinePath = (new FilePathArgument('pipeline-file'))->getExisting($input, $this->flowConfig);
    }
}
