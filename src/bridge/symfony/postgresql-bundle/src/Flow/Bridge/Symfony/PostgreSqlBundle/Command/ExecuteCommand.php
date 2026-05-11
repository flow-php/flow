<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Version;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

#[AsCommand(name: 'flow:migrations:execute', description: 'Execute a single migration')]
final class ExecuteCommand extends Command
{
    public function __construct(
        private readonly ContainerInterface $container,
        private readonly string $defaultConnection,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('version', InputArgument::REQUIRED, 'The migration version to execute')
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Execute migration as a dry run')
            ->addOption('up', null, InputOption::VALUE_NONE, 'Execute the migration up (default)')
            ->addOption('down', null, InputOption::VALUE_NONE, 'Execute the migration down');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $migrator = type_instance_of(Migrator::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.migrator",
        ));
        $versionString = type_string()->assert($input->getArgument('version'));
        $version = Version::fromString($versionString);
        $dryRun = (bool) $input->getOption('dry-run');
        $direction = $input->getOption('down') ? Direction::DOWN : Direction::UP;
        $directionLabel = $direction === Direction::UP ? '<fg=green>UP</>' : '<fg=yellow>DOWN</>';

        $io->title('Execute Migration' . ($dryRun ? ' (dry run)' : ''));

        $io->definitionList(['Version' => "<fg=cyan>{$version}</>"], [
            'Direction' => $direction === Direction::UP ? '<fg=green>UP</>' : '<fg=yellow>DOWN</>',
        ]);

        if (
            $input->isInteractive()
            && !$io->confirm(
                \sprintf('Execute migration %s %s?', $direction === Direction::UP ? 'UP' : 'DOWN', $version),
                false,
            )
        ) {
            $io->warning('Execution cancelled.');

            return Command::SUCCESS;
        }

        $result = $migrator->executeVersion($version, $direction, $dryRun);

        if ($result->error !== null) {
            $io->error(\sprintf(
                '%s %s failed: %s',
                $direction === Direction::UP ? 'UP' : 'DOWN',
                $version,
                $result->error->getMessage(),
            ));

            return Command::FAILURE;
        }

        $io->table(['Direction', 'Version', 'Result', 'Time'], [
            [$directionLabel, (string) $version, '<fg=green>OK</>', $result->executionTimeMs . 'ms'],
        ]);

        if ($dryRun) {
            $io->note('Dry run completed. No changes were applied.');
        } else {
            $io->success(\sprintf('Migration %s executed successfully in %dms.', $version, $result->executionTimeMs));
        }

        return Command::SUCCESS;
    }
}
