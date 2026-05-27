<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Command;

use Flow\PostgreSql\Migrations\Configuration as MigrationsConfiguration;
use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\VersionResolver;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function count;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function sprintf;

#[AsCommand(name: 'flow:migrations:migrate', description: 'Execute migrations')]
final class MigrateCommand extends Command
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
            ->addArgument(
                'version',
                InputArgument::OPTIONAL,
                'The version to migrate to (first, prev, next, latest, or version string)',
                'latest',
            )
            ->addOption('connection', 'c', InputOption::VALUE_OPTIONAL, 'The connection to use', null)
            ->addOption('dry-run', null, InputOption::VALUE_NONE, 'Execute migration as a dry run')
            ->addOption('all-or-nothing', null, InputOption::VALUE_NONE, 'Wrap the entire migration in a transaction');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $connection = type_string()->assert($input->getOption('connection') ?? $this->defaultConnection);
        $migrator = type_instance_of(Migrator::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.migrator",
        ));
        $resolver = type_instance_of(VersionResolver::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.version_resolver",
        ));
        $configuration = type_instance_of(MigrationsConfiguration::class)->assert($this->container->get(
            "flow.postgresql.{$connection}.migrations.configuration",
        ));
        $versionAlias = type_string()->assert($input->getArgument('version'));
        $dryRun = $input->getOption('dry-run') === true;
        $allOrNothing = $input->getOption('all-or-nothing') ? true : null;

        $io->title('Migrate' . ($dryRun ? ' (dry run)' : ''));

        $io->definitionList(
            ['Connection' => "<fg=cyan>{$connection}</>"],
            ['Target' => "<fg=cyan>{$versionAlias}</>"],
            ['Directory' => "<fg=gray>{$configuration->migrationsDirectory}</>"],
        );

        $statuses = $migrator->status();
        $pendingCount = count($statuses->pending());

        if (count($statuses) === 0) {
            $io->success('No migrations found.');

            return Command::SUCCESS;
        }

        if ($pendingCount === 0 && $versionAlias === 'latest') {
            $io->success('Already up to date.');

            return Command::SUCCESS;
        }

        $statusRows = [];

        foreach ($statuses as $migration) {
            $stateLabel = match ($migration->state) {
                MigrationState::EXECUTED => '<fg=green>EXECUTED</>',
                MigrationState::PENDING => '<fg=yellow>PENDING</>',
                MigrationState::UNAVAILABLE => '<fg=red>UNAVAILABLE</>',
            };
            $statusRows[] = [$stateLabel, (string) $migration->version, $migration->name];
        }

        $io->table(['Status', 'Version', 'Name'], $statusRows);

        $version = $resolver->resolve($versionAlias);

        if ($input->isInteractive() && !$io->confirm(sprintf('Migrate to version <fg=cyan>%s</>?', $version), false)) {
            $io->warning('Migration cancelled.');

            return Command::SUCCESS;
        }

        $results = $migrator->migrate($version, $dryRun, $allOrNothing);

        if (count($results) === 0) {
            $io->success('Already up to date.');

            return Command::SUCCESS;
        }

        $rows = [];
        $totalTime = 0;

        foreach ($results as $result) {
            $totalTime += $result->executionTimeMs;
            $direction = $result->direction === Direction::UP ? '<fg=green>UP</>' : '<fg=yellow>DOWN</>';

            if ($result->error !== null) {
                $rows[] = [$direction, (string) $result->version, '<fg=red>FAILED</>', $result->executionTimeMs . 'ms'];
                $io->table(['Direction', 'Version', 'Result', 'Time'], $rows);
                $io->error(sprintf('%s failed: %s', $result->version, $result->error->getMessage()));

                return Command::FAILURE;
            }

            if ($result->skipped) {
                $rows[] = [$direction, (string) $result->version, '<fg=gray>SKIPPED</>', '-'];
            } else {
                $rows[] = [$direction, (string) $result->version, '<fg=green>OK</>', $result->executionTimeMs . 'ms'];
            }
        }

        $io->table(['Direction', 'Version', 'Result', 'Time'], $rows);

        if ($dryRun) {
            $io->note('Dry run completed. No changes were applied.');
        } else {
            $io->success(sprintf('%d migration(s) executed in %dms.', count($results), $totalTime));
        }

        return Command::SUCCESS;
    }
}
