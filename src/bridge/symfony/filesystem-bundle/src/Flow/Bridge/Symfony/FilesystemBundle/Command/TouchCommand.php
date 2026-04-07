<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_boolean, type_null, type_string, type_union};
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:touch', description: 'Create an empty file at a URI.', aliases: ['flow:fs:touch'])]
final class TouchCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    protected function configure() : void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'File URI')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->addOption('force', 'F', InputOption::VALUE_NONE, 'Overwrite the file with empty content if it already exists.')
            ->setHelp('Creates an empty file. Refuses on existing files unless --force is given.');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));
            $force = type_boolean()->assert($input->getOption('force'));

            $table = $this->resolver->resolve($fstabName);
            $path = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($path);

            if ($filesystem->status($path) !== null && !$force) {
                $io->getErrorStyle()->error(\sprintf('File already exists: %s. Use --force to overwrite.', $path->uri()));

                return Command::FAILURE;
            }

            $stream = $filesystem->writeTo($path);
            $stream->append('');
            $stream->close();
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $io->success(\sprintf('Touched %s', $path->uri()));

        return Command::SUCCESS;
    }
}
