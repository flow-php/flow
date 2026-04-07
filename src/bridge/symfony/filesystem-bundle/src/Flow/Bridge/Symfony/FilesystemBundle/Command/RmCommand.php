<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_boolean, type_null, type_string, type_union};
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:rm', description: 'Delete a file or directory URI on a configured fstab.', aliases: ['flow:fs:rm'])]
final class RmCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    protected function configure() : void
    {
        $this
            ->addArgument('path', InputArgument::REQUIRED, 'File or directory URI')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->addOption('recursive', 'r', InputOption::VALUE_NONE, 'Required to delete a directory.')
            ->setHelp('Removes a file or directory URI. Use --recursive to delete a directory.');
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawPath = type_string()->assert($input->getArgument('path'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));
            $recursive = type_boolean()->assert($input->getOption('recursive'));

            $table = $this->resolver->resolve($fstabName);
            $path = $this->resolver->parseUri($rawPath);
            $filesystem = $table->for($path);

            $status = $filesystem->status($path);

            if ($status === null) {
                $io->getErrorStyle()->error(\sprintf('Path not found: %s', $path->uri()));

                return Command::FAILURE;
            }

            if ($status->isDirectory() && !$recursive) {
                $io->getErrorStyle()->error(\sprintf('%s is a directory; pass --recursive to delete it.', $path->uri()));

                return Command::FAILURE;
            }

            $filesystem->rm($path);
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $io->success(\sprintf('Removed %s', $path->uri()));

        return Command::SUCCESS;
    }
}
