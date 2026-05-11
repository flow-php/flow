<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

use function Flow\Filesystem\DSL\file_move;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

#[AsCommand(
    name: 'flow:filesystem:mv',
    description: 'Move a file between two URIs on the same fstab.',
    aliases: ['flow:fs:mv'],
)]
final class MvCommand extends Command
{
    public function __construct(
        private readonly FstabResolver $resolver,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addArgument('source', InputArgument::REQUIRED, 'Source file URI')
            ->addArgument('destination', InputArgument::REQUIRED, 'Destination file URI')
            ->addOption('fstab', 'f', InputOption::VALUE_REQUIRED, 'Fstab name; defaults to the bundle default fstab.')
            ->setHelp(<<<'HELP'
                Moves a file from one URI to another by copying then deleting the source.
                Both URIs MUST resolve through the same fstab. The source is deleted only
                after the destination write completes successfully.
                HELP);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawSource = type_string()->assert($input->getArgument('source'));
            $rawDest = type_string()->assert($input->getArgument('destination'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));

            $table = $this->resolver->resolve($fstabName);
            $source = $this->resolver->parseUri($rawSource);
            $dest = $this->resolver->parseUri($rawDest);
            $activeFstab = $fstabName ?? $this->resolver->defaultFstabName();

            try {
                $sourceFs = $table->for($source);
            } catch (\Throwable $e) {
                throw new InvalidArgumentException(
                    \sprintf('in fstab "%s": source: %s', $activeFstab, $e->getMessage()),
                    0,
                    $e,
                );
            }

            try {
                $table->for($dest);
            } catch (\Throwable $e) {
                throw new InvalidArgumentException(
                    \sprintf('in fstab "%s": destination: %s', $activeFstab, $e->getMessage()),
                    0,
                    $e,
                );
            }

            $sourceStatus = $sourceFs->status($source);

            if ($sourceStatus === null) {
                throw new InvalidArgumentException(\sprintf('Source not found: %s', $source->uri()));
            }

            if ($sourceStatus->isDirectory()) {
                throw new InvalidArgumentException(\sprintf('Refusing to move directory: %s', $source->uri()));
            }

            file_move($table)->execute($source, $dest);
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $io->success(\sprintf('Moved %s → %s', $source->uri(), $dest->uri()));

        return Command::SUCCESS;
    }
}
