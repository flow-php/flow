<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use function Flow\Types\DSL\{type_null, type_string, type_union};
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\{InputArgument, InputInterface, InputOption};
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(name: 'flow:filesystem:mv', description: 'Move a file between two URIs on the same fstab.', aliases: ['flow:fs:mv'])]
final class MvCommand extends Command
{
    public function __construct(private readonly FstabResolver $resolver)
    {
        parent::__construct();
    }

    protected function configure() : void
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

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        try {
            $rawSource = type_string()->assert($input->getArgument('source'));
            $rawDest = type_string()->assert($input->getArgument('destination'));
            $fstabName = type_union(type_string(), type_null())->assert($input->getOption('fstab'));

            $table = $this->resolver->resolve($fstabName);
            $source = $this->resolver->parseUri($rawSource);
            $dest = $this->resolver->parseUri($rawDest);

            CpCommand::streamCopy($table, $source, $dest, $fstabName ?? $this->resolver->defaultFstabName());

            $table->for($source)->rm($source);
        } catch (\Throwable $e) {
            $io->getErrorStyle()->error($e->getMessage());

            return Command::FAILURE;
        }

        $io->success(\sprintf('Moved %s → %s', $source->uri(), $dest->uri()));

        return Command::SUCCESS;
    }
}
