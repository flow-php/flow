<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Flow\Website\Service\FlowConfigFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;

#[AsCommand(name: 'app:generate:flow-completer', description: 'Generate CodeMirror completer for Flow methods')]
final class GenerateFlowCompleterCommand extends Command
{
    public function __construct(
        private readonly Environment $twig,
        private readonly string $projectDir,
        private readonly FlowConfigFactory $configFactory,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Generating Flow Methods Completer');

        $apiJsonPath = $this->projectDir . '/../../web/landing/resources/api.json';
        $dslJsonPath = $this->projectDir . '/../../web/landing/resources/dsl.json';

        $fs = $this->configFactory->filesystem();

        if ($fs->status(path($apiJsonPath)) === null) {
            $io->error("API JSON file not found: {$apiJsonPath}");

            return Command::FAILURE;
        }

        if ($fs->status(path($dslJsonPath)) === null) {
            $io->error("DSL JSON file not found: {$dslJsonPath}");

            return Command::FAILURE;
        }

        $flowReturningFunctions = df($this->configFactory->configBuilder('flow_completer'))
            ->read(from_json($dslJsonPath))
            ->collect()
            ->filter(ref('return_type')->isNotNull())
            ->withEntry(
                'has_flow_return',
                ref('return_type')
                    ->onEach(
                        ref('element')
                            ->arrayGet('name')
                            ->equals(lit('Flow'))
                            ->and(ref('element')->arrayGet('namespace')->equals(lit('Flow\\ETL'))),
                    )
                    ->arrayKeep(true)
                    ->size()
                    ->greaterThan(lit(0)),
            )
            ->filter(ref('has_flow_return')->equals(lit(true)))
            ->fetch()
            ->reduceToArray('name');

        $io->info(\sprintf('Found %d DSL functions returning Flow', \count($flowReturningFunctions)));

        $methodsData = df($this->configFactory->configBuilder('flow_completer'))
            ->read(from_json($apiJsonPath))
            ->collect()
            ->filter(ref('class_slug')->equals(lit('flow')))
            ->select('name', 'class', 'class_slug', 'parameters', 'return_type', 'doc_comment')
            ->fetch()
            ->toArray();

        $io->info(\sprintf('Found %d Flow methods', \count($methodsData)));

        $codeMirrorContent = $this->twig->render('completers/flow-codemirror.js.twig', [
            'flow_methods' => $methodsData,
            'flow_functions' => $flowReturningFunctions,
            'generated_at' => new \DateTime(),
            'total_count' => \count($methodsData),
        ]);

        $codeMirrorOutputFile = $this->projectDir . '/assets/codemirror/completions/flow.js';

        $fs->writeTo(path($codeMirrorOutputFile))->append($codeMirrorContent)->close();

        $io->success("Generated Flow completer: {$codeMirrorOutputFile}");
        $io->info('Flow methods: ' . \count($methodsData));
        $io->info('Flow-returning functions: ' . \count($flowReturningFunctions));

        return Command::SUCCESS;
    }
}
