<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use DateTime;
use Flow\Website\Service\FlowConfigFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

use function array_column;
use function count;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;
use function sprintf;

#[AsCommand(
    name: 'app:generate:data-frame-completer',
    description: 'Generate CodeMirror completer for DataFrame methods',
)]
final class GenerateDataFrameCompleterCommand extends Command
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

        $io->title('Generating DataFrame Methods Completer');

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

        $flowReturningFunctions = df($this->configFactory->configBuilder('dataframe_completer'))
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

        $io->info(sprintf('Found %d DSL functions returning Flow', count($flowReturningFunctions)));

        $dataFrameReturningMethodsArray = df($this->configFactory->configBuilder('dataframe_completer'))
            ->read(from_json($apiJsonPath))
            ->collect()
            ->filter(ref('return_type')->isNotNull())
            ->withEntry(
                'returns_dataframe',
                ref('return_type')
                    ->onEach(
                        ref('element')
                            ->arrayGet('name')
                            ->equals(lit('DataFrame'))
                            ->and(ref('element')->arrayGet('namespace')->equals(lit('Flow\\ETL'))),
                    )
                    ->arrayKeep(true)
                    ->size()
                    ->greaterThan(lit(0))
                    ->or(
                        ref('return_type')
                            ->onEach(ref('element')->arrayGet('name')->equals(lit('self')))
                            ->arrayKeep(true)
                            ->size()
                            ->greaterThan(lit(0))
                            ->and(ref('class_slug')->equals(lit('dataframe'))),
                    ),
            )
            ->filter(ref('returns_dataframe')->equals(lit(true)))
            ->select('class_slug', 'name')
            ->groupBy([ref('class_slug')])
            ->aggregate(collect('name'))
            ->fetch()
            ->toArray();

        $dataFrameReturningMethods = array_column($dataFrameReturningMethodsArray, 'name_collection', 'class_slug');

        $io->info(sprintf('Found %d methods returning DataFrame', count($dataFrameReturningMethods)));

        // Pass raw data to template - Twig handles formatting
        $methodsData = df($this->configFactory->configBuilder('dataframe_completer'))
            ->read(from_json($apiJsonPath))
            ->collect()
            ->filter(ref('class_slug')->equals(lit('dataframe')))
            ->select('name', 'class', 'class_slug', 'parameters', 'return_type', 'doc_comment')
            ->fetch()
            ->toArray();

        $io->info(sprintf('Found %d DataFrame methods', count($methodsData)));

        $content = $this->twig->render('completers/dataframe-codemirror.js.twig', [
            'dataframe_methods' => $methodsData,
            'dataframe_returning_methods' => $dataFrameReturningMethods,
            'generated_at' => new DateTime(),
            'total_count' => count($methodsData),
        ]);

        $outputFile = $this->projectDir . '/assets/codemirror/completions/dataframe.js';

        $fs->writeTo(path($outputFile))->append($content)->close();

        $io->success("Generated DataFrame completer: {$outputFile}");
        $io->info('DataFrame methods: ' . count($methodsData));
        $io->info('DataFrame-returning methods: ' . count($dataFrameReturningMethods));

        return Command::SUCCESS;
    }
}
