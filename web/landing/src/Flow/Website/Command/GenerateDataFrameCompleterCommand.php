<?php

declare(strict_types=1);

namespace Flow\Website\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Twig\Environment;

#[AsCommand(
    name: 'app:generate:data-frame-completer',
    description: 'Generate CodeMirror completer for DataFrame methods'
)]
final class GenerateDataFrameCompleterCommand extends Command
{
    public function __construct(
        private readonly Environment $twig,
        private readonly string $projectDir,
    ) {
        parent::__construct();
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $io = new SymfonyStyle($input, $output);

        $io->title('Generating DataFrame Methods Completer');

        $apiJsonPath = $this->projectDir . '/../../web/landing/resources/api.json';
        $dslJsonPath = $this->projectDir . '/../../web/landing/resources/dsl.json';

        if (!\file_exists($apiJsonPath)) {
            $io->error("API JSON file not found: {$apiJsonPath}");

            return Command::FAILURE;
        }

        if (!\file_exists($dslJsonPath)) {
            $io->error("DSL JSON file not found: {$dslJsonPath}");

            return Command::FAILURE;
        }

        $apiMethods = \json_decode(\file_get_contents($apiJsonPath), true, 512, JSON_THROW_ON_ERROR);
        $dslFunctions = \json_decode(\file_get_contents($dslJsonPath), true, 512, JSON_THROW_ON_ERROR);

        if (!\is_array($apiMethods)) {
            $io->error('Invalid API JSON structure');

            return Command::FAILURE;
        }

        if (!\is_array($dslFunctions)) {
            $io->error('Invalid DSL JSON structure');

            return Command::FAILURE;
        }

        // Extract DSL functions that return Flow (df, data_frame)
        $flowReturningFunctions = $this->extractFlowReturningFunctions($dslFunctions);
        $io->info(\sprintf('Found %d DSL functions returning Flow', \count($flowReturningFunctions)));

        // Extract methods that return DataFrame
        $dataFrameReturningMethods = $this->extractDataFrameReturningMethods($apiMethods);
        $io->info(\sprintf('Found %d methods returning DataFrame', \count($dataFrameReturningMethods)));

        // Filter only DataFrame methods
        $dataFrameMethods = \array_filter(
            $apiMethods,
            static fn (array $method) : bool => $method['class_slug'] === 'dataframe'
        );

        $io->info(\sprintf('Found %d DataFrame methods', \count($dataFrameMethods)));

        // No fallback needed - DataFrame completer only shows DataFrame methods

        $methodsData = \array_map(
            fn (array $method) : array => $this->buildMethodData($method),
            $dataFrameMethods
        );

        $content = $this->twig->render('completers/dataframe-codemirror.js.twig', [
            'dataframe_methods' => $methodsData,
            'dataframe_returning_methods' => $dataFrameReturningMethods,
            'generated_at' => new \DateTime(),
            'total_count' => \count($methodsData),
        ]);

        $outputFile = $this->projectDir . '/assets/codemirror/completions/dataframe.js';
        $outputDir = \dirname($outputFile);

        if (!\is_dir($outputDir)) {
            \mkdir($outputDir, 0755, true);
            $io->info("Created directory: {$outputDir}");
        }

        \file_put_contents($outputFile, $content);

        $io->success("Generated DataFrame completer: {$outputFile}");
        $io->info('DataFrame methods: ' . \count($methodsData));
        $io->info('DataFrame-returning methods: ' . \count($dataFrameReturningMethods));

        return Command::SUCCESS;
    }

    /**
     * @param array{name: string, type?: array, has_default_value: bool, default_value?: ?string} $param
     */
    private function buildHighlightedParam(array $param) : string
    {
        $paramStr = '';

        if (!empty($param['type'])) {
            $paramStr .= '<span class="fn-type">' . $this->formatType($param['type']) . '</span> ';
        }

        $paramStr .= '<span class="fn-param">$' . $param['name'] . '</span>';

        if ($param['has_default_value'] && isset($param['default_value'])) {
            $paramStr .= ' <span class="fn-operator">=</span> <span class="fn-default">' . \htmlspecialchars($param['default_value']) . '</span>';
        }

        return $paramStr;
    }

    /**
     * @param array<string, mixed> $method
     */
    private function buildHighlightedSignature(array $method) : string
    {
        $params = \array_map(
            fn (array $param) : string => $this->buildHighlightedParam($param),
            $method['parameters']
        );

        $signature = '<span class="fn-name">' . $method['name'] . '</span>'
            . '<span class="fn-operator">(</span>'
            . \implode('<span class="fn-operator">,</span> ', $params)
            . '<span class="fn-operator">)</span>';

        if (!empty($method['return_type'])) {
            $signature .= ' <span class="fn-operator">:</span> '
                . '<span class="fn-return">' . $this->formatType($method['return_type']) . '</span>';
        }

        return $signature;
    }

    /**
     * @param array<string, mixed> $method
     *
     * @return array<string, mixed>
     */
    private function buildMethodData(array $method) : array
    {
        return [
            'name' => $method['name'],
            'snippet' => $this->buildSnippet($method),
            'docComment' => $this->formatDocComment($method['doc_comment'] ?? null),
            'highlightedSignature' => $this->buildHighlightedSignature($method),
            'meta' => $method['class_slug'],
            'className' => $method['class'],
            'parameters' => $method['parameters'],
        ];
    }

    /**
     * @param array<string, mixed> $method
     */
    private function buildSnippet(array $method) : string
    {
        $params = $method['parameters'];
        $methodName = $method['name'];

        if (empty($params)) {
            return $methodName . '()';
        }

        $snippetParams = [];
        $tabstop = 1;

        foreach ($params as $param) {
            $typeHint = !empty($param['type'])
                ? $this->formatType($param['type']) . ' '
                : '';

            $snippetParams[] = '${' . $tabstop . ':' . $typeHint . '$' . $param['name'] . '}';
            $tabstop++;
        }

        return $methodName . '(' . \implode(', ', $snippetParams) . ')';
    }

    /**
     * Extract methods that return DataFrame from API methods.
     * Returns array grouped by class: ['flow' => ['extract', 'read'], 'dataframe' => ['filter', 'select']].
     *
     * @param array<array<string, mixed>> $apiMethods
     *
     * @return array<string, array<string>>
     */
    private function extractDataFrameReturningMethods(array $apiMethods) : array
    {
        $methodsByClass = [];

        foreach ($apiMethods as $method) {
            if (empty($method['return_type'])) {
                continue;
            }

            foreach ($method['return_type'] as $returnType) {
                // Check if return type is DataFrame or self (from DataFrame class)
                $isDataFrame = $returnType['name'] === 'DataFrame' && $returnType['namespace'] === 'Flow\\ETL';
                $isSelfFromDataFrame = $returnType['name'] === 'self' && $method['class_slug'] === 'dataframe';

                if ($isDataFrame || $isSelfFromDataFrame) {
                    $classSlug = $method['class_slug'];

                    if (!isset($methodsByClass[$classSlug])) {
                        $methodsByClass[$classSlug] = [];
                    }
                    $methodsByClass[$classSlug][] = $method['name'];

                    break;
                }
            }
        }

        return $methodsByClass;
    }

    /**
     * Extract function names that return Flow from DSL functions.
     *
     * @param array<array<string, mixed>> $dslFunctions
     *
     * @return array<string>
     */
    private function extractFlowReturningFunctions(array $dslFunctions) : array
    {
        $flowFunctions = [];

        foreach ($dslFunctions as $function) {
            if (empty($function['return_type'])) {
                continue;
            }

            foreach ($function['return_type'] as $returnType) {
                // Check if return type is Flow (Flow\ETL\Flow)
                if ($returnType['name'] === 'Flow' && $returnType['namespace'] === 'Flow\\ETL') {
                    $flowFunctions[] = $function['name'];

                    break;
                }
            }
        }

        return $flowFunctions;
    }

    private function formatDocComment(?string $docComment) : string
    {
        if ($docComment === null) {
            return '';
        }

        $decoded = \base64_decode($docComment, true);

        if ($decoded === false) {
            return '';
        }

        $docComment = \preg_replace('/^\/\*\*|\*\/$/', '', $decoded);
        $lines = \explode("\n", (string) $docComment);
        $lines = \array_map(static fn (string $line) : string => \preg_replace('/^\s*\*\s?/', '', $line), $lines);
        $lines = \array_filter($lines, static fn (string $line) : bool => \trim($line) !== '');

        return \implode('<br>', $lines);
    }

    /**
     * @param array<array{name: string}> $types
     */
    private function formatType(array $types) : string
    {
        return \implode('|', \array_map(static fn (array $t) : string => $t['name'], $types));
    }
}
