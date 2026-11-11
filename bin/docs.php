#!/usr/bin/env php
<?php

declare(strict_types=1);

use Composer\Autoload\ClassLoader;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\ClassesExtractor;
use Flow\Documentation\FunctionsExtractor;
use Flow\Documentation\Models\FunctionModel;
use Flow\ETL\Flow;
use JetBrains\PHPStormStub\PhpStormStubsMap;
use Roave\BetterReflection\BetterReflection;
use Roave\BetterReflection\Reflector\DefaultReflector;
use Roave\BetterReflection\Reflector\Exception\IdentifierNotFound;
use Roave\BetterReflection\Reflector\Reflector;
use Roave\BetterReflection\SourceLocator\Ast\Locator;
use Roave\BetterReflection\SourceLocator\SourceStubber\PhpStormStubsSourceStubber;
use Roave\BetterReflection\SourceLocator\Type\AggregateSourceLocator;
use Roave\BetterReflection\SourceLocator\Type\ComposerSourceLocator;
use Roave\BetterReflection\SourceLocator\Type\MemoizingSourceLocator;
use Roave\BetterReflection\SourceLocator\Type\PhpInternalSourceLocator;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

require __DIR__ . '/../vendor/autoload.php';

if (version_compare(PHP_VERSION, '8.4', '>=')) {
    print PHP_EOL . 'Building docs can be run only on the lowest supported version of PHP: 8.3' . PHP_EOL;

    exit(1);
}

if (false === in_array(PHP_SAPI, ['cli', 'phpdbg', 'embed'], true)) {
    print PHP_EOL . 'This app may only be invoked from a command line, got "' . PHP_SAPI . '"' . PHP_EOL;

    exit(1);
}

ini_set('memory_limit', -1);

$dslPaths = [
    __DIR__ . '/../src/core/etl/src/Flow/ETL/DSL/functions.php',
    __DIR__ . '/../src/core/etl/src/Flow/Floe/DSL/functions.php',
    __DIR__ . '/../src/core/etl/src/Flow/Serializer/DSL/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-avro/src/Flow/ETL/Adapter/Avro/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-chartjs/src/Flow/ETL/Adapter/ChartJS/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-csv/src/Flow/ETL/Adapter/CSV/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-doctrine/src/Flow/ETL/Adapter/Doctrine/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-excel/src/Flow/ETL/Adapter/Excel/DSL/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-google-sheet/src/Flow/ETL/Adapter/GoogleSheet/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-http/src/Flow/ETL/Adapter/Http/DSL/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-json/src/Flow/ETL/Adapter/JSON/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-parquet/src/Flow/ETL/Adapter/Parquet/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-postgresql/src/Flow/ETL/Adapter/PostgreSql/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-seal/src/Flow/ETL/Adapter/Seal/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-text/src/Flow/ETL/Adapter/Text/functions.php',
    __DIR__ . '/../src/adapter/etl-adapter-xml/src/Flow/ETL/Adapter/XML/functions.php',
    __DIR__ . '/../src/lib/filesystem/src/Flow/Filesystem/DSL/functions.php',
    __DIR__ . '/../src/lib/types/src/Flow/Types/DSL/functions.php',
    __DIR__ . '/../src/lib/parquet/src/Flow/Parquet/DSL/functions.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/DSL/schema.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/DSL/query.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/DSL/condition.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/DSL/parser.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/DSL/client.php',
    __DIR__ . '/../src/lib/postgresql/src/Flow/PostgreSql/Migrations/DSL/functions.php',
    __DIR__ . '/../src/lib/telemetry/src/Flow/Telemetry/DSL/functions.php',
    __DIR__ . '/../src/lib/azure-sdk/src/Flow/Azure/SDK/DSL/functions.php',
    __DIR__ . '/../src/bridge/filesystem/azure/src/Flow/Filesystem/Bridge/Azure/DSL/functions.php',
    __DIR__ . '/../src/bridge/filesystem/async-aws/src/Flow/Filesystem/Bridge/AsyncAWS/DSL/functions.php',
    __DIR__ . '/../src/bridge/filesystem/sftp/src/Flow/Filesystem/Bridge/SFTP/DSL/functions.php',
    __DIR__ . '/../src/bridge/monolog/telemetry/src/Flow/Bridge/Monolog/Telemetry/DSL/functions.php',
    __DIR__ . '/../src/bridge/symfony/console/src/Flow/Bridge/Symfony/Console/DSL/functions.php',
    __DIR__
        . '/../src/bridge/symfony/http-foundation-telemetry/src/Flow/Bridge/Symfony/HttpFoundationTelemetry/DSL/functions.php',
    __DIR__ . '/../src/bridge/symfony/telemetry-bundle/src/Flow/Bridge/Symfony/TelemetryBundle/DSL/functions.php',
    __DIR__ . '/../src/bridge/psr7/telemetry/src/Flow/Bridge/Psr7/Telemetry/DSL/functions.php',
    __DIR__ . '/../src/bridge/psr18/telemetry/src/Flow/Bridge/Psr18/Telemetry/DSL/functions.php',
    __DIR__ . '/../src/bridge/telemetry/otlp/src/Flow/Bridge/Telemetry/OTLP/DSL/functions.php',
];

// The PHP the playground runs: keep in sync with PHP_VERSION and the enabled extensions in wasm/build.sh.
$playgroundPhpVersionId = 80512;
$playgroundPhpExtensions = [
    'Core',
    'date',
    'pcre',
    'zlib',
    'bcmath',
    'uri',
    'standard',
    'filter',
    'hash',
    'iconv',
    'json',
    'mbstring',
    'Zend OPcache',
    'libxml',
    'SPL',
    'Phar',
    'random',
    'Reflection',
    'snappy',
    'tokenizer',
    'dom',
    'xml',
    'xmlreader',
    'xmlwriter',
    'zip',
];

$betterReflection = new BetterReflection();
$reflector = new DefaultReflector(new MemoizingSourceLocator(new AggregateSourceLocator([
    new ComposerSourceLocator(
        ClassLoader::getRegisteredLoaders()[dirname(__DIR__) . '/vendor'],
        $betterReflection->astLocator(),
    ),
    new PhpInternalSourceLocator($betterReflection->astLocator(), $betterReflection->sourceStubber()),
])));

$application = new Application('Flow-PHP - Documentation');

$application->addCommand(new class($dslPaths, $reflector, $betterReflection->astLocator()) extends Command {
    /**
     * @param list<non-empty-string> $dslPaths
     */
    public function __construct(
        private readonly array $dslPaths,
        private readonly Reflector $reflector,
        private readonly Locator $astLocator,
    ) {
        parent::__construct();
    }

    public function configure(): void
    {
        $this
            ->setName('dsl:dump')
            ->setDescription('Dump DSL into json file.')
            ->addOption(
                'repository-root-path',
                null,
                InputArgument::OPTIONAL,
                'Repository root path.',
                dirname(__DIR__) . '/',
            )
            ->addArgument('output', InputArgument::REQUIRED, 'Where to dump dsl.');
    }

    public function execute(InputInterface $input, OutputInterface $output): int
    {
        $extractor = new FunctionsExtractor(
            (string) $input->getOption('repository-root-path'),
            $this->reflector,
            $this->astLocator,
        );

        $normalizedFunctions = [];

        foreach ($extractor->extract($this->dslPaths) as $function) {
            if (($attribute = $function->attributes->findByName('DocumentationDSL')) !== null) {
                if ($attribute->arguments['module'] === Module::DEPRECATED) {
                    continue;
                }
            }

            $normalizedFunctions[] = $function->normalize();
        }

        file_put_contents(
            __DIR__ . '/../' . ltrim((string) $input->getArgument('output'), '/'),
            json_encode($normalizedFunctions),
        );

        return Command::SUCCESS;
    }
});

$application->addCommand(new class($dslPaths, $reflector, $betterReflection->astLocator()) extends Command {
    /**
     * @param list<non-empty-string> $dslPaths
     */
    public function __construct(
        private readonly array $dslPaths,
        private readonly Reflector $reflector,
        private readonly Locator $astLocator,
    ) {
        parent::__construct();
    }

    public function configure(): void
    {
        $this
            ->setName('api:dump')
            ->setDescription('Dump public API of Flow and of every class reachable from DSL functions into json file.')
            ->addArgument('output', InputArgument::REQUIRED, 'Where to dump classes.');
    }

    public function execute(InputInterface $input, OutputInterface $output): int
    {
        $repositoryRootPath = dirname(__DIR__) . '/';
        $roots = [Flow::class];

        foreach ((new FunctionsExtractor(
            $repositoryRootPath,
            $this->reflector,
            $this->astLocator,
        ))->extract($this->dslPaths) as $function) {
            foreach ([...$function->returnType->types, ...$function->parameters->types()] as $type) {
                if ($type->namespace !== null) {
                    $roots[] = $type->namespace . '\\' . $type->name;
                }
            }
        }

        $normalizedClasses = [];

        foreach ((new ClassesExtractor($repositoryRootPath, $this->reflector))->extract($roots) as $class) {
            $normalizedClasses[] = $class->normalize();
        }

        file_put_contents(
            __DIR__ . '/../' . ltrim((string) $input->getArgument('output'), '/'),
            json_encode($normalizedClasses),
        );

        return Command::SUCCESS;
    }
});

$application->addCommand(new class($playgroundPhpVersionId, $playgroundPhpExtensions) extends Command {
    /**
     * @param list<string> $phpExtensions
     */
    public function __construct(
        private readonly int $phpVersionId,
        private readonly array $phpExtensions,
    ) {
        parent::__construct();
    }

    public function configure(): void
    {
        $this
            ->setName('php:dump')
            ->setDescription('Dump PHP functions and classes available in the playground into json file.')
            ->addArgument('output', InputArgument::REQUIRED, 'Where to dump functions and classes.');
    }

    public function execute(InputInterface $input, OutputInterface $output): int
    {
        $betterReflection = new BetterReflection();
        $reflector = new DefaultReflector(
            new PhpInternalSourceLocator(
                $betterReflection->astLocator(),
                new PhpStormStubsSourceStubber($betterReflection->phpParser(), $this->phpVersionId),
            ),
        );
        $functions = [];

        foreach (type_map(type_string(), type_string())->assert(PhpStormStubsMap::FUNCTIONS) as $name => $stubPath) {
            // PS_UNRESERVE_PREFIX_ functions stand in for language constructs such as isset() or list().
            if (!$this->isPlaygroundStub($stubPath) || str_starts_with($name, 'PS_UNRESERVE_PREFIX_')) {
                continue;
            }

            try {
                $reflectionFunction = $reflector->reflectFunction($name);
            } catch (IdentifierNotFound) {
                continue;
            }

            $functions[] = FunctionModel::fromReflection('', $reflectionFunction)->normalize();
        }

        $classes = [];

        foreach ((new ClassesExtractor('', $reflector, $this->phpExtensions))->extract(array_keys(array_filter(
            type_map(type_string(), type_string())->assert(PhpStormStubsMap::CLASSES),
            $this->isPlaygroundStub(...),
        ))) as $class) {
            $classes[] = $class->normalize();
        }

        file_put_contents(__DIR__ . '/../' . ltrim((string) $input->getArgument('output'), '/'), json_encode([
            'functions' => $functions,
            'classes' => $classes,
        ]));

        return Command::SUCCESS;
    }

    private function isPlaygroundStub(string $stubPath): bool
    {
        return in_array(explode('/', $stubPath)[0], $this->phpExtensions, true);
    }
});

$application->run();
