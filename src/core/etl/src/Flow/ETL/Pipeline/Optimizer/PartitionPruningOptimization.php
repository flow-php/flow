<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\PartitionsExtractor;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Paths;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Any;
use Flow\ETL\Function\CompositeScalarFunction;
use Flow\ETL\Function\CompositeScalarFunction\CompositeScalarFunctionIterator;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Loader;
use Flow\ETL\Partition\ScalarFunctionFilter;
use Flow\ETL\Pipeline;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

final class PartitionPruningOptimization implements Optimization
{
    /**
     * @var array<string, Paths>
     */
    private array $paths = [];

    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        $extractor = $pipeline->source();

        return $extractor instanceof PartitionsExtractor
            && $extractor instanceof FileExtractor
            && $extractor->source()->isPattern()
            && $element instanceof ScalarFunctionFilterTransformer
            && $element->function instanceof ScalarFunctionChain
            && (
                $element->function->getRootFunction() instanceof Reference
                || $element->function->getRootFunction() instanceof CompositeScalarFunction
            );
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        /**
         * @var ScalarFunctionFilterTransformer $element
         * @var FileExtractor&PartitionsExtractor $extractor
         */
        $extractor = $pipeline->source();

        $paths = $this->paths($extractor);

        /**
         * @var ScalarFunctionChain $filterFunction
         */
        $filterFunction = $element->function;

        /**
         * @var All|Any|Reference $root
         */
        $root = $filterFunction->getRootFunction();

        if ($root instanceof Reference && $root instanceof ScalarFunction) {
            if ($paths->partitions()->has($root->name())) {
                $extractor->addPartitionFilter(new ScalarFunctionFilter($filterFunction, $this->entryFactory));

                return $pipeline;
            }
        }

        if ($root instanceof CompositeScalarFunction) {
            foreach ((new CompositeScalarFunctionIterator($root))->getIterator() as $subFunction) {
                if ($subFunction instanceof Reference && $subFunction instanceof ScalarFunction) {
                    if ($paths->partitions()->has($subFunction->name())) {
                        $extractor->addPartitionFilter(new ScalarFunctionFilter($filterFunction, $this->entryFactory));

                        return $pipeline;
                    }
                }
            }
        }

        return $pipeline->add($element);
    }

    private function paths(PartitionsExtractor&FileExtractor $extractor) : Paths
    {
        if (\array_key_exists($extractor->source()->uri(), $this->paths)) {
            return $this->paths[$extractor->source()->uri()];
        }

        $this->paths[$extractor->source()->uri()] = new Paths(...\iterator_to_array($this->filesystem->scan($extractor->source())));

        return $this->paths[$extractor->source()->uri()];
    }
}
