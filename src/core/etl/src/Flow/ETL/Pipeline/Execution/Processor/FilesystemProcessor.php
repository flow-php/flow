<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution\Processor;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Pipeline\Execution\LogicalPlan;
use Flow\ETL\Pipeline\Pipes;

final class FilesystemProcessor implements Processor
{
    public function process(LogicalPlan $plan, FlowContext $context) : LogicalPlan
    {
        $operations = $plan->filesystemOperations();

        foreach ($operations->fileExtractors() as $extractor) {
            if (!$context->streams()->fs()->exists($extractor->source())) {
                throw new RuntimeException('Not existing path used to extract data: ' . $extractor->source()->uri());
            }
        }

        /** @var array<FileLoader> $removeLoaders */
        $removeLoaders = [];

        foreach ($operations->fileLoaders() as $loader) {
            if ($context->mode() === SaveMode::ExceptionIfExists) {
                if ($context->streams()->fs()->exists($loader->destination())) {
                    throw new RuntimeException('Destination path "' . $loader->destination()->uri() . '" already exists, please change path to different or set different SaveMode');
                }
            }

            if ($context->mode() === SaveMode::Overwrite) {
                if ($context->streams()->fs()->exists($loader->destination())) {
                    $context->streams()->fs()->rm($loader->destination());

                    continue;
                }
            }

            if ($context->mode() === SaveMode::Ignore) {
                if ($context->streams()->fs()->exists($loader->destination())) {
                    $removeLoaders[] = $loader;

                    continue;
                }
            }

            if ($context->mode() === SaveMode::Append) {
                if (!$context->threadSafe()) {
                    throw new RuntimeException('Appending to destination "' . $loader->destination()->uri() . '" in non thread safe mode is not supported.');
                }

                if ($context->streams()->fs()->fileExists($loader->destination())) {
                    throw new RuntimeException('Appending to existing single file destination "' . $loader->destination()->uri() . '" is not supported.');
                }
            }
        }

        $newPipes = Pipes::empty();

        foreach ($plan->pipes->all() as $pipe) {
            $keep = true;

            foreach ($removeLoaders as $removeLoader) {
                if ($pipe === $removeLoader) {
                    $keep = false;
                }
            }

            if ($keep) {
                $newPipes->add($pipe);
            }
        }

        return new LogicalPlan($plan->extractor, $newPipes);
    }
}
