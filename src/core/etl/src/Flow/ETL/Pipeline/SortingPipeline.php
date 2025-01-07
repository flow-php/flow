<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Exception\OutOfMemoryException;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Row\References;
use Flow\ETL\Sort\{ExternalSort, MemorySort};
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

final readonly class SortingPipeline implements Pipeline
{
    public function __construct(private Pipeline $pipeline, private References $refs)
    {
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        try {
            if ($context->config->sort->algorithm->useMemory() && $context->config->sort->memoryLimit->isGreaterThan(Unit::fromBytes(0))) {
                $extractor = (new MemorySort($context->config->sort->memoryLimit))
                    ->sortBy($this->pipeline, $context, $this->refs);
            } else {
                $extractor = (new ExternalSort(
                    new ExternalSort\BucketsCache\FilesystemBucketsCache(
                        $context->filesystem(protocol('file')),
                        $context->config->serializer(),
                        100,
                        $context->config->cache->localFilesystemCacheDir->suffix('/flow-php-external-sort/')
                    ),
                    $context->config->cache->externalSortBucketsCount
                )
                )->sortBy($this->pipeline, $context, $this->refs);
            }
        } catch (OutOfMemoryException) {
            $extractor = (new ExternalSort(
                new ExternalSort\BucketsCache\FilesystemBucketsCache(
                    $context->filesystem(protocol('file')),
                    $context->config->serializer(),
                    100,
                    $context->config->cache->localFilesystemCacheDir->suffix('/flow-php-external-sort/')
                ),
                $context->config->cache->externalSortBucketsCount
            )
            )->sortBy($this->pipeline, $context, $this->refs);
        }

        return $extractor->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
