<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution\Plan;

use Flow\ETL\Extractor;
use Flow\ETL\Loader;

final class FilesystemOperations
{
    /**
     * @param array<Loader> $loaders
     */
    public function __construct(private readonly Extractor $extractor, private readonly array $loaders)
    {
    }

    /**
     * @return array<Extractor\FileExtractor>
     */
    public function fileExtractors() : array
    {
        return $this->getFileExtractors($this->extractor);
    }

    /**
     * @return array<Loader\FileLoader>
     */
    public function fileLoaders() : array
    {
        $fileLoaders = [];

        foreach ($this->loaders as $loader) {
            $fileLoaders[] = $this->getFileLoader($loader);
        }

        return \array_merge(...$fileLoaders);
    }

    /**
     * @param Extractor $extractor
     *
     * @return array<Extractor\FileExtractor>
     */
    private function getFileExtractors(Extractor $extractor) : array
    {
        if ($extractor instanceof Extractor\FileExtractor) {
            return [$extractor];
        }

        if ($extractor instanceof Extractor\OverridingExtractor) {
            $extractors = [];

            foreach ($extractor->extractors() as $nextExtractor) {
                $extractors[] = $this->getFileExtractors($nextExtractor);
            }

            return \array_merge(...$extractors);
        }

        return [];
    }

    /**
     * @param Loader $loader
     *
     * @return array<Loader\FileLoader>
     */
    private function getFileLoader(Loader $loader) : array
    {
        if ($loader instanceof Loader\FileLoader) {
            return [$loader];
        }

        if ($loader instanceof Loader\OverridingLoader) {
            $loaders = [];

            foreach ($loader->loaders() as $nextLoader) {
                $loaders[] = $this->getFileLoader($nextLoader);
            }

            return \array_merge(...$loaders);
        }

        return [];
    }
}
