<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Examples;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use NorbertTech\StaticContentGeneratorBundle\Content\SourceProvider;

final class ExamplesSourceProvider implements SourceProvider
{
    public function __construct(private readonly Examples $examples)
    {

    }

    public function all(): array
    {
        $sources = [];
        foreach ($this->examples->topics() as $topic) {
            $sources[] = new Source('topic', ['topic' => $topic]);

            foreach ($this->examples->examples($topic) as $example) {
                $sources[] = new Source('example', ['topic' => $topic, 'example' => $example]);
            }
        }
        return $sources;
    }
}