<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Examples;
use NorbertTech\StaticContentGeneratorBundle\Content\{Source, SourceProvider};

final class ExamplesSourceProvider implements SourceProvider
{
    public function __construct(private readonly Examples $examples)
    {

    }

    public function all() : array
    {
        $sources = [];

        foreach ($this->examples->topics() as $topic) {
            $examples = $this->examples->examples($topic);
            $firstExample = \current($examples);
            $firstExampleOptions = $this->examples->options($topic, $firstExample);

            // Only add topic source if the first example doesn't have options (no redirect)
            if (\count($firstExampleOptions) === 0) {
                $sources[] = new Source('topic', ['topic' => $topic]);
            }

            foreach ($examples as $example) {
                $options = $this->examples->options($topic, $example);

                if (\count($options) > 0) {
                    foreach ($options as $option) {
                        $sources[] = new Source('example_option', ['topic' => $topic, 'example' => $example, 'option' => $option]);
                    }
                } else {
                    $sources[] = new Source('example', ['topic' => $topic, 'example' => $example]);
                }
            }
        }

        return $sources;
    }
}
