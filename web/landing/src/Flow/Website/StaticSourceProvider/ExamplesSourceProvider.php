<?php

declare(strict_types=1);

namespace Flow\Website\StaticSourceProvider;

use Flow\Website\Service\Examples;
use NorbertTech\StaticContentGeneratorBundle\Content\Source;
use NorbertTech\StaticContentGeneratorBundle\Content\SourceProvider;

use function count;

final readonly class ExamplesSourceProvider implements SourceProvider
{
    public function __construct(
        private Examples $examples,
    ) {}

    public function all(): array
    {
        $sources = [];

        foreach ($this->examples->topics() as $topic) {
            $examples = $this->examples->examples($topic);

            foreach ($examples as $example) {
                $options = $this->examples->options($topic, $example);

                if (count($options) > 0) {
                    foreach ($options as $option) {
                        $sources[] = new Source('example_option', [
                            'topic' => $topic,
                            'example' => $example,
                            'option' => $option,
                        ]);
                        $sources[] = new Source('example_option_playground', [
                            'topic' => $topic,
                            'example' => $example,
                            'option' => $option,
                        ]);

                        foreach ($this->examples->dataFiles($topic, $example, $option) as $path) {
                            $sources[] = new Source('example_option_data', [
                                'topic' => $topic,
                                'example' => $example,
                                'option' => $option,
                                'path' => $path,
                            ]);
                        }
                    }
                } else {
                    $sources[] = new Source('example', ['topic' => $topic, 'example' => $example]);
                    $sources[] = new Source('example_playground', ['topic' => $topic, 'example' => $example]);

                    foreach ($this->examples->dataFiles($topic, $example) as $path) {
                        $sources[] = new Source('example_data', [
                            'topic' => $topic,
                            'example' => $example,
                            'path' => $path,
                        ]);
                    }
                }
            }
        }

        return $sources;
    }
}
