<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration;

use Flow\Website\Service\Examples;
use PHPUnit\Framework\TestCase;

final class ExamplesTest extends TestCase
{
    public function test_description_merges_example_and_option_levels() : void
    {
        $path = __DIR__ . '/../Fixtures/WithMergedDescriptions';
        $service = new Examples($path);

        $result = $service->description('merged_topic', 'example_with_both', 'option_a');

        self::assertStringContainsString('Example level intro.', $result);
        self::assertStringContainsString('---', $result);
        self::assertStringContainsString('Option specific details.', $result);
    }

    public function test_description_returns_only_option_when_no_example_level() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        $result = $service->description('topic_3', 'example_1');

        // Valid fixture topic_3/example_1 is a 2-level example (no options), so no merge logic applies
        self::assertNull($result);
    }

    public function test_playground_url_for_example_with_option() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        $result = $service->playgroundUrl('topic_1', 'example_1', 'option_1');

        self::assertSame([
            'name' => 'example_option_playground',
            'arguments' => [
                'topic' => 'topic_1',
                'example' => 'example_1',
                'option' => 'option_1',
            ],
        ], $result);
    }

    public function test_playground_url_for_example_without_option() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        $result = $service->playgroundUrl('topic_1', 'example_1');

        self::assertSame([
            'name' => 'example_playground',
            'arguments' => [
                'topic' => 'topic_1',
                'example' => 'example_1',
            ],
        ], $result);
    }

    public function test_return_topic_example_code() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        self::assertStringContainsString(
            'example code file',
            $service->code('topic_3', 'example_1')
        );
    }

    public function test_return_topic_examples_list() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        self::assertEqualsCanonicalizing(
            $service->examples('topic_2'),
            [
                'example_1',
                'example_2',
            ]
        );
    }

    public function test_return_topics_list() : void
    {
        $path = __DIR__ . '/../Fixtures/Valid';
        $service = new Examples($path);

        self::assertEqualsCanonicalizing(
            $service->topics(),
            [
                'topic_1',
                'topic_2',
                'topic_3',
            ]
        );
    }

    public function test_throw_exception_when_example_code_does_not_exists() : void
    {
        $path = __DIR__ . '/../Fixtures/InvalidCode';
        $service = new Examples($path);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Code example doesn\'t exists, it should be located in path: ".*"./');

        $service->code('topic_1', 'example_1');
    }

    public function test_throw_exception_when_topic_directory_does_not_exists() : void
    {
        $path = __DIR__ . '/../Fixtures/InvalidExample';
        $service = new Examples($path);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/Topic "non_existing_topic" doesn\'t exists, it should be located in path: ".*"./');

        $service->examples('non_existing_topic');
    }

    public function test_throw_exception_when_topic_directory_is_empty() : void
    {
        $path = __DIR__ . '/../Fixtures/InvalidExample';
        $service = new Examples($path);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Topic "topic_1" doesn\'t have any example, there should be at least one example in path');

        $service->examples('topic_1');
    }

    public function test_throw_exception_when_topics_directory_is_empty() : void
    {
        $path = __DIR__ . '/../Fixtures/InvalidTopic';
        $service = new Examples($path);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Topics root directory doesn\'t exists, it should be located in path:');

        $service->topics();
    }
}
