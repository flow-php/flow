<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

final class PostHogRecordingTest extends EndToEndTestCase
{
    public function test_capture_calls_are_recorded(): void
    {
        $this->recordPostHog($browser = $this->playwrightBrowser());
        $browser->visit('/');

        $this->pageOf($browser)->evaluate(
            "() => { window.posthog.capture('test:with_properties', { kind: 'proof' }); window.posthog.capture('test:without_properties'); }",
        );

        static::assertSame(
            [
                ['name' => 'test:with_properties', 'properties' => ['kind' => 'proof']],
                ['name' => 'test:without_properties', 'properties' => null],
            ],
            $this->capturedEvents($browser),
        );
    }
}
