<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Exception\InvalidExplainConfigException;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use PHPUnit\Framework\TestCase;

final class ExplainConfigTest extends TestCase
{
    public function test_chaining_fluent_methods() : void
    {
        $config = ExplainConfig::forEstimate()
            ->withAnalyze()
            ->withBuffers()
            ->withTiming()
            ->withVerbose()
            ->withFormat(ExplainFormat::TEXT);

        self::assertTrue($config->analyze);
        self::assertTrue($config->buffers);
        self::assertTrue($config->timing);
        self::assertTrue($config->verbose);
        self::assertSame(ExplainFormat::TEXT, $config->format);
    }

    public function test_constructor_throws_when_buffers_enabled_without_analyze() : void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: true);
    }

    public function test_constructor_throws_when_timing_enabled_without_analyze() : void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('TIMING option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: false, timing: true);
    }

    public function test_constructor_throws_when_wal_enabled_without_analyze() : void
    {
        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('WAL option requires ANALYZE to be enabled');

        new ExplainConfig(analyze: false, buffers: false, timing: false, wal: true);
    }

    public function test_constructor_with_valid_config_without_analyze() : void
    {
        $config = new ExplainConfig(
            analyze: false,
            costs: true,
            buffers: false,
            timing: false,
            wal: false
        );

        self::assertFalse($config->analyze);
        self::assertTrue($config->costs);
        self::assertFalse($config->buffers);
        self::assertFalse($config->timing);
        self::assertFalse($config->wal);
    }

    public function test_for_analysis_can_be_customized_with_fluent_methods() : void
    {
        $config = ExplainConfig::forAnalysis()
            ->withVerbose()
            ->withMemory()
            ->withSettings();

        self::assertTrue($config->verbose);
        self::assertTrue($config->memory);
        self::assertTrue($config->settings);
    }

    public function test_for_analysis_is_static_factory() : void
    {
        $config = ExplainConfig::forAnalysis();

        self::assertTrue($config->analyze);
        self::assertFalse($config->verbose);
        self::assertTrue($config->costs);
        self::assertTrue($config->buffers);
        self::assertTrue($config->timing);
        self::assertTrue($config->summary);
        self::assertFalse($config->memory);
        self::assertFalse($config->settings);
        self::assertFalse($config->wal);
    }

    public function test_for_analysis_returns_config_with_analyze_options() : void
    {
        $config = ExplainConfig::forAnalysis();

        self::assertTrue($config->analyze);
        self::assertTrue($config->costs);
        self::assertTrue($config->buffers);
        self::assertTrue($config->timing);
        self::assertTrue($config->summary);
        self::assertFalse($config->wal);
    }

    public function test_for_estimate_can_be_customized_with_fluent_methods() : void
    {
        $config = ExplainConfig::forEstimate()
            ->withVerbose()
            ->withMemory()
            ->withSettings();

        self::assertTrue($config->verbose);
        self::assertTrue($config->memory);
        self::assertTrue($config->settings);
        self::assertFalse($config->analyze);
    }

    public function test_for_estimate_is_static_factory() : void
    {
        $config = ExplainConfig::forEstimate();

        self::assertFalse($config->analyze);
        self::assertFalse($config->verbose);
        self::assertTrue($config->costs);
        self::assertFalse($config->buffers);
        self::assertFalse($config->timing);
        self::assertFalse($config->summary);
        self::assertFalse($config->memory);
        self::assertFalse($config->settings);
        self::assertFalse($config->wal);
    }

    public function test_for_estimate_returns_config_without_analyze() : void
    {
        $config = ExplainConfig::forEstimate();

        self::assertFalse($config->analyze);
        self::assertTrue($config->costs);
        self::assertFalse($config->buffers);
        self::assertFalse($config->timing);
        self::assertFalse($config->summary);
        self::assertFalse($config->wal);
    }

    public function test_from_array_and_normalize_are_inverse() : void
    {
        $original = new ExplainConfig(
            analyze: true,
            verbose: true,
            costs: true,
            buffers: true,
            timing: true,
            summary: true,
            memory: true,
            settings: true,
            wal: true,
            format: ExplainFormat::TEXT,
        );

        $normalized = $original->normalize();
        $restored = ExplainConfig::fromArray($normalized);

        self::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance() : void
    {
        $data = [
            'analyze' => true,
            'verbose' => true,
            'costs' => true,
            'buffers' => true,
            'timing' => true,
            'summary' => true,
            'memory' => true,
            'settings' => true,
            'wal' => true,
            'format' => 'text',
        ];

        $config = ExplainConfig::fromArray($data);

        self::assertTrue($config->analyze);
        self::assertTrue($config->verbose);
        self::assertTrue($config->costs);
        self::assertTrue($config->buffers);
        self::assertTrue($config->timing);
        self::assertTrue($config->summary);
        self::assertTrue($config->memory);
        self::assertTrue($config->settings);
        self::assertTrue($config->wal);
        self::assertSame(ExplainFormat::TEXT, $config->format);
    }

    public function test_from_array_validates_config() : void
    {
        $data = [
            'analyze' => false,
            'verbose' => false,
            'costs' => true,
            'buffers' => true,
            'timing' => false,
            'summary' => false,
            'memory' => false,
            'settings' => false,
            'wal' => false,
            'format' => 'json',
        ];

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        ExplainConfig::fromArray($data);
    }

    public function test_from_array_with_for_estimate_config() : void
    {
        $original = ExplainConfig::forEstimate();
        $normalized = $original->normalize();
        $restored = ExplainConfig::fromArray($normalized);

        self::assertEquals($original, $restored);
    }

    public function test_normalize_returns_all_fields() : void
    {
        $config = ExplainConfig::forAnalysis()
            ->withVerbose()
            ->withMemory()
            ->withSettings()
            ->withWal()
            ->withFormat(ExplainFormat::YAML);

        $normalized = $config->normalize();

        self::assertTrue($normalized['analyze']);
        self::assertTrue($normalized['verbose']);
        self::assertTrue($normalized['costs']);
        self::assertTrue($normalized['buffers']);
        self::assertTrue($normalized['timing']);
        self::assertTrue($normalized['summary']);
        self::assertTrue($normalized['memory']);
        self::assertTrue($normalized['settings']);
        self::assertTrue($normalized['wal']);
        self::assertSame('yaml', $normalized['format']);
    }

    public function test_normalize_returns_expected_keys() : void
    {
        $config = ExplainConfig::forAnalysis();

        $normalized = $config->normalize();

        $expectedKeys = [
            'analyze',
            'verbose',
            'costs',
            'buffers',
            'timing',
            'summary',
            'memory',
            'settings',
            'wal',
            'format',
        ];

        self::assertSame($expectedKeys, \array_keys($normalized));
    }

    public function test_with_analyze_returns_new_instance() : void
    {
        $original = ExplainConfig::forEstimate();
        $modified = $original->withAnalyze();

        self::assertNotSame($original, $modified);
        self::assertFalse($original->analyze);
        self::assertTrue($modified->analyze);
    }

    public function test_with_buffers_requires_analyze() : void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('BUFFERS option requires ANALYZE to be enabled');

        $config->withBuffers();
    }

    public function test_with_buffers_works_with_analyze() : void
    {
        $config = ExplainConfig::forEstimate();
        $modified = $config->withAnalyze()->withBuffers();

        self::assertTrue($modified->analyze);
        self::assertTrue($modified->buffers);
    }

    public function test_with_costs_returns_new_instance() : void
    {
        $config = ExplainConfig::forAnalysis()->withoutCosts();
        $modified = $config->withCosts();

        self::assertFalse($config->costs);
        self::assertTrue($modified->costs);
    }

    public function test_with_format_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withFormat(ExplainFormat::TEXT);

        self::assertSame(ExplainFormat::JSON, $original->format);
        self::assertSame(ExplainFormat::TEXT, $modified->format);
    }

    public function test_with_memory_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withMemory();

        self::assertFalse($original->memory);
        self::assertTrue($modified->memory);
    }

    public function test_with_settings_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withSettings();

        self::assertFalse($original->settings);
        self::assertTrue($modified->settings);
    }

    public function test_with_summary_returns_new_instance() : void
    {
        $original = ExplainConfig::forEstimate();
        $modified = $original->withSummary();

        self::assertFalse($original->summary);
        self::assertTrue($modified->summary);
    }

    public function test_with_timing_requires_analyze() : void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('TIMING option requires ANALYZE to be enabled');

        $config->withTiming();
    }

    public function test_with_timing_works_with_analyze() : void
    {
        $config = ExplainConfig::forEstimate();
        $modified = $config->withAnalyze()->withTiming();

        self::assertTrue($modified->analyze);
        self::assertTrue($modified->timing);
    }

    public function test_with_verbose_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withVerbose();

        self::assertFalse($original->verbose);
        self::assertTrue($modified->verbose);
    }

    public function test_with_wal_requires_analyze() : void
    {
        $config = ExplainConfig::forEstimate();

        $this->expectException(InvalidExplainConfigException::class);
        $this->expectExceptionMessage('WAL option requires ANALYZE to be enabled');

        $config->withWal();
    }

    public function test_with_wal_works_with_analyze() : void
    {
        $config = ExplainConfig::forAnalysis();
        $modified = $config->withWal();

        self::assertTrue($modified->analyze);
        self::assertTrue($modified->wal);
    }

    public function test_without_analyze_disables_dependent_options() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutAnalyze();

        self::assertTrue($original->analyze);
        self::assertTrue($original->buffers);
        self::assertTrue($original->timing);

        self::assertFalse($modified->analyze);
        self::assertFalse($modified->buffers);
        self::assertFalse($modified->timing);
        self::assertFalse($modified->wal);
    }

    public function test_without_buffers_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutBuffers();

        self::assertTrue($original->buffers);
        self::assertFalse($modified->buffers);
    }

    public function test_without_costs_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutCosts();

        self::assertTrue($original->costs);
        self::assertFalse($modified->costs);
    }

    public function test_without_memory_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis()->withMemory();
        $modified = $original->withoutMemory();

        self::assertTrue($original->memory);
        self::assertFalse($modified->memory);
    }

    public function test_without_settings_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis()->withSettings();
        $modified = $original->withoutSettings();

        self::assertTrue($original->settings);
        self::assertFalse($modified->settings);
    }

    public function test_without_summary_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutSummary();

        self::assertTrue($original->summary);
        self::assertFalse($modified->summary);
    }

    public function test_without_timing_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis();
        $modified = $original->withoutTiming();

        self::assertTrue($original->timing);
        self::assertFalse($modified->timing);
    }

    public function test_without_verbose_returns_new_instance() : void
    {
        $original = ExplainConfig::forAnalysis()->withVerbose();
        $modified = $original->withoutVerbose();

        self::assertTrue($original->verbose);
        self::assertFalse($modified->verbose);
    }

    public function test_without_wal_returns_new_instance() : void
    {
        $config = ExplainConfig::forAnalysis()->withWal();
        $modified = $config->withoutWal();

        self::assertTrue($config->wal);
        self::assertFalse($modified->wal);
    }
}
