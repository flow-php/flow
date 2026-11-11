<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Describe;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Runtime\Heartbeat\HeartbeatStage;
use Flow\ETL\Runtime\Heartbeat\ScheduledHeartbeat;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\Described;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidTypeException;

final class RebuildTest extends FlowTestCase
{
    public function test_an_unknown_class_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Class Flow\\Nope\\Missing is not autoloadable here: install the package that provides it',
        );

        /** @var class-string $class */
        $class = 'Flow\\Nope\\Missing';

        (new Rebuild())->object(new Description($class, []), Describable::class);
    }

    public function test_a_class_that_is_none_of_the_three_kinds_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Flow\\ETL\\Foundation\\Describe\\Rebuild is not Describable');

        (new Rebuild())->object(new Description(Rebuild::class, []), Describable::class);
    }

    public function test_an_interface_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Flow\\ETL\\Foundation\\Describe\\Describable is not a concrete class');

        (new Rebuild())->object(new Description(Describable::class, []), Describable::class);
    }

    public function test_the_wrong_type_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Flow\\ETL\\Tests\\Double\\Described is not a Flow\\ETL\\Foundation\\Describe\\ConfiguredFilesystem',
        );

        (new Rebuild())->object((new Described('x'))->describe(), ConfiguredFilesystem::class);
    }

    public function test_a_describable_is_rebuilt_from_its_description(): void
    {
        static::assertEquals(
            new Described('x'),
            (new Rebuild())->object((new Described('x'))->describe(), Described::class),
        );
    }

    public function test_of_rebuilds_a_normalized_description(): void
    {
        static::assertEquals(
            new Described('x'),
            (new Rebuild())->of((new Described('x'))->describe()->normalize(), Described::class),
        );
    }

    public function test_all_keeps_keys(): void
    {
        $rebuilt = (new Rebuild())->all([
            'first' => (new Described('a'))->describe()->normalize(),
            7 => (new Described('b'))->describe()->normalize(),
        ], Described::class);

        static::assertEquals(['first' => new Described('a'), 7 => new Described('b')], $rebuilt);
    }

    public function test_of_refuses_a_non_array(): void
    {
        $this->expectException(InvalidTypeException::class);

        (new Rebuild())->of('not a description', Described::class);
    }

    public function test_progress_is_void_by_default(): void
    {
        static::assertInstanceOf(VoidProgress::class, (new Rebuild())->progress(StageName::sort));
    }

    public function test_progress_is_bound_by_the_given_stages(): void
    {
        $spy = new ProgressSpy();

        (new Rebuild($spy))->progress(StageName::join);

        static::assertSame(['join'], $spy->stages());
    }

    public function test_progress_from_a_stage_description_reports_into_its_id(): void
    {
        $spy = new ProgressSpy();
        (new Rebuild($spy))->progress(
            StageName::sort,
            (new HeartbeatStage(
                StageName::sort,
                ProgressUnit::rows,
                7,
                'memory://in/x.floe',
                (new HeartbeatContext())->throttle(new ScheduledHeartbeat(new RecordingHeartbeat(), 1)),
                0,
            ))
                ->describe()
                ?->normalize(),
        );

        static::assertSame([['sort', ProgressUnit::rows, 7, 'memory://in/x.floe']], $spy->built());
    }

    public function test_progress_without_a_description_is_a_new_stage_of_the_name(): void
    {
        $spy = new ProgressSpy();

        (new Rebuild($spy))->progress(StageName::group_by);
        (new Rebuild($spy))->progress(StageName::sort, (new VoidProgress())->describe()?->normalize());

        static::assertSame(
            [
                ['group_by', ProgressUnit::rows, null, null],
                ['sort',     ProgressUnit::rows, null, null],
            ],
            $spy->built(),
        );
    }
}
