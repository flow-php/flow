<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Physical\PlannedNode;
use Flow\ETL\Planning\Physical\PlannedNodes;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\CountingFileLoader;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\RecordingDatastoreLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;

final class PlannedNodesTest extends FlowTestCase
{
    public function test_an_added_node_is_returned_by_identity(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::read();
        $plannedNode = new PlannedNode([], [], null);

        static::assertSame($plannedNode, $planned->add($node, $plannedNode));
        static::assertTrue($planned->has($node));
        static::assertSame($plannedNode, $planned->of($node));
        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_of_throws_for_a_node_never_planned(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('was never planned');

        (new PlannedNodes())->of(NodeMother::read());
    }

    public function test_it_keeps_the_first_refusal(): void
    {
        $planned = new PlannedNodes();
        $first = new SchemaNotDerivableException('first');

        static::assertNull($planned->refusal());

        $planned->refuse($first);
        $planned->refuse(new SchemaNotDerivableException('second'));

        static::assertSame($first, $planned->refusal());
    }

    public function test_steps_are_the_bound_ones_until_the_plan_refuses(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::read();
        $steps = [new LimitTransformer(5)];
        $bound = [new LimitTransformer(5)];
        $planned->add($node, new PlannedNode($steps, $bound, null));

        static::assertSame($bound, $planned->steps($node));

        $planned->refuse(new SchemaNotDerivableException('refused'));

        static::assertSame($steps, $planned->steps($node));
    }

    public function test_a_commit_stage_is_built_once_per_loader_with_the_directory_it_writes_to(): void
    {
        $spy = new ProgressSpy();
        $planned = new PlannedNodes($spy);
        $loader = to_floe(path('memory://out/part-{part}.floe'), filesystem: memory_filesystem());

        $planned->planWrite($loader);
        $planned->planWrite($loader);

        static::assertSame(
            [
                ['write',  ProgressUnit::rows,  null, 'memory://out/'],
                ['commit', ProgressUnit::files, null, 'memory://out/'],
            ],
            $spy->built(),
        );
        static::assertCount(1, $planned->stages());
    }

    public function test_plan_write_plans_a_write_stage_for_every_loader_and_a_commit_for_a_file_loader(): void
    {
        $spy = new ProgressSpy();
        $planned = new PlannedNodes($spy);

        $planned->planWrite(to_memory(new ArrayMemory()));
        $planned->planWrite(to_floe(path('memory://out/part-{part}.floe'), filesystem: memory_filesystem()));

        static::assertSame(['write', 'write', 'commit'], $spy->stages());
    }

    public function test_a_datastore_loaders_collection_is_the_subject_of_its_write_stage(): void
    {
        $spy = new ProgressSpy();

        (new PlannedNodes($spy))->planWrite(new RecordingDatastoreLoader());

        static::assertSame([['write', ProgressUnit::rows, null, 'crm.contacts']], $spy->built());
    }

    public function test_a_datastore_loader_has_no_subject_without_a_heartbeat(): void
    {
        $loader = new RecordingDatastoreLoader();

        (new PlannedNodes())->planWrite($loader);

        static::assertSame(0, $loader->collectionCalls);
    }

    public function test_with_nobody_listening_a_commit_stage_asks_its_write_for_nothing(): void
    {
        $loader = new CountingFileLoader(to_floe(
            path('memory://out/part-{part}.floe'),
            filesystem: memory_filesystem(),
        ));

        (new PlannedNodes())->planWrite($loader);

        static::assertSame(0, $loader->destinationCalls->count());
    }

    public function test_a_joined_plan_shares_the_stages_and_keeps_its_own_nodes(): void
    {
        $planned = new PlannedNodes(new ProgressSpy());
        $joined = $planned->joined();
        $loader = to_floe(path('memory://out/part-{part}.floe'), filesystem: memory_filesystem());
        $read = NodeMother::read();
        $joined->add($read, new PlannedNode([], [], null));

        $joined->planWrite($loader);
        $joined->planRead($read);

        static::assertSame([true, true], [
            $planned->stages()->offsetExists($loader),
            $planned->readStageOf($read) !== null,
        ]);
        $this->expectException(InvalidLogicException::class);
        $planned->of($read);
    }

    public function test_the_writes_a_node_hosts_are_listed_in_the_order_they_were_hosted(): void
    {
        $planned = new PlannedNodes();
        $host = NodeMother::read();
        $first = new Write($host, to_floe(path('memory://a/part-{part}.floe'), filesystem: memory_filesystem()));
        $second = new Write($host, to_floe(path('memory://b/part-{part}.floe'), filesystem: memory_filesystem()));

        $planned->hosts($host, $first, true);
        $planned->hosts($host, $second, false);

        static::assertSame([[$first, true], [$second, false]], $planned->hostedBy($host));
        static::assertSame([], $planned->hostedBy(NodeMother::read()));
    }
}
