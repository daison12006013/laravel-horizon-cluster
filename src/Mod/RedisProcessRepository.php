<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Carbon\CarbonImmutable;
use Laravel\Horizon\Repositories\RedisProcessRepository as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisProcessRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * {@inheritDoc}
     */
    public function orphaned($master, array $processIds): void
    {
        $time = CarbonImmutable::now()->getTimestamp();

        $shouldRemove = array_diff($this->connection()->hkeys(
            $key = "{$master}:orphans"
        ), $processIds);

        if (! empty($shouldRemove)) {
            $this->connection()->hdel($key, ...$shouldRemove);
        }

        $this->blocking(function ($pipe) use ($key, $time, $processIds) {
            foreach ($processIds as $processId) {
                $pipe->hsetnx($key, $processId, $time);
            }
        });
    }
}
