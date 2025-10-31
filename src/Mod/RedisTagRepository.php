<?php

namespace Daison\LaravelHorizonCluster\Mod;

use Laravel\Horizon\Repositories\RedisTagRepository as Base;

/**
 * @author Daison Carino <daison12006013@gmail.com>
 */
class RedisTagRepository extends Base
{
    use PipelineToBlockingTrait;

    /**
     * {@inheritDoc}
     */
    public function add($id, array $tags): void
    {
        $this->blocking(function ($pipe) use ($id, $tags) {
            foreach ($tags as $tag) {
                $pipe->zadd($tag, str_replace(',', '.', microtime(true)), $id);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public function addTemporary($minutes, $id, array $tags): void
    {
        $this->blocking(function ($pipe) use ($minutes, $id, $tags) {
            foreach ($tags as $tag) {
                $pipe->zadd($tag, str_replace(',', '.', microtime(true)), $id);

                $pipe->expire($tag, $minutes * 60);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public function forgetJobs($tags, $ids): void
    {
        $this->blocking(function ($pipe) use ($tags, $ids) {
            foreach ((array) $tags as $tag) {
                foreach ((array) $ids as $id) {
                    $pipe->zrem($tag, $id);
                }
            }
        });
    }
}
